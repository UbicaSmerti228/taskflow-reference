package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/UbicaSmerti228/taskflow-reference/internal/resilience"
)

// Message — одно сообщение топика.
type Message struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
}

// Handler обрабатывает сообщение. Ошибка означает «попробуй ещё раз»: сообщение будет подано снова.
// Сообщение, которое не обработать никогда (например, битый JSON), нужно вернуть через Skip.
type Handler func(ctx context.Context, m Message) error

// Skip помечает ошибку сообщения, которое повтор не исправит: оно будет записано в лог и пропущено.
func Skip(err error) error { return resilience.Permanent(err) }

// Consumer читает топик в составе consumer group и подтверждает сообщения после обработки.
type Consumer struct {
	cl         *kgo.Client
	topic      string
	partitions int32
	handle     Handler
	log        *slog.Logger
	retry      resilience.Retry
}

// NewConsumer создаёт читателя. Все экземпляры с одной group делят партиции топика между собой.
// partitions — сколько партиций дать топику, если издатель его ещё не создал.
func NewConsumer(brokers []string, group, topic string, partitions int32, handle Handler, log *slog.Logger) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		// Offset подтверждаем сами и только после обработки. С автоподтверждением сообщение могло бы
		// считаться прочитанным, хотя обработка упала: оно бы потерялось.
		kgo.DisableAutoCommit(),
		// Новая группа читает топик с начала: события, опубликованные до первого запуска, не пропадут.
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: %w", err)
	}
	return &Consumer{
		cl:         cl,
		topic:      topic,
		partitions: partitions,
		handle:     handle,
		log:        log,
		// Обработчик повторяется, пока не справится: пропустить сообщение значит потерять событие.
		retry: resilience.Retry{Attempts: math.MaxInt, Base: 200 * time.Millisecond, Max: 5 * time.Second},
	}, nil
}

// Run читает и обрабатывает сообщения, пока не отменён ctx.
//
// Гарантия — «хотя бы один раз»: если процесс упадёт между обработкой и подтверждением,
// сообщение придёт снова. Поэтому обработчик обязан быть идемпотентным.
func (c *Consumer) Run(ctx context.Context) {
	// Читатель может стартовать раньше издателя, когда топика ещё нет. Создаём его сами
	// и повторяем, пока Kafka не ответит: без неё этому сервису делать нечего.
	logged := false
	err := c.retry.Do(ctx, func(ctx context.Context) error {
		err := ensureTopic(ctx, c.cl, c.topic, c.partitions)
		if err != nil && !logged && ctx.Err() == nil {
			c.log.WarnContext(ctx, "kafka is not ready, retrying", "err", err)
			logged = true
		}
		return err
	})
	if err != nil {
		return // остановка сервиса
	}
	c.log.InfoContext(ctx, "kafka consumer started", "topic", c.topic)

	for {
		fetches := c.cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			c.log.WarnContext(ctx, "kafka fetch failed", "topic", topic, "partition", partition, "err", err)
		})

		// Сообщения партиции обрабатываются по одному и по порядку.
		var done []*kgo.Record
		for it := fetches.RecordIter(); !it.Done(); {
			rec := it.Next()
			if !c.process(ctx, rec) {
				break // остановка: это сообщение и следующие придут снова после запуска
			}
			done = append(done, rec)
		}
		c.commit(ctx, done)
	}
}

// process возвращает false, если обработку прервала остановка сервиса.
func (c *Consumer) process(ctx context.Context, rec *kgo.Record) bool {
	m := Message{Topic: rec.Topic, Partition: rec.Partition, Offset: rec.Offset, Key: rec.Key, Value: rec.Value}
	attempt := 0
	err := c.retry.Do(ctx, func(ctx context.Context) error {
		attempt++
		err := c.handle(ctx, m)
		if err != nil && attempt == 1 && ctx.Err() == nil {
			c.log.WarnContext(ctx, "message handling failed, retrying", "partition", m.Partition, "offset", m.Offset, "err", err)
		}
		return err
	})
	switch {
	case err == nil:
		return true
	case ctx.Err() != nil:
		return false
	default:
		// Сюда попадают только ошибки, помеченные Skip. В рабочей системе такое сообщение
		// стоит переложить в отдельный топик (dead letter queue), чтобы разобраться позже.
		c.log.ErrorContext(ctx, "message skipped", "partition", m.Partition, "offset", m.Offset, "err", err)
		return true
	}
}

func (c *Consumer) commit(ctx context.Context, done []*kgo.Record) {
	if len(done) == 0 {
		return
	}
	// При остановке ctx уже отменён, а подтвердить обработанное всё равно нужно.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := c.cl.CommitRecords(ctx, done...); err != nil && !errors.Is(err, context.Canceled) {
		// Не страшно: сообщения придут ещё раз, а обработчик идемпотентен.
		c.log.WarnContext(ctx, "kafka commit failed, messages will be redelivered", "err", err)
	}
}

// Ping проверяет, что брокер отвечает.
func (c *Consumer) Ping(ctx context.Context) error { return c.cl.Ping(ctx) }

// Close выходит из группы и закрывает соединения: партиции сразу переходят другим экземплярам.
func (c *Consumer) Close() { c.cl.Close() }
