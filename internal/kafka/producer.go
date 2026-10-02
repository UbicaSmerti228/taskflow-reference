// Package kafka — тонкая обёртка над клиентом franz-go: издатель событий и читатель в consumer group.
// Остальной код про библиотеку не знает и работает с интерфейсами.
package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
)

// HeaderEventID — заголовок сообщения с идентификатором события: его видно, не разбирая тело.
const HeaderEventID = "event_id"

// Producer публикует события.
type Producer struct {
	cl         *kgo.Client
	partitions int32

	mu     sync.Mutex
	topics map[string]bool // топики, которые уже точно существуют
}

// NewProducer создаёт издателя. Соединение устанавливается при первой отправке, а не здесь:
// сервис стартует, даже когда Kafka недоступна, и события ждут в outbox.
//
// partitions — сколько партиций дать топику, если его ещё нет: топик создаётся перед первой отправкой в него.
func NewProducer(brokers []string, partitions int32) (*Producer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		// Запись подтверждена, только когда её получили все синхронные реплики: подтверждённое не теряется.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		// Клиент сам повторяет отправку при сбоях и не создаёт дубликатов (idempotent producer — режим по умолчанию).
		// Здесь ограничено, сколько он пробует, прежде чем вернуть ошибку.
		kgo.RecordDeliveryTimeout(10*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka producer: %w", err)
	}
	return &Producer{cl: cl, partitions: partitions, topics: map[string]bool{}}, nil
}

// EnsureTopic создаёт топик, если его ещё нет. Число реплик берётся из настроек брокера.
//
// Топик создаётся явно, а не автоматически при первой записи: автосоздание даёт число партиций
// по умолчанию, а изменить его потом без нарушения порядка сообщений нельзя.
func (p *Producer) EnsureTopic(ctx context.Context, topic string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.topics[topic] {
		return nil
	}
	if err := ensureTopic(ctx, p.cl, topic, p.partitions); err != nil {
		return err
	}
	p.topics[topic] = true
	return nil
}

func ensureTopic(ctx context.Context, cl *kgo.Client, topic string, partitions int32) error {
	resp, err := kadm.NewClient(cl).CreateTopic(ctx, partitions, -1, nil, topic)
	if err == nil {
		err = resp.Err
	}
	if err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
		return fmt.Errorf("create topic %s: %w", topic, err)
	}
	return nil
}

// Publish отправляет пачку и ждёт подтверждения каждого сообщения.
// Ключ определяет партицию: события с одним ключом читаются в порядке отправки.
func (p *Producer) Publish(ctx context.Context, batch []event.Record) error {
	records := make([]*kgo.Record, len(batch))
	for i, rec := range batch {
		if err := p.EnsureTopic(ctx, rec.Topic); err != nil {
			return err
		}
		records[i] = &kgo.Record{
			Topic:   rec.Topic,
			Key:     []byte(rec.Key),
			Value:   rec.Payload,
			Headers: []kgo.RecordHeader{{Key: HeaderEventID, Value: []byte(rec.EventID)}},
		}
	}
	if err := p.cl.ProduceSync(ctx, records...).FirstErr(); err != nil {
		return fmt.Errorf("kafka produce: %w", err)
	}
	return nil
}

// Ping проверяет, что брокер отвечает.
func (p *Producer) Ping(ctx context.Context) error { return p.cl.Ping(ctx) }

// Close дожидается отправки того, что в пути, и закрывает соединения.
func (p *Producer) Close() { p.cl.Close() }
