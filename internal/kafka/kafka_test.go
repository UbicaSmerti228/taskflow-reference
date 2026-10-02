package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/UbicaSmerti228/taskflow-reference/internal/event"
)

const topic = "test.events"

// Брокер в тестах — kfake: настоящий протокол Kafka внутри процесса, без Docker.
func newCluster(t *testing.T) []string {
	t.Helper()
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c.ListenAddrs()
}

func newProducer(t *testing.T, brokers []string) *Producer {
	t.Helper()
	p, err := NewProducer(brokers, 3)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p
}

func records(key string, n int) []event.Record {
	batch := make([]event.Record, n)
	for i := range batch {
		id := fmt.Sprintf("%s-%d", key, i)
		batch[i] = event.Record{EventID: id, Topic: topic, Key: key, Payload: []byte(id)}
	}
	return batch
}

// sink собирает обработанные сообщения.
type sink struct {
	mu   sync.Mutex
	got  []Message
	fail func(m Message, seen int) error // seen — сколько раз это сообщение уже приходило
	seen map[string]int
}

func (s *sink) handle(_ context.Context, m Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		s.seen = map[string]int{}
	}
	seen := s.seen[string(m.Value)]
	s.seen[string(m.Value)]++
	if s.fail != nil {
		if err := s.fail(m, seen); err != nil {
			return err
		}
	}
	s.got = append(s.got, m)
	return nil
}

func (s *sink) values() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.got))
	for i, m := range s.got {
		out[i] = string(m.Value)
	}
	return out
}

// consume запускает читателя и возвращает функцию остановки.
func consume(t *testing.T, brokers []string, group string, s *sink) (stop func()) {
	t.Helper()
	c, err := NewConsumer(brokers, group, topic, 3, s.handle, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	c.retry.Base = time.Millisecond // повторы без долгих пауз
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		<-done
		c.Close()
	}
	t.Cleanup(stop)
	return stop
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPublishAndConsume(t *testing.T) {
	brokers := newCluster(t)
	p := newProducer(t, brokers)
	ctx := context.Background()

	if err := p.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	// Топика ещё нет: первая отправка создаёт его с тремя партициями.
	for _, key := range []string{"a", "b", "c", "d"} {
		if err := p.Publish(ctx, records(key, 5)); err != nil {
			t.Fatal(err)
		}
	}
	topics, err := kadm.NewClient(p.cl).ListTopics(ctx, topic)
	if err != nil || len(topics[topic].Partitions) != 3 {
		t.Fatalf("у топика %d партиций (%v), want 3", len(topics[topic].Partitions), err)
	}
	// Топик уже создал другой экземпляр сервиса: это не ошибка.
	if err := newProducer(t, brokers).EnsureTopic(ctx, topic); err != nil {
		t.Fatalf("EnsureTopic для существующего топика: %v", err)
	}

	s := &sink{}
	consume(t, brokers, "g1", s)
	waitFor(t, "20 сообщений", func() bool { return len(s.values()) == 20 })

	// Сообщения с одним ключом лежат в одной партиции и приходят в порядке отправки.
	s.mu.Lock()
	defer s.mu.Unlock()
	partition, next := map[string]int32{}, map[string]int{}
	for _, m := range s.got {
		key := string(m.Key)
		if p, ok := partition[key]; ok && p != m.Partition {
			t.Errorf("ключ %s оказался в партициях %d и %d", key, p, m.Partition)
		}
		partition[key] = m.Partition
		if want := fmt.Sprintf("%s-%d", key, next[key]); string(m.Value) != want {
			t.Errorf("ключ %s: пришло %s, want %s", key, m.Value, want)
		}
		next[key]++
	}
}

// Обработчик упал — сообщение подаётся снова, пока не будет обработано. Следующие ждут своей очереди.
func TestConsumerRetriesFailedMessage(t *testing.T) {
	brokers := newCluster(t)
	if err := newProducer(t, brokers).Publish(context.Background(), records("k", 3)); err != nil {
		t.Fatal(err)
	}
	s := &sink{fail: func(m Message, seen int) error {
		if string(m.Value) == "k-1" && seen < 3 {
			return errors.New("база недоступна")
		}
		return nil
	}}
	consume(t, brokers, "g1", s)
	waitFor(t, "3 сообщения", func() bool { return len(s.values()) == 3 })

	if got := s.values(); got[0] != "k-0" || got[1] != "k-1" || got[2] != "k-2" {
		t.Errorf("порядок обработки %v, want k-0, k-1, k-2", got)
	}
	if s.seen["k-1"] != 4 {
		t.Errorf("сообщение k-1 подавалось %d раз, want 4", s.seen["k-1"])
	}
}

// Сообщение, которое не обработать никогда, пропускается и не блокирует очередь.
func TestConsumerSkipsPoisonMessage(t *testing.T) {
	brokers := newCluster(t)
	if err := newProducer(t, brokers).Publish(context.Background(), records("k", 3)); err != nil {
		t.Fatal(err)
	}
	s := &sink{fail: func(m Message, _ int) error {
		if string(m.Value) == "k-1" {
			return Skip(errors.New("битое сообщение"))
		}
		return nil
	}}
	consume(t, brokers, "g1", s)
	waitFor(t, "2 сообщения", func() bool { return len(s.values()) == 2 })
	if s.seen["k-1"] != 1 {
		t.Errorf("битое сообщение подавалось %d раз, want 1", s.seen["k-1"])
	}
}

// Обработанное подтверждено: после перезапуска оно не приходит снова. Новое — приходит.
func TestConsumerResumesAfterRestart(t *testing.T) {
	brokers := newCluster(t)
	p := newProducer(t, brokers)
	ctx := context.Background()
	if err := p.Publish(ctx, records("first", 4)); err != nil {
		t.Fatal(err)
	}

	s := &sink{}
	stop := consume(t, brokers, "g1", s)
	waitFor(t, "4 сообщения", func() bool { return len(s.values()) == 4 })
	stop()

	if err := p.Publish(ctx, records("second", 2)); err != nil {
		t.Fatal(err)
	}
	again := &sink{}
	consume(t, brokers, "g1", again)
	waitFor(t, "2 новых сообщения", func() bool { return len(again.values()) == 2 })
	for _, v := range again.values() {
		if v != "second-0" && v != "second-1" {
			t.Errorf("после перезапуска пришло уже обработанное сообщение %s", v)
		}
	}

	// У другой группы свои отметки: она читает топик с начала.
	other := &sink{}
	consume(t, brokers, "g2", other)
	waitFor(t, "6 сообщений у новой группы", func() bool { return len(other.values()) == 6 })
}

// Остановка во время обработки: сообщение не подтверждено и после перезапуска придёт снова.
func TestConsumerRedeliversUnfinishedMessage(t *testing.T) {
	brokers := newCluster(t)
	if err := newProducer(t, brokers).Publish(context.Background(), records("k", 1)); err != nil {
		t.Fatal(err)
	}
	failing := &sink{fail: func(Message, int) error { return errors.New("база недоступна") }}
	stop := consume(t, brokers, "g1", failing)
	waitFor(t, "несколько попыток", func() bool {
		failing.mu.Lock()
		defer failing.mu.Unlock()
		return failing.seen["k-0"] >= 3
	})
	stop()

	healthy := &sink{}
	consume(t, brokers, "g1", healthy)
	waitFor(t, "сообщение после перезапуска", func() bool { return len(healthy.values()) == 1 })
}

func TestProducerFailsWithoutBroker(t *testing.T) {
	p, err := NewProducer([]string{"127.0.0.1:1"}, 3)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := p.Publish(ctx, records("k", 1)); err == nil {
		t.Error("Publish() без брокера вернул nil")
	}
	if err := p.EnsureTopic(ctx, topic); err == nil {
		t.Error("EnsureTopic() без брокера вернул nil")
	}
}

// Читатель запущен раньше издателя и раньше самой Kafka: он ждёт, создаёт топик и начинает читать.
func TestConsumerStartsBeforeTopicExists(t *testing.T) {
	brokers := newCluster(t)
	s := &sink{}
	consume(t, brokers, "g1", s)

	if err := newProducer(t, brokers).Publish(context.Background(), records("k", 2)); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "2 сообщения", func() bool { return len(s.values()) == 2 })
}

func TestConsumerWaitsForBroker(t *testing.T) {
	c, err := NewConsumer([]string{"127.0.0.1:1"}, "g1", topic, 3, (&sink{}).handle, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.retry.Base = time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()
	select {
	case <-done:
		t.Fatal("без брокера читатель завершился сам, а должен ждать")
	case <-time.After(300 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("читатель не остановился по отмене контекста")
	}
	if err := c.Ping(context.Background()); err == nil {
		t.Error("Ping() без брокера вернул nil")
	}
}
