//go:build integration
// +build integration

package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type Container struct {
	wg           sync.WaitGroup
	mu           sync.Mutex
	sentMessages map[string]struct{}
}

func (c *Container) Add(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sentMessages[msg] = struct{}{}
}

func (c *Container) Delete(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.sentMessages, msg)
	c.wg.Done()
}

func (c *Container) Wait() {
	c.wg.Wait()
}

func WaitContainer(expectCount int) *Container {
	cnt := &Container{sentMessages: make(map[string]struct{})}
	cnt.wg.Add(expectCount)
	return cnt
}

func TestSyncProducer(t *testing.T) {
	var (
		topic       = generateTopic(16)
		value       = "test_value"
		threadCount = 4
		msgCount    = 1000
		msgSum      = msgCount * threadCount
		msgSent     = atomic.NewInt32(0)
		msgConsumed = atomic.NewInt32(0)
		cnt         = WaitContainer(msgSum)
	)

	client, err := kafkaCli()
	assert.NoError(t, err)

	producer, err := client.SyncProducer(topic)
	assert.NoError(t, err)
	defer producer.Close()

	for i := 1; i <= threadCount; i++ {
		go func(i int) {
			for j := 0; j < msgCount; j++ {
				v := fmt.Sprintf("sync_%s_%d_%d", value, i, j)
				msg, err := producer.Produce(sarama.StringEncoder(v))
				assert.NoError(t, err)
				assert.NotNil(t, msg)
				msgSent.Inc()
				cnt.Add(v)
			}
		}(i)
	}

	consumer := client.ConsumerGroup("test_group_1")
	defer consumer.Close()

	err = consumer.ConsumeFunc(context.Background(), []string{topic}, func(_ context.Context, message *sarama.ConsumerMessage) error {
		msgConsumed.Inc()
		cnt.Delete(string(message.Value))
		return nil
	})
	assert.NoError(t, err)

	cnt.Wait()
	err = producer.Close()
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)

	assert.Equal(t, msgSent, msgConsumed)
}

func TestWithResetOffset(t *testing.T) {
	var (
		topic       = generateTopic(16)
		value       = "test_value"
		threadCount = 4
		msgCount    = 1000
		resetOffset = int64(978)
		msgSum      = msgCount * threadCount
		msgSent     = atomic.NewInt32(0)
		msgConsumed = atomic.NewInt32(0)
		cnt         = WaitContainer(msgSum - int(resetOffset))
	)

	client, err := kafkaCli()
	assert.NoError(t, err)

	producer, err := client.SyncProducer(topic)
	assert.NoError(t, err)
	defer producer.Close()

	for i := 1; i <= threadCount; i++ {
		go func(i int) {
			for j := 0; j < msgCount; j++ {
				v := fmt.Sprintf("sync_%s_%d_%d", value, i, j)
				msg, err := producer.Produce(sarama.StringEncoder(v))
				assert.NoError(t, err)
				assert.NotNil(t, msg)
				msgSent.Inc()
				cnt.Add(v)
			}
		}(i)
	}

	consumer := client.ConsumerGroup("test_group_1")
	defer consumer.Close()

	err = consumer.ConsumeFunc(context.Background(), []string{topic}, func(_ context.Context, message *sarama.ConsumerMessage) error {
		fmt.Println("consume", message.Offset)
		msgConsumed.Inc()
		cnt.Delete(string(message.Value))
		return nil
	}, WithResetToNewestOffset(0, resetOffset, topic, ""))
	assert.NoError(t, err)

	cnt.Wait()
	err = producer.Close()
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)

	assert.Equal(t, msgSent.Load()-int32(resetOffset), msgConsumed.Load())
}

func TestAsyncProducerCh(t *testing.T) {
	var (
		topic       = generateTopic(16)
		value       = "test_value"
		threadCount = 4
		msgCount    = 1000
		msgSum      = msgCount * threadCount
		msgSent     = atomic.NewInt32(0)
		msgConsumed = atomic.NewInt32(0)
		cnt         = WaitContainer(msgSum)
	)

	client, err := kafkaCli()
	assert.NoError(t, err)

	producer, err := client.AsyncProducer(topic)
	assert.NoError(t, err)
	defer producer.Close()

	for i := 1; i <= threadCount; i++ {
		go func(i int) {
			for j := 0; j < msgCount; j++ {
				v := fmt.Sprintf("sync_%s_%d_%d", value, i, j)
				ch := producer.ProduceCh(sarama.StringEncoder(v))
				res := <-ch
				assert.NoError(t, res.Error)
				assert.NotNil(t, res.Message)

				b, err := res.Message.Value.Encode()
				assert.NoError(t, err)
				assert.Equal(t, string(b), v)
				msgSent.Inc()
				cnt.Add(v)
			}
		}(i)
	}

	consumer := client.ConsumerGroup("test_group_1")
	defer consumer.Close()

	err = consumer.ConsumeFunc(context.Background(), []string{topic}, func(_ context.Context, message *sarama.ConsumerMessage) error {
		msgConsumed.Inc()
		cnt.Delete(string(message.Value))
		return nil
	})
	assert.NoError(t, err)

	cnt.Wait()
	err = producer.Close()
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)

	assert.Equal(t, msgSent, msgConsumed)
}

func TestAsyncProducer(t *testing.T) {
	var (
		topic          = generateTopic(16)
		value          = "test_value"
		threadCount    = 4
		msgCount       = 1000
		msgSum         = msgCount * threadCount
		msgSent        = atomic.NewInt32(0)
		msgSentSuccess = atomic.NewInt32(0)
		msgSentError   = atomic.NewInt32(0)
		msgConsumed    = atomic.NewInt32(0)
		cnt            = WaitContainer(msgSum)
	)

	client, err := kafkaCli()
	assert.NoError(t, err)

	producer, err := client.AsyncProducer(topic)
	assert.NoError(t, err)
	defer producer.Close()

	wgSuccess := sync.WaitGroup{}
	wgSuccess.Add(1)
	go func() {
		defer wgSuccess.Done()
		for range producer.Successes() {
			msgSentSuccess.Inc()
		}
	}()

	wgErr := sync.WaitGroup{}
	wgErr.Add(1)
	go func() {
		defer wgErr.Done()
		for range producer.Errors() {
			msgSentSuccess.Inc()
		}
	}()

	for i := 1; i <= threadCount; i++ {
		go func(i int) {
			for j := 0; j < msgCount; j++ {
				v := fmt.Sprintf("sync_%s_%d_%d", value, i, j)
				producer.Produce(sarama.StringEncoder(v))
				msgSent.Inc()
				cnt.Add(v)
			}
		}(i)
	}

	consumer := client.ConsumerGroup("test_group_1")
	defer consumer.Close()

	err = consumer.ConsumeFunc(context.Background(), []string{topic}, func(_ context.Context, message *sarama.ConsumerMessage) error {
		msgConsumed.Inc()
		cnt.Delete(string(message.Value))
		return nil
	})
	assert.NoError(t, err)

	cnt.Wait()
	err = producer.Close()
	assert.NoError(t, err)

	err = consumer.Close()
	assert.NoError(t, err)

	wgSuccess.Wait()
	wgErr.Wait()

	assert.Equal(t, msgSent, msgSentSuccess)
	assert.Equal(t, "0", msgSentError.String())
	assert.Equal(t, msgSent, msgConsumed)
}

func kafkaCli() (*ClientImpl, error) {
	return NewClient(zap.NewNop().Sugar(), &Config{
		KafkaBrokers: []string{"127.0.0.1:9092"},
		KafkaVersion: "2.8.0",
	})
}

func generateTopic(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")
	rand.Seed(time.Now().UnixNano())

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
