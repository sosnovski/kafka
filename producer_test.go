package kafka

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

type kafkaTestSuite struct {
	suite.Suite
	container testcontainers.Container
	cli       *ClientImpl
}

func TestKafkaTestSuite(t *testing.T) {
	suite.Run(t, new(kafkaTestSuite))
}

func (s *kafkaTestSuite) TestSyncProducer() {
	var (
		t           = s.T()
		topic       = generate(16)
		group       = generate(16)
		value       = "test_value"
		threadCount = 4
		msgCount    = 1000
		msgSum      = msgCount * threadCount
		msgSent     = atomic.NewInt32(0)
		msgConsumed = atomic.NewInt32(0)
		cnt         = waitContainer(msgSum)
	)

	producer, err := s.cli.SyncProducer(topic)
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

	consumer := s.cli.ConsumerGroup(group)
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

func (s *kafkaTestSuite) TestWithResetOffset() {
	var (
		t           = s.T()
		topic       = generate(16)
		group       = generate(16)
		value       = "test_value"
		threadCount = 4
		msgCount    = 1000
		resetOffset = int64(978)
		msgSum      = msgCount * threadCount
		msgSent     = atomic.NewInt32(0)
		msgConsumed = atomic.NewInt32(0)
		cnt         = waitContainer(msgSum - int(resetOffset))
	)

	producer, err := s.cli.SyncProducer(topic)
	assert.NoError(t, err)
	defer producer.Close()

	for i := 1; i <= threadCount; i++ {
		for j := 0; j < msgCount; j++ {
			v := fmt.Sprintf("sync_%s_%d_%d", value, i, j)
			cnt.Add(v)
			go func(i, j int) {
				msg, err := producer.Produce(sarama.StringEncoder(v))
				assert.NoError(t, err)
				assert.NotNil(t, msg)
				msgSent.Inc()
			}(i, j)
		}
	}

	consumer := s.cli.ConsumerGroup(group)
	defer consumer.Close()

	err = consumer.ConsumeFunc(context.Background(), []string{topic}, func(_ context.Context, message *sarama.ConsumerMessage) error {
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

func (s *kafkaTestSuite) TestAsyncProducerCh() {
	var (
		t           = s.T()
		topic       = generate(16)
		group       = generate(16)
		value       = "test_value"
		threadCount = 4
		msgCount    = 1000
		msgSum      = msgCount * threadCount
		msgSent     = atomic.NewInt32(0)
		msgConsumed = atomic.NewInt32(0)
		cnt         = waitContainer(msgSum)
	)

	producer, err := s.cli.AsyncProducer(topic)
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

	consumer := s.cli.ConsumerGroup(group)
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

func (s *kafkaTestSuite) TestAsyncProducer() {
	var (
		t              = s.T()
		topic          = generate(16)
		group          = generate(16)
		value          = "test_value"
		threadCount    = 4
		msgCount       = 1000
		msgSum         = msgCount * threadCount
		msgSent        = atomic.NewInt32(0)
		msgSentSuccess = atomic.NewInt32(0)
		msgSentError   = atomic.NewInt32(0)
		msgConsumed    = atomic.NewInt32(0)
		cnt            = waitContainer(msgSum)
	)

	producer, err := s.cli.AsyncProducer(topic)
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

	consumer := s.cli.ConsumerGroup(group)
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

func (s *kafkaTestSuite) SetupSuite() {
	t := s.T()

	kafkaContainer, err := kafka.Run(t.Context(),
		"confluentinc/confluent-local:7.5.0",
		kafka.WithClusterID("test-cluster"),
	)
	assert.NoError(t, err)

	brokers, err := kafkaContainer.Brokers(t.Context())
	assert.NoError(t, err)
	t.Log(brokers)

	s.cli, err = NewClient(zap.NewNop(), &Config{Brokers: brokers, Version: "2.8.0"})
	assert.NoError(t, err)

	s.container = kafkaContainer
}

func (s *kafkaTestSuite) TearDownSuite() {
	if s.cli != nil {
		assert.NoError(s.T(), s.cli.Close())
	}

	if s.container != nil {
		assert.NoError(s.T(), testcontainers.TerminateContainer(s.container))
	}
}

type container struct {
	wg           sync.WaitGroup
	mu           sync.Mutex
	sentMessages map[string]struct{}
}

func (c *container) Add(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sentMessages[msg] = struct{}{}
}

func (c *container) Delete(msg string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.sentMessages, msg)
	c.wg.Done()
}

func (c *container) Wait() {
	c.wg.Wait()
}

func generate(n int) string {
	var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}

	return string(b)
}

func waitContainer(expectCount int) *container {
	cnt := &container{sentMessages: make(map[string]struct{})}
	cnt.wg.Add(expectCount)
	return cnt
}
