package kafka

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

var (
	SaramaPanicHandler func(i interface{})
)

type (
	Client interface {
		// Consumer You must call Close() on sarama.Consumer
		Consumer(options ...ConfigOption) (sarama.Consumer, error)
		// ConsumerGroup You must call ConsumerGroup.Close()
		ConsumerGroup(groupID string, options ...ConfigOption) ConsumerGroup
		// SyncProducer You must call SyncProducer.Close()
		SyncProducer(topic string, options ...ConfigOption) (SyncProducer, error)
		// AsyncProducer You must call AsyncProducer.Close()
		AsyncProducer(topic string, options ...ConfigOption) (AsyncProducer, error)
	}

	ConfigOption = func(config *sarama.Config)

	ClientImpl struct {
		logger *zap.Logger
		sarama.Client
		brokers []string
	}
)

func NewClient(logger *zap.Logger, cfg *Config, options ...ConfigOption) (*ClientImpl, error) {
	panicHandler := SaramaPanicHandler
	if panicHandler == nil {
		panicHandler = func(i interface{}) {
			logger.Error("sarama panic", zap.Any("err", i))
		}
	}

	if sarama.PanicHandler == nil {
		sarama.PanicHandler = panicHandler
	}

	saramaCfg := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(cfg.Version)
	if err != nil {
		return nil, fmt.Errorf("failed to parse kafka version %s: %w", cfg.Version, err)
	}

	saramaCfg.Version = version

	saramaCfg.Consumer.Offsets.AutoCommit.Enable = true
	saramaCfg.Consumer.Offsets.AutoCommit.Interval = time.Millisecond * 500

	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	if cfg.OffsetNewest {
		saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	}

	for _, option := range options {
		option(saramaCfg)
	}

	saramaCli, err := sarama.NewClient(cfg.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &ClientImpl{
		logger:  logger,
		brokers: cfg.Brokers,
		Client:  saramaCli,
	}, nil
}

// Consumer You must call Close() on sarama.Consumer
func (c *ClientImpl) Consumer(options ...ConfigOption) (sarama.Consumer, error) {
	return sarama.NewConsumer(c.brokers, withOptions(*c.Client.Config(), options...))
}

// ConsumerGroup You must call ConsumerGroup.Close()
func (c *ClientImpl) ConsumerGroup(groupID string, options ...ConfigOption) ConsumerGroup {
	return NewConsumerGroup(c.logger, c.brokers, withOptions(*c.Client.Config(), options...), groupID)
}

// SyncProducer You must call SyncProducer.Close()
func (c *ClientImpl) SyncProducer(topic string, options ...ConfigOption) (SyncProducer, error) {
	return NewSyncProducer(c.brokers, withOptions(*c.Client.Config(), options...), topic)
}

// AsyncProducer You must call AsyncProducer.Close()
func (c *ClientImpl) AsyncProducer(topic string, options ...ConfigOption) (AsyncProducer, error) {
	return NewAsyncProducer(c.brokers, withOptions(*c.Client.Config(), options...), topic)
}

func withOptions(config sarama.Config, options ...ConfigOption) *sarama.Config {
	for _, option := range options {
		option(&config)
	}

	return &config
}
