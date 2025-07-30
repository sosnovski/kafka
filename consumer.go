package kafka

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/IBM/sarama"
	retry "github.com/cenkalti/backoff/v4"
	"go.uber.org/zap"
)

type (
	ConsumerGroup interface {
		ConsumeFunc(ctx context.Context, topics []string, handlerFunc HandlerFunc, options ...ConsumerHandlerOption) error
		Close() error
	}

	// HandlerFunc if an error is returned, the topic message will be marked as used and error wil be logged if ErrHandlerFunc is not set
	HandlerFunc func(context.Context, *sarama.ConsumerMessage) error

	// ErrHandlerFunc invoked if HandlerFunc returned an error
	ErrHandlerFunc func(context.Context, *sarama.ConsumerMessage, error)

	ConsumerHandlerOption func(c *ConsumerHandler)

	closerErr struct { //nolint:errname
		err []error
	}

	ConsumerHandler struct {
		logger       *zap.Logger
		Handler      HandlerFunc
		ErrHandler   ErrHandlerFunc
		RetryBackOff retry.BackOff
		OnSetup      []func(sarama.ConsumerGroupSession) error
		OnCleanup    []func(sarama.ConsumerGroupSession) error
	}

	consumerGroup struct {
		closers      []io.Closer
		logger       *zap.Logger
		saramaConfig *sarama.Config
		addresses    []string
		groupID      string
	}
)

func NewConsumerGroup(
	logger *zap.Logger,
	addresses []string,
	config *sarama.Config,
	groupID string,
) ConsumerGroup {
	config.Consumer.Return.Errors = true

	return &consumerGroup{
		logger:       logger,
		saramaConfig: config,
		addresses:    addresses,
		groupID:      groupID,
	}
}

func (c *consumerGroup) ConsumeFunc(
	ctx context.Context,
	topics []string,
	handlerFunc HandlerFunc,
	options ...ConsumerHandlerOption,
) error {
	saramaConsumer, err := sarama.NewConsumerGroup(c.addresses, c.groupID, c.saramaConfig)
	if err != nil {
		return fmt.Errorf("failed to create kafka consumer group from client: %w", err)
	}

	c.appendCloser(saramaConsumer)

	go func() {
		for err := range saramaConsumer.Errors() {
			c.logger.Error("consumer group error",
				zap.Error(err),
			)
		}
	}()

	handler := &ConsumerHandler{logger: c.logger, Handler: handlerFunc}

	for _, option := range options {
		option(handler)
	}

	go func() {
		for {
			if err := saramaConsumer.Consume(ctx, topics, handler); err != nil {
				c.logger.Error("failed to consume by consumer group %s: %s", zap.String("groupId", c.groupID), zap.Error(err))
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()

	return nil
}

func (c *consumerGroup) Close() error {
	var errs []error
	for _, closer := range c.closers {
		if err := closer.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	if len(errs) > 0 {
		return &closerErr{err: errs}
	}

	return nil
}

func (c *consumerGroup) appendCloser(closer io.Closer) {
	c.closers = append(c.closers, closer)
}

func (h *ConsumerHandler) Setup(session sarama.ConsumerGroupSession) error {
	for _, onSetup := range h.OnSetup {
		if err := onSetup(session); err != nil {
			return err
		}
	}

	return nil
}

func (h *ConsumerHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	session.Commit()

	for _, onCleanup := range h.OnCleanup {
		if err := onCleanup(session); err != nil {
			return err
		}
	}

	return nil
}

func (h *ConsumerHandler) ConsumeClaim(
	session sarama.ConsumerGroupSession,
	claim sarama.ConsumerGroupClaim,
) (err error) {
	defer func() {
		if r := recover(); r != nil {
			switch v := r.(type) {
			case string:
				err = errors.New(v)
			case error:
				err = v
			default:
				err = fmt.Errorf("%v", v)
			}
		}
	}()

	for message := range claim.Messages() {
		var err error
		if h.RetryBackOff != nil {
			err = retry.Retry(func() error {
				return h.Handler(session.Context(), message)
			}, h.RetryBackOff)
		} else {
			err = h.Handler(session.Context(), message)
		}

		if err != nil {
			if h.ErrHandler != nil {
				h.ErrHandler(session.Context(), message, err)
			} else {
				h.logger.Error("failed to handle message",
					zap.Error(err),
					zap.ByteString("key", message.Key),
					zap.String("topic", message.Topic),
					zap.Int32("partition", message.Partition),
					zap.Int64("offset", message.Offset),
				)
			}
		}

		session.MarkMessage(message, "")
	}

	return err
}

func (c closerErr) Error() string {
	return fmt.Sprintf("closers error: %v", c.err)
}

// WithRetryBackOff
// Setup of retries if HandlerFunc returns an error
func WithRetryBackOff(retryBackOff retry.BackOff) ConsumerHandlerOption {
	return func(h *ConsumerHandler) {
		h.RetryBackOff = retryBackOff
	}
}

// WithErrHandler
// Setup ErrHandlerFunc for handle errors from HandlerFunc
func WithErrHandler(errHandled ErrHandlerFunc) ConsumerHandlerOption {
	return func(h *ConsumerHandler) {
		h.ErrHandler = errHandled
	}
}

// WithResetToOldestOffset
// Offsets offset back. Use if you need to re-read messages that were read by this consumer group.
func WithResetToOldestOffset(partition int32, offset int64, topic, meta string) ConsumerHandlerOption {
	once := sync.Once{}

	return func(h *ConsumerHandler) {
		h.OnSetup = append(h.OnSetup, func(session sarama.ConsumerGroupSession) error {
			once.Do(func() {
				session.ResetOffset(topic, partition, offset, meta)
				session.Commit()
			})
			return nil
		})
	}
}

// WithResetToNewestOffset
// Shifts offset forward. Use if you need to shift the offset forward after creating a consumer group.
func WithResetToNewestOffset(partition int32, offset int64, topic, meta string) ConsumerHandlerOption {
	once := sync.Once{}

	return func(h *ConsumerHandler) {
		h.OnSetup = append(h.OnSetup, func(session sarama.ConsumerGroupSession) error {
			once.Do(func() {
				session.MarkOffset(topic, partition, offset, meta)
				session.Commit()
			})
			return nil
		})
	}
}
