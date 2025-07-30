package kafka

import (
	"fmt"
	"sync"

	"github.com/IBM/sarama"
)

type (
	SyncProducer interface {
		Produce(value sarama.Encoder, options ...MsgOption) (*sarama.ProducerMessage, error)
		ProduceBatch(items []Item) ([]*sarama.ProducerMessage, error)
		Close() error
	}

	AsyncProducer interface {
		Produce(value sarama.Encoder, options ...MsgOption)
		ProduceCh(value sarama.Encoder, options ...MsgOption) <-chan *AsyncResult
		Successes() <-chan *sarama.ProducerMessage
		Errors() <-chan *sarama.ProducerError
		Close() error
	}

	AsyncResult struct {
		Message *sarama.ProducerMessage
		Error   error
	}

	MsgOption func(*sarama.ProducerMessage)

	syncProducer struct {
		saramaProducer sarama.SyncProducer
		saramaConfig   *sarama.Config
		address        []string
		topic          string
	}

	asyncProducer struct {
		mu             sync.RWMutex
		wg             sync.WaitGroup
		errCh          chan *sarama.ProducerError
		successCh      chan *sarama.ProducerMessage
		wait           map[*sarama.ProducerMessage]chan *AsyncResult
		saramaProducer sarama.AsyncProducer
		saramaConfig   *sarama.Config
		address        []string
		topic          string
	}
)

// NewSyncProducer create new copy of sarama.Client
// You must call SyncProducer.Close()
func NewSyncProducer(address []string, config *sarama.Config, topic string) (SyncProducer, error) {
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	saramaProducer, err := sarama.NewSyncProducer(address, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync producer: %w", err)
	}

	return &syncProducer{
		saramaProducer: saramaProducer,
		saramaConfig:   config,
		address:        address,
		topic:          topic,
	}, nil
}

// NewAsyncProducer create new copy of sarama.Client
// You must call AsyncProducer.Close()
func NewAsyncProducer(address []string, config *sarama.Config, topic string) (AsyncProducer, error) {
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	saramaProducer, err := sarama.NewAsyncProducer(address, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create sync producer: %w", err)
	}

	producer := &asyncProducer{
		wait:           make(map[*sarama.ProducerMessage]chan *AsyncResult),
		saramaProducer: saramaProducer,
		saramaConfig:   config,
		address:        address,
		topic:          topic,
	}

	producer.wg.Add(2)
	go producer.readSuccess()
	go producer.readError()

	producer.wg.Wait()

	return producer, nil
}

func (p *syncProducer) Produce(value sarama.Encoder, options ...MsgOption) (*sarama.ProducerMessage, error) {
	message := messageWithOptions(value, p.topic, options...)

	if _, _, err := p.saramaProducer.SendMessage(message); err != nil {
		return nil, err
	}

	return message, nil
}

type Item struct {
	Value   sarama.Encoder
	Options []MsgOption
}

func (p *syncProducer) ProduceBatch(items []Item) ([]*sarama.ProducerMessage, error) {
	var messages []*sarama.ProducerMessage
	for _, item := range items {
		messages = append(messages, messageWithOptions(item.Value, p.topic, item.Options...))
	}

	if err := p.saramaProducer.SendMessages(messages); err != nil {
		return nil, err
	}

	return messages, nil
}

func (p *syncProducer) Close() error {
	return p.saramaProducer.Close()
}

func (p *asyncProducer) Produce(value sarama.Encoder, options ...MsgOption) {
	p.saramaProducer.Input() <- messageWithOptions(value, p.topic, options...)
}

func (p *asyncProducer) ProduceCh(value sarama.Encoder, options ...MsgOption) <-chan *AsyncResult {
	message := messageWithOptions(value, p.topic, options...)
	ch := make(chan *AsyncResult, 1)
	p.addWait(message, ch)

	p.saramaProducer.Input() <- message

	return ch
}

func (p *asyncProducer) Successes() <-chan *sarama.ProducerMessage {
	return p.successCh
}

func (p *asyncProducer) Errors() <-chan *sarama.ProducerError {
	return p.errCh
}

func (p *asyncProducer) Close() error {
	return p.saramaProducer.Close()
}

func (p *asyncProducer) readSuccess() {
	p.successCh = make(chan *sarama.ProducerMessage)
	defer close(p.successCh)

	p.wg.Done()

	for message := range p.saramaProducer.Successes() {
		ch, ok := p.isWait(message)
		if ok {
			ch <- &AsyncResult{
				Message: message,
			}
			p.closeAndDeleteWait(message)
			continue
		}

		p.successCh <- message
	}
}

func (p *asyncProducer) readError() {
	p.errCh = make(chan *sarama.ProducerError)
	defer close(p.errCh)

	p.wg.Done()

	for err := range p.saramaProducer.Errors() {
		ch, ok := p.isWait(err.Msg)
		if ok {
			ch <- &AsyncResult{
				Message: err.Msg,
				Error:   err.Err,
			}
			p.closeAndDeleteWait(err.Msg)
			continue
		}

		p.errCh <- err
	}
}

func (p *asyncProducer) addWait(message *sarama.ProducerMessage, ch chan *AsyncResult) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.wait[message] = ch
}

func (p *asyncProducer) closeAndDeleteWait(message *sarama.ProducerMessage) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if ch, ok := p.wait[message]; ok {
		close(ch)
		delete(p.wait, message)
	}
}

func (p *asyncProducer) isWait(message *sarama.ProducerMessage) (chan *AsyncResult, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	ch, ok := p.wait[message]
	return ch, ok
}

func WithMsgKey(key sarama.Encoder) MsgOption {
	return func(message *sarama.ProducerMessage) {
		message.Key = key
	}
}

func WithMsgHeaders(headers []sarama.RecordHeader) MsgOption {
	return func(message *sarama.ProducerMessage) {
		message.Headers = headers
	}
}

func WithMsgMeta(meta interface{}) MsgOption {
	return func(message *sarama.ProducerMessage) {
		message.Metadata = meta
	}
}

func messageWithOptions(value sarama.Encoder, topic string, options ...MsgOption) *sarama.ProducerMessage {
	message := &sarama.ProducerMessage{
		Value: value,
		Topic: topic,
	}

	for _, option := range options {
		option(message)
	}

	return message
}
