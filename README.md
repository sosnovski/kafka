# Client to Kafka based on Sarama

## Examples

### Create client
```go
cli := NewClient(zap.NewNop().Sugar(), &Config{
    KafkaBrokers: []string{"127.0.0.1:9092"},
    KafkaVersion: "2.8.0", 
    OffsetNewest: true,
})
```

### Create consumer
```go
consumer := client.ConsumerGroup("test_group_1")
defer consumer.Close()

err := consumer.ConsumeFunc(context.Background(), []string{topic}, func(ctx context.Context, message *sarama.ConsumerMessage) error {
    // process message
    // if return not nil error, a message wil be commited and error wil be logger 
    return nil
})
```

### Create consumer with retry
```go
import (
    retry "github.com/cenkalti/backoff/v4"
)

consumer := client.ConsumerGroup("test_group_1")
defer consumer.Close()

ctx := context.Background()

err := consumer.ConsumeFunc(ctx, []string{topic}, func(ctx context.Context, message *sarama.ConsumerMessage) error {
    // process message
    // if return not nil error, a message wil be retrying
    // if retrying ended with error, a message wil be commited and error wil be logger
    return nil
}, WithRetryBackOff(retry.WithContext(retry.NewExponentialBackOff(), ctx)))
```

### Create consumer with error handler
```go
consumer := client.ConsumerGroup("test_group_1")
defer consumer.Close()

ctx := context.Background()

err := consumer.ConsumeFunc(ctx, []string{topic}, func(ctx context.Context, message *sarama.ConsumerMessage) error {
    // process message
    // if return not nil error, ErrHandlerFunc wil be called and error wil not be logged
    return nil
}, WithErrHandler(func(ctx context.Context, message *sarama.ConsumerMessage, err error) {
    zap.NewNop().Error("error on consumer message", zap.Error(err)) 
    // or save a message into storage
}))
```

### Create consumer with retry and error handler
```go
import (
    retry "github.com/cenkalti/backoff/v4"
)

consumer := client.ConsumerGroup("test_group_1")
defer consumer.Close()

ctx := context.Background()

err := consumer.ConsumeFunc(ctx, []string{topic}, func(ctx context.Context, message *sarama.ConsumerMessage) error {
    // process message
    // if return not nil error, a message wil be retrying
    // if retrying ended with error, ErrHandlerFunc wil be called and error wil not be logged
    return nil
},
    WithRetryBackOff(retry.WithContext(retry.NewExponentialBackOff(), ctx)),
    WithErrHandler(func(ctx context.Context, message *sarama.ConsumerMessage, err error) {
        zap.NewNop().Error("error on consumer message", zap.Error(err))
        // or save a message into storage
    }),
)
```