package kafka

import (
	"github.com/IBM/sarama"
)

type Config struct {
	Brokers      []string
	Version      string
	OffsetNewest bool
}

type SASL struct {
	Mechanism sarama.SASLMechanism
	User      string
	Password  string
	Enabled   bool
}
