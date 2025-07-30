package kafka

import (
	"crypto/tls"

	"github.com/IBM/sarama"
)

func AuthOption(sasl SASL) ConfigOption {
	return func(config *sarama.Config) {
		config.Net.SASL.Enable = sasl.Enabled
		config.Net.SASL.Handshake = true
		config.Net.SASL.Mechanism = sasl.Mechanism
		config.Net.SASL.User = sasl.User
		config.Net.SASL.Password = sasl.Password
		config.Net.TLS.Enable = sasl.Enabled
		config.Net.TLS.Config = &tls.Config{InsecureSkipVerify: true}

		switch config.Net.SASL.Mechanism {
		case sarama.SASLTypeSCRAMSHA256:
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &xdgSCRAMClient{
					HashGeneratorFcn: SHA256,
				}
			}
		case sarama.SASLTypeSCRAMSHA512:
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &xdgSCRAMClient{
					HashGeneratorFcn: SHA512,
				}
			}
		}
	}
}
