package kafka

import (
	"github.com/IBM/sarama"
	"google.golang.org/protobuf/proto"
)

type protoEncoder struct {
	msg     proto.Message
	b       []byte
	err     error
	encoded bool
}

func ProtoEncoder(msg proto.Message) sarama.Encoder {
	return &protoEncoder{
		msg: msg,
	}
}

func (p *protoEncoder) Encode() ([]byte, error) {
	if !p.encoded {
		p.b, p.err = proto.Marshal(p.msg)
		p.encoded = true
	}

	if p.err != nil {
		return nil, p.err
	}

	return p.b, nil
}

func (p *protoEncoder) Length() int {
	b, _ := p.Encode()
	return len(b)
}
