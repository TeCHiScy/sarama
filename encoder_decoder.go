package sarama

import (
	"fmt"
)

// Encoder is the interface that wraps the basic Encode method.
// Anything implementing Encoder can be turned into bytes using Kafka's encoding rules.
type encoder interface {
	encode(pe packetEncoder) error
}

type encoderWithHeader interface {
	encoder
	headerVersion() int16
}

// Encode takes an Encoder and turns it into bytes while potentially recording metrics.
func encode(e encoder, metrics *Metrics) ([]byte, error) {
	if e == nil {
		return nil, nil
	}

	var prepEnc prepEncoder
	var realEnc realEncoder

	if err := e.encode(&prepEnc); err != nil {
		return nil, err
	}

	if prepEnc.length < 0 || prepEnc.length > int(MaxRequestSize) {
		return nil, PacketEncodingError{fmt.Sprintf("invalid request size (%d)", prepEnc.length)}
	}

	realEnc.raw = make([]byte, prepEnc.length)
	realEnc.metrics = metrics
	if err := e.encode(&realEnc); err != nil {
		return nil, err
	}

	return realEnc.raw, nil
}

// decoder is the interface that wraps the basic Decode method.
// Anything implementing Decoder can be extracted from bytes using Kafka's encoding rules.
type decoder interface {
	decode(pd packetDecoder) error
}

type versionedDecoder interface {
	decode(pd packetDecoder, version int16) error
}

// decode takes bytes and a decoder and fills the fields of the decoder from the bytes,
// interpreted using Kafka's encoding rules.
func decode(buf []byte, in decoder, metrics *Metrics) error {
	if buf == nil {
		return nil
	}

	helper := realDecoder{
		raw:     buf,
		metrics: metrics,
	}
	if err := in.decode(&helper); err != nil {
		return err
	}

	if helper.off != len(buf) {
		return PacketDecodingError{"invalid length"}
	}

	return nil
}

func versionedDecode(buf []byte, in versionedDecoder, version int16, metrics *Metrics) error {
	if buf == nil {
		return nil
	}

	helper := realDecoder{
		raw:     buf,
		metrics: metrics,
	}
	if err := in.decode(&helper, version); err != nil {
		return err
	}

	if helper.off != len(buf) {
		return PacketDecodingError{
			Info: fmt.Sprintf("invalid length (off=%d, len=%d)", helper.off, len(buf)),
		}
	}

	return nil
}
