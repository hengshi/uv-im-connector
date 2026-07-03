package lark

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/encoding/protowire"
)

const (
	frameMethodControl int32 = 0
	frameMethodData    int32 = 1

	frameHeaderTypeKey   = "type"
	frameHeaderTypePing  = "ping"
	frameHeaderTypePong  = "pong"
	frameHeaderMessageID = "message_id"
	frameHeaderSum       = "sum"
	frameHeaderSeq       = "seq"
)

type frameHeader struct {
	Key   string
	Value string
}

type wsFrame struct {
	SeqID           uint64
	LogID           uint64
	Service         int32
	Method          int32
	Headers         []frameHeader
	PayloadEncoding string
	PayloadType     string
	Payload         []byte
	LogIDNew        string
}

func (f *wsFrame) headerValue(key string) string {
	for _, h := range f.Headers {
		if h.Key == key {
			return h.Value
		}
	}
	return ""
}

func (f *wsFrame) marshal() []byte {
	buf := make([]byte, 0, 64+len(f.Payload))
	buf = protowire.AppendTag(buf, 1, protowire.VarintType)
	buf = protowire.AppendVarint(buf, f.SeqID)
	buf = protowire.AppendTag(buf, 2, protowire.VarintType)
	buf = protowire.AppendVarint(buf, f.LogID)
	buf = protowire.AppendTag(buf, 3, protowire.VarintType)
	buf = protowire.AppendVarint(buf, uint64(uint32(f.Service)))
	buf = protowire.AppendTag(buf, 4, protowire.VarintType)
	buf = protowire.AppendVarint(buf, uint64(uint32(f.Method)))
	for _, h := range f.Headers {
		buf = protowire.AppendTag(buf, 5, protowire.BytesType)
		buf = protowire.AppendVarint(buf, uint64(headerSize(h)))
		buf = protowire.AppendTag(buf, 1, protowire.BytesType)
		buf = protowire.AppendString(buf, h.Key)
		buf = protowire.AppendTag(buf, 2, protowire.BytesType)
		buf = protowire.AppendString(buf, h.Value)
	}
	buf = protowire.AppendTag(buf, 6, protowire.BytesType)
	buf = protowire.AppendString(buf, f.PayloadEncoding)
	buf = protowire.AppendTag(buf, 7, protowire.BytesType)
	buf = protowire.AppendString(buf, f.PayloadType)
	if f.Payload != nil {
		buf = protowire.AppendTag(buf, 8, protowire.BytesType)
		buf = protowire.AppendBytes(buf, f.Payload)
	}
	buf = protowire.AppendTag(buf, 9, protowire.BytesType)
	buf = protowire.AppendString(buf, f.LogIDNew)
	return buf
}

func headerSize(h frameHeader) int {
	return protowire.SizeTag(1) + protowire.SizeBytes(len(h.Key)) +
		protowire.SizeTag(2) + protowire.SizeBytes(len(h.Value))
}

func unmarshalFrame(b []byte) (*wsFrame, error) {
	if len(b) == 0 {
		return nil, errors.New("lark ws frame: empty buffer")
	}
	f := &wsFrame{}
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if err := protowire.ParseError(n); err != nil {
			return nil, fmt.Errorf("consume tag: %w", err)
		}
		b = b[n:]
		switch num {
		case 1:
			v, m, err := consumeVarint(typ, b, "seq_id")
			if err != nil {
				return nil, err
			}
			f.SeqID = v
			b = b[m:]
		case 2:
			v, m, err := consumeVarint(typ, b, "log_id")
			if err != nil {
				return nil, err
			}
			f.LogID = v
			b = b[m:]
		case 3:
			v, m, err := consumeVarint(typ, b, "service")
			if err != nil {
				return nil, err
			}
			f.Service = int32(v)
			b = b[m:]
		case 4:
			v, m, err := consumeVarint(typ, b, "method")
			if err != nil {
				return nil, err
			}
			f.Method = int32(v)
			b = b[m:]
		case 5:
			if typ != protowire.BytesType {
				return nil, fmt.Errorf("field 5 expects bytes")
			}
			raw, m := protowire.ConsumeBytes(b)
			if err := protowire.ParseError(m); err != nil {
				return nil, fmt.Errorf("consume header: %w", err)
			}
			h, err := unmarshalHeader(raw)
			if err != nil {
				return nil, err
			}
			f.Headers = append(f.Headers, h)
			b = b[m:]
		case 6:
			s, m, err := consumeString(typ, b, "payload_encoding")
			if err != nil {
				return nil, err
			}
			f.PayloadEncoding = s
			b = b[m:]
		case 7:
			s, m, err := consumeString(typ, b, "payload_type")
			if err != nil {
				return nil, err
			}
			f.PayloadType = s
			b = b[m:]
		case 8:
			if typ != protowire.BytesType {
				return nil, fmt.Errorf("field 8 expects bytes")
			}
			raw, m := protowire.ConsumeBytes(b)
			if err := protowire.ParseError(m); err != nil {
				return nil, fmt.Errorf("consume payload: %w", err)
			}
			f.Payload = append([]byte(nil), raw...)
			b = b[m:]
		case 9:
			s, m, err := consumeString(typ, b, "log_id_new")
			if err != nil {
				return nil, err
			}
			f.LogIDNew = s
			b = b[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if err := protowire.ParseError(m); err != nil {
				return nil, fmt.Errorf("skip unknown field %d: %w", num, err)
			}
			b = b[m:]
		}
	}
	return f, nil
}

func unmarshalHeader(b []byte) (frameHeader, error) {
	var h frameHeader
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if err := protowire.ParseError(n); err != nil {
			return frameHeader{}, fmt.Errorf("header tag: %w", err)
		}
		b = b[n:]
		switch num {
		case 1:
			s, m, err := consumeString(typ, b, "header key")
			if err != nil {
				return frameHeader{}, err
			}
			h.Key = s
			b = b[m:]
		case 2:
			s, m, err := consumeString(typ, b, "header value")
			if err != nil {
				return frameHeader{}, err
			}
			h.Value = s
			b = b[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, b)
			if err := protowire.ParseError(m); err != nil {
				return frameHeader{}, fmt.Errorf("skip header field %d: %w", num, err)
			}
			b = b[m:]
		}
	}
	return h, nil
}

func consumeVarint(typ protowire.Type, b []byte, field string) (uint64, int, error) {
	if typ != protowire.VarintType {
		return 0, 0, fmt.Errorf("%s expects varint", field)
	}
	v, m := protowire.ConsumeVarint(b)
	if err := protowire.ParseError(m); err != nil {
		return 0, 0, fmt.Errorf("consume %s: %w", field, err)
	}
	return v, m, nil
}

func consumeString(typ protowire.Type, b []byte, field string) (string, int, error) {
	if typ != protowire.BytesType {
		return "", 0, fmt.Errorf("%s expects bytes", field)
	}
	s, m := protowire.ConsumeString(b)
	if err := protowire.ParseError(m); err != nil {
		return "", 0, fmt.Errorf("consume %s: %w", field, err)
	}
	return s, m, nil
}

func newPingFrame(serviceID int32) *wsFrame {
	return &wsFrame{Method: frameMethodControl, Service: serviceID, Headers: []frameHeader{{Key: frameHeaderTypeKey, Value: frameHeaderTypePing}}}
}

func newPongFrame(serviceID int32) *wsFrame {
	return &wsFrame{Method: frameMethodControl, Service: serviceID, Headers: []frameHeader{{Key: frameHeaderTypeKey, Value: frameHeaderTypePong}}}
}

func newAckFrame(inbound *wsFrame, ok bool) *wsFrame {
	code := 200
	if !ok {
		code = 500
	}
	return &wsFrame{Method: inbound.Method, Service: inbound.Service, Headers: inbound.Headers, Payload: []byte(fmt.Sprintf(`{"code":%d,"headers":null,"data":null}`, code))}
}
