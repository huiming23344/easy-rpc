package codec

import (
	"bufio"
	"encoding/json"
	"io"
	"log"
)

type JsonCodec struct {
	conn io.ReadWriteCloser
	buf  *bufio.Writer
	dec  *json.Decoder
	enc  *json.Encoder
}

func NewJsonCodec(conn io.ReadWriteCloser) Codec {
	buf := bufio.NewWriter(conn)
	return &JsonCodec{
		conn: conn,
		buf:  buf,
		enc:  json.NewEncoder(buf),
		dec:  json.NewDecoder(conn),
	}
}

var _ Codec = (*JsonCodec)(nil)

func (j *JsonCodec) Close() error {
	return j.conn.Close()
}

func (j *JsonCodec) ReadHeader(h *Header) error {
	return j.dec.Decode(h)
}

func (j *JsonCodec) ReadBody(b interface{}) error {
	return j.dec.Decode(b)
}

func (j *JsonCodec) Write(h *Header, b interface{}) (err error) {
	defer func() {
		_ = j.buf.Flush()
		if err != nil {
			_ = j.conn.Close()
		}
	}()
	if err := j.enc.Encode(h); err != nil {
		log.Println("rpc codec: gob error encoding header:", err)
		return err
	}
	if err := j.enc.Encode(b); err != nil {
		log.Println("rpc codec: gob error encoding body:", err)
		return err
	}
	return nil
}
