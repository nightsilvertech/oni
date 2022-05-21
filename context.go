package oni

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
)

type oniCtx struct {
	ctx     context.Context
	message kafka.Message
	reader  *kafka.Reader
}

// Context is propagation consume result that used by HandlerFunc decoration func
// for passing the result of consume process
type Context interface {
	// BindValue is function for binding bytes of message value to
	// a struct definition through pointer
	BindValue(v interface{}) error

	// RawValue is function for throws bytes of message value without
	// any modification through Context
	RawValue() []byte

	// Value is function for throws string converted of message value
	// any modification through Context
	Value() string

	// RawKey is function for throws bytes of message key without
	// any modification through Context
	RawKey() []byte

	// Key is function for throws string converted of message key
	// any modification through Context
	Key() string

	// Ack is function to acknowledge the message from topic and
	// after Ack message will not exist inside topic
	Ack() error
}

// BindValue is function for binding bytes of message value to
// a struct definition through pointer
func (ctx *oniCtx) BindValue(v interface{}) error {
	return json.Unmarshal(ctx.message.Value, v)
}

// RawValue is function for throws bytes of message value without
// any modification through Context
func (ctx *oniCtx) RawValue() []byte {
	return ctx.message.Value
}

// Value is function for throws string converted of message value
// any modification through Context
func (ctx *oniCtx) Value() string {
	return string(ctx.message.Value)
}

// RawKey is function for throws bytes of message key without
// any modification through Context
func (ctx *oniCtx) RawKey() []byte {
	return ctx.message.Key
}

// Key is function for throws string converted of message key
// any modification through Context
func (ctx *oniCtx) Key() string {
	return string(ctx.message.Key)
}

// Ack is function to acknowledge the message from topic and
// after Ack message will not exist inside topic
func (ctx *oniCtx) Ack() error {
	return ctx.reader.CommitMessages(ctx.ctx, ctx.message)
}

// newContext is private function that generate Context to HandlerFunc
// to be used for consume information
func newContext(ctx context.Context, reader *kafka.Reader, message kafka.Message) Context {
	return &oniCtx{
		ctx:     ctx,
		reader:  reader,
		message: message,
	}
}
