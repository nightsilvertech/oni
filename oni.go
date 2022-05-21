package oni

import (
	"context"
	"github.com/segmentio/kafka-go"
	"sync"
)

// Oni copy from kafka.ReaderConfig
type Oni struct {
	kafka.ReaderConfig
}

// Client start contacting to kafka server to start consuming message from topic
func (c *Oni) Client() Engine {
	return &engine{
		reader:   kafka.NewReader(c.ReaderConfig),
		handlers: make(map[string]HandlerFunc),
	}
}

// Engine is application programming interface (API) for oni client that
// represents web framework but for consuming kafka topics with multiple
// target key that assigned to kafka message which sent by kafka producer
type Engine interface {
	// multiReaderInvoker is function that runs multiple kafka reader concurrently
	// this responsible for consuming from multiple HandlerFunc defined by Engine
	multiReaderInvoker(wg *sync.WaitGroup)


	// Handler is function for appending HandlerFunc with specific key to engine implementation
	// to be assigned to engine.handlers that will be invoked concurrently by Commit function
	Handler(key string, handlerFunc HandlerFunc)

	// Commit is function for reading several HandlerFunc that defined by Engine and
	// will be invoked using goroutine through multiReaderInvoker to start consuming
	// topic message seperated by key
	Commit()

	// Close is function that responsible to close reader at the end of main function execution
	Close() error
}

// HandlerFunc is decorator function that will be used for defining handler
// for invoking some various business logic function that belongs to target key
type HandlerFunc func(ctx Context)

// engine is struct implementation instance from Engine interface that
// store information about HandlerFunc and kafka reader to be used by other
// function that belongs to Engine and for Context propagation to HandlerFunc
type engine struct {
	reader   *kafka.Reader
	handlers map[string]HandlerFunc
}

// multiReaderInvoker is function that runs multiple kafka reader concurrently
// this responsible for consuming from multiple HandlerFunc defined by Engine
func (e *engine) multiReaderInvoker(wg *sync.WaitGroup) {
	ctx := context.Background()
	for {
		m, err := e.reader.FetchMessage(ctx)
		if err != nil {
			break
		}
		e.handlers[string(m.Key)](newContext(ctx, e.reader, m))
	}
	defer wg.Done()
}

// Commit is function for reading several HandlerFunc that defined by Engine and
// will be invoked using goroutine through multiReaderInvoker to start consuming
// topic message seperated by key
func (e *engine) Commit() {
	var wg sync.WaitGroup
	wg.Add(len(e.handlers))

	for range e.handlers {
		wg.Add(1)
		go e.multiReaderInvoker(&wg)
	}

	wg.Wait()
}

// Handler is function for appending HandlerFunc with specific key to engine implementation
// to be assigned to engine.handlers that will be invoked concurrently by Commit function
func (e *engine) Handler(key string, handlerFunc HandlerFunc) {
	e.handlers[key] = handlerFunc
}

// Close is function that responsible to close reader at the end of main function execution
func (e *engine) Close() error {
	return e.reader.Close()
}
