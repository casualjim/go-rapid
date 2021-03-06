package eventbus

import (
	"reflect"
	"sync"
	"time"

	"github.com/sasha-s/go-deadlock"

	"github.com/rs/zerolog"
)

// NewEvent creates a new event, named after the args
func NewEvent(args interface{}) Event {
	ea := reflect.Indirect(reflect.ValueOf(args))
	return Event{
		Name: ea.Type().Name(),
		At:   time.Now().UTC(),
		Args: ea.Interface(),
	}
}

// Event you can subscribe to
type Event struct {
	Name string
	At   time.Time
	Args interface{}
}

func (ev Event) MarshalZerologObject(e *zerolog.Event) {
	e.Str("name", ev.Name).Time("at", ev.At).Interface("args", ev.Args)
}

// NOOPHandler drops events on the floor without taking action
var NOOPHandler = Handler(func(_ Event) error { return nil })

// Handler wraps a function that will be called when an event is received
// In this mode the handler is quiet when an error is produced by the handler
// so the user of the eventbus needs to handle that error
func Handler(on func(Event) error) EventHandler {
	return &defaultHandler{
		on: on,
	}
}

// Forward is an event handler that forwards events to another event bus
func Forward(bus EventBus) EventHandler {
	return Handler(func(evt Event) error {
		bus.Publish(evt)
		return nil
	})
}

type defaultHandler struct {
	on func(Event) error
}

// On event trigger
func (h *defaultHandler) On(event Event) error {
	return h.on(event)
}

func newSubscription(log zerolog.Logger, handler EventHandler, errorHandler func(error)) *eventSubscription {
	return &eventSubscription{
		handler: handler,
		onError: errorHandler,
		log:     log,
	}
}

type eventSubscription struct {
	listener chan Event
	handler  EventHandler
	log      zerolog.Logger
	once     sync.Once
	onError  func(error)
}

func (e *eventSubscription) Listen() {
	e.once.Do(func() {
		e.listener = make(chan Event)
		go func(listener chan Event) {
			for evt := range listener {
				e.log.Debug().Object("event", evt).Msg("calling On for event handler")
				if err := e.handler.On(evt); err != nil {
					e.onError(err)
				}
			}
		}(e.listener)
	})
}

func (e *eventSubscription) Stop() {
	close(e.listener)
	e.once = sync.Once{}
}

func (e *eventSubscription) Matches(handler EventHandler) bool {
	return e.handler == handler
}

// EventHandler deals with handling events
type EventHandler interface {
	On(Event) error
}

type filteredHandler struct {
	Next    EventHandler
	Matches EventPredicate
}

func (f *filteredHandler) On(evt Event) error {
	if !f.Matches(evt) {
		return nil
	}
	return f.Next.On(evt)
}

func ToEventType(args interface{}, next EventHandler) EventHandler {
	tn := reflect.Indirect(reflect.ValueOf(args)).Type().Name()
	predicate := func(e Event) bool { return e.Name == tn }
	return Filtered(predicate, next)
}

// EventPredicate for filtering events
type EventPredicate func(Event) bool

// Filtered composes an event handler with a filter
func Filtered(matches EventPredicate, next EventHandler) EventHandler {
	return &filteredHandler{
		Matches: matches,
		Next:    next,
	}
}

// NopBus represents a zero value for an event bus
var NopBus EventBus = &nopBus{}

type nopBus struct {
}

func (b *nopBus) Close() error                { return nil }
func (b *nopBus) Publish(Event)               {}
func (b *nopBus) Subscribe(...EventHandler)   {}
func (b *nopBus) Unsubscribe(...EventHandler) {}
func (b *nopBus) Len() int                    { return 0 }

// EventBus does fanout to registered channels
type EventBus interface {
	Close() error
	Publish(Event)
	Subscribe(...EventHandler)
	Unsubscribe(...EventHandler)
	Len() int
}

type defaultEventBus struct {
	lock *deadlock.RWMutex

	channel      chan Event
	handlers     []*eventSubscription
	closing      chan chan struct{}
	log          zerolog.Logger
	errorHandler func(error)
}

// New event bus with specified logger
func New(log zerolog.Logger) EventBus {
	return NewWithTimeout(log, 100*time.Millisecond)
}

// NewWithTimeout creates a new eventbus with a timeout after which an event handler gets canceled
func NewWithTimeout(log zerolog.Logger, timeout time.Duration) EventBus {
	e := &defaultEventBus{
		closing:      make(chan chan struct{}),
		channel:      make(chan Event, 100),
		log:          log,
		lock:         new(deadlock.RWMutex),
		errorHandler: func(err error) { log.Debug().Msg(err.Error()) },
	}
	go e.dispatcherLoop(timeout)
	return e
}

func (e *defaultEventBus) dispatcherLoop(timeout time.Duration) {
	totWait := new(sync.WaitGroup)
	for {
		select {
		case evt := <-e.channel:
			e.log.Debug().Object("event", evt).Msg("Got event in channel")

			totWait.Add(1)
			e.lock.RLock()
			sz := len(e.handlers)
			if sz == 0 {
				e.log.Debug().Msg("there are no active listeners, skipping broadcast")
				e.lock.RUnlock()
				totWait.Done()
				continue
			}

			evts := make(chan chan<- Event, sz)
			for _, handler := range e.handlers {
				evts <- handler.listener
			}
			close(evts)
			e.lock.RUnlock()

			for handler := range evts {
				go func(handler chan<- Event) {
					//defer wg.Done()
					timer := time.NewTimer(timeout)
					select {
					case handler <- evt:
						e.log.Debug().Object("event", evt).Msg("raised event in channel")
						timer.Stop()
					case <-timer.C:
						e.log.Error().Object("event", evt).Dur("timeout", timeout).Msg("sending to listener timed out")
					}
				}(handler)
			}
			totWait.Done()
		case closed := <-e.closing:
			totWait.Wait()
			close(e.channel)
			e.lock.Lock()
			for _, h := range e.handlers {
				h.Stop()
			}
			e.handlers = nil
			e.lock.Unlock()

			closed <- struct{}{}
			close(closed)
			e.log.Info().Msg("event bus closed")
			return
		}
	}
}

// SetErrorHandler changes the default error handler which logs as error
// to the new error handler provided to this method
func (e *defaultEventBus) SetErrorHandler(handler func(error)) {
	e.lock.Lock()
	e.errorHandler = handler
	e.lock.Unlock()
}

// Publish an event to all interested subscribers
func (e *defaultEventBus) Publish(evt Event) {
	e.channel <- evt
}

// Subscribe to events published in the bus
func (e *defaultEventBus) Subscribe(handlers ...EventHandler) {
	e.lock.Lock()
	e.log.Info().Int("count", len(handlers)).Msg("adding listeners")
	for _, handler := range handlers {
		sub := newSubscription(e.log, handler, e.errorHandler)
		e.handlers = append(e.handlers, sub)
		sub.Listen()
	}
	e.lock.Unlock()
}

func (e *defaultEventBus) Unsubscribe(handlers ...EventHandler) {
	e.lock.Lock()
	if len(e.handlers) == 0 {
		e.log.Info().Int("count", len(handlers)).Msg("nothing to remove from handlers")
		e.lock.Unlock()
		return
	}
	e.log.Debug().Int("count", len(handlers)).Msg("removing listeners")
	for _, h := range handlers {
		for i, handler := range e.handlers {
			if handler.Matches(h) {
				handler.Stop()
				// replace handler because it will still process messages in flight
				e.handlers = append(e.handlers[:i], e.handlers[i+1:]...)
				break
			}
		}
	}
	e.lock.Unlock()
}

func (e *defaultEventBus) Close() error {
	e.log.Info().Msg("closing eventbus")
	ch := make(chan struct{})
	e.closing <- ch
	<-ch
	close(e.closing)

	return nil
}

func (e *defaultEventBus) Len() int {
	e.log.Debug().Msg("getting the length of the handlers")
	e.lock.RLock()
	sz := len(e.handlers)
	e.lock.RUnlock()
	return sz
}
