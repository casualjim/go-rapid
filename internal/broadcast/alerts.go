package broadcast

import (
	"context"
	"time"

	"google.golang.org/protobuf/encoding/prototext"

	"github.com/xtgo/set"

	"github.com/casualjim/go-rapid/internal/transport"

	"github.com/casualjim/go-rapid/remoting"
	"github.com/rs/zerolog"
)

// Alerts to send batch updates
func Alerts(ctx context.Context, addr *remoting.Endpoint, bc Broadcaster, interval time.Duration, maxSize int) *AlertBatcher {
	stopSignal := make(chan chan struct{})
	queue := make(chan alertMessage, 500)

	sch := &AlertBatcher{
		ctx:      ctx,
		addr:     addr,
		bc:       bc,
		maxSize:  maxSize,
		interval: interval,
		stop:     stopSignal,
		queue:    queue,
	}

	return sch
}

type alertMessage struct {
	msg     *remoting.AlertMessage
	trigger string
}

// AlertBatcher allow for stopping a broadcast batch loop or enqueueing messages to it
type AlertBatcher struct {
	ctx      context.Context
	addr     *remoting.Endpoint
	bc       Broadcaster
	interval time.Duration
	maxSize  int
	stop     chan chan struct{}
	queue    chan alertMessage
}

// CancelAll the scheduled broadcasts
func (s *AlertBatcher) Stop() {
	// sending the last batch of messages might take a little while
	// ensure we block until that has happened
	done := make(chan struct{})
	s.stop <- done
	<-done
}

// Enqueue a message for broadcasting
func (s *AlertBatcher) Enqueue(ctx context.Context, msg *remoting.AlertMessage) {
	s.queue <- alertMessage{
		msg:     msg,
		trigger: transport.RequestIDFromContext(ctx),
	}
}

// Start batching alerts
func (s *AlertBatcher) Start() {
	latch := make(chan struct{})
	go func() {
		close(latch)
		var msgs []alertMessage
		for {
			select {
			case <-time.After(s.interval): // regularly scheduled broadcasting
				s.sendBatch(msgs)
				msgs = nil
			case msg := <-s.queue:
				msgs = append(msgs, msg)
				if s.maxSize > 0 && len(msgs) >= s.maxSize { // drain because we're full
					s.sendBatch(msgs)
					msgs = nil
				}

			case done := <-s.stop: // ok we're done here, but first drain the remaining messages
				s.sendBatch(msgs)
				done <- struct{}{}
				return
			}
		}
	}()
	<-latch
}

func (s *AlertBatcher) sendBatch(alerts []alertMessage) {
	if len(alerts) == 0 {
		return
	}

	msgs := make([]*remoting.AlertMessage, len(alerts))
	var triggers []string
	for i := range alerts {
		msgs[i] = alerts[i].msg
		triggers = set.StringsDo(set.Union, triggers, alerts[i].trigger)
	}

	l := zerolog.Ctx(s.ctx).With().Int("size", len(msgs)).Strs("triggers", triggers)
	if len(triggers) == 1 {
		l = l.Str("request_id", triggers[0])
	}
	lg := l.Logger()
	lg.Info().Msg("sending alert batch")
	req := &remoting.BatchedAlertMessage{
		Sender:   s.addr,
		Messages: msgs,
	}

	bctx := transport.CreateNewRequestID(s.ctx)
	nlg := lg.With().Str("request_id", transport.RequestIDFromContext(bctx)).Logger()
	nlg.Debug().Str("batch", prototext.Format(req)).Msg("batched messages")
	s.bc.Broadcast(nlg.WithContext(bctx), remoting.WrapRequest(req))
}
