package broadcast

import (
	"context"
	"time"

	"github.com/casualjim/go-rapid/remoting"
	"github.com/gogo/protobuf/proto"
	"go.uber.org/zap"
)

// Alerts to send batch updates
func Alerts(log *zap.Logger, addr *remoting.Endpoint, bc Broadcaster, interval time.Duration, maxSize int) *AlertBatcher {
	stopSignal := make(chan chan struct{})
	queue := make(chan *remoting.AlertMessage, 500)

	sch := &AlertBatcher{
		log:      log,
		addr:     addr,
		bc:       bc,
		maxSize:  maxSize,
		interval: interval,
		stop:     stopSignal,
		queue:    queue,
	}

	return sch
}

// AlertBatcher allow for stopping a broadcast batch loop or enqueueing messages to it
type AlertBatcher struct {
	log      *zap.Logger
	addr     *remoting.Endpoint
	bc       Broadcaster
	interval time.Duration
	maxSize  int
	stop     chan chan struct{}
	queue    chan *remoting.AlertMessage
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
func (s *AlertBatcher) Enqueue(msg *remoting.AlertMessage) {
	s.queue <- msg
}

// Start batching alerts
func (s *AlertBatcher) Start() {
	latch := make(chan struct{})
	go func() {
		close(latch)
		var msgs []*remoting.AlertMessage
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

func (s *AlertBatcher) sendBatch(msgs []*remoting.AlertMessage) {
	if len(msgs) == 0 {
		return
	}

	s.log.Info("sending alert batch", zap.Int("size", len(msgs)))
	req := &remoting.BatchedAlertMessage{
		Sender:   s.addr,
		Messages: msgs,
	}
	s.log.Debug("batched messages", zap.String("batch", proto.CompactTextString(req)))
	s.bc.Broadcast(context.Background(), remoting.WrapRequest(req))
}
