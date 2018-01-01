package broadcast

// // ScheduledBroadcasts allow for stopping a broadcast batch loop or enqueueing messages to it
// type ScheduledBroadcasts struct {
// 	stop  chan<- chan struct{}
// 	queue chan<- remoting.LinkUpdateMessage
// }

// // Stop the scheduled broadcasts
// func (s *ScheduledBroadcasts) Stop() {
// 	// sending the last batch of messages might take a little while
// 	// ensure we block until that has happened
// 	done := make(chan struct{})
// 	s.stop <- done
// 	<-done
// }

// // Enqueue a message for broadcasting
// func (s *ScheduledBroadcasts) Enqueue(msg remoting.LinkUpdateMessage) {
// 	s.queue <- msg
// }

// // Schedule to send batch updates
// func Schedule(bc Broadcaster, interval time.Duration, maxSize int) *ScheduledBroadcasts {
// 	stopSignal := make(chan chan struct{})
// 	queue := make(chan remoting.LinkUpdateMessage, 100)

// 	sch := &ScheduledBroadcasts{
// 		stop:  stopSignal,
// 		queue: queue,
// 	}

// 	ticker := time.NewTicker(interval)
// 	go func() {
// 		var msgs []*remoting.LinkUpdateMessage
// 		for {
// 			select {
// 			case <-ticker.C: // regularly scheduled broadcasting
// 				sendBatch(bc, msgs)
// 				msgs = nil
// 			case msg := <-queue:
// 				msgs = append(msgs, &msg)
// 				if len(msgs) > maxSize { // drain because we're full
// 					sendBatch(bc, msgs)
// 					msgs = nil
// 				}

// 			case done := <-stopSignal: // ok we're done here, but first drain the remaining messages
// 				ticker.Stop()
// 				sendBatch(bc, msgs)
// 				done <- struct{}{}
// 				return
// 			}
// 		}
// 	}()

// 	return sch
// }

// func sendBatch(bc Broadcaster, msgs []*remoting.LinkUpdateMessage) {
// 	if len(msgs) == 0 {
// 		return
// 	}
// 	bm := &remoting.BatchedLinkUpdateMessage{
// 		Messages: msgs,
// 	}
// 	bc.BatchUpdate(context.Background(), bm)
// }
