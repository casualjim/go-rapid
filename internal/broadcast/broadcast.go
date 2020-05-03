package broadcast

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/casualjim/go-rapid/api"
	"github.com/casualjim/go-rapid/remoting"
	"github.com/rs/zerolog"
)

// Broadcaster interface different broadcasting mechanisms can implement
type Broadcaster interface {
	Start()
	Stop()
	Broadcast(context.Context, *remoting.RapidRequest)
	SetMembership([]*remoting.Endpoint)
}

// Filter for deciding who to broadcast to
type Filter func(*remoting.Endpoint) bool

// MatchAll filter for a broadcaster
func MatchAll(_ *remoting.Endpoint) bool { return true }

// UnicastToAll broadcaster
func UnicastToAll(log zerolog.Logger, client api.Client) Broadcaster {
	return Unicast(log, client, MatchAll)
}

// Unicast broadcaster
func Unicast(log zerolog.Logger, client api.Client, filter Filter) Broadcaster {
	//rx := make(chan bcMessage, 100)
	if filter == nil {
		filter = MatchAll
	}

	return &unicastFiltered{
		Filter: filter,
		client: client,
		log:    log,
		rand:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

type unicastFiltered struct {
	Filter Filter
	sync.RWMutex
	members []*remoting.Endpoint
	client  api.Client
	log     zerolog.Logger
	rand    *rand.Rand
}

// Result contains either an error or a success result
type Result struct {
	Err      error
	Response *remoting.RapidResponse
}

// Results is a collection of broadcast results
type Results []Result

// IsSuccess returns true if none of the results in the collection failed
func (b Results) IsSuccess() bool {
	return !b.HasError()
}

// HasError returns true if any of the results in the collection failed
func (b Results) HasError() bool {
	for _, r := range b {
		if r.Err != nil {
			return true
		}
	}
	return false
}

type ctxKey uint8

const (
	_ ctxKey = iota
	ctxCollector
)

func SetCollectorCtx(ctx context.Context, collector chan Results) context.Context {
	return context.WithValue(ctx, ctxCollector, collector)
}

func (u *unicastFiltered) Broadcast(ctx context.Context, req *remoting.RapidRequest) {
	cval := ctx.Value(ctxCollector)
	var wg sync.WaitGroup
	var sink chan Result

	if cval != nil {
		collector := cval.(chan Results)
		sink = make(chan Result, len(u.members))

		go func() {
			var results Results
			for result := range sink {
				results = append(results, result)
			}
			collector <- results
			close(collector)
		}()

	}

	sendMsg := func(ctx context.Context, recipient *remoting.Endpoint, req *remoting.RapidRequest) {
		resp, err := u.client.DoBestEffort(ctx, recipient, req)
		if err != nil {
			u.log.Warn().Err(err).Str("recipient", recipient.String()).Msg("failed to broadcast")
			if cval != nil {
				sink <- Result{Err: err}
			}
		}
		if cval != nil {
			if err == nil {
				sink <- Result{Response: resp}
			}
			wg.Done()
		}
	}

	u.log.Debug().Int("member_count", len(u.members)).Str("message", protojson.Format(req)).Msg("broadcasting")
	u.RLock()
	for _, rec := range u.members {
		recipient := rec
		if u.Filter(recipient) {
			if cval != nil {
				wg.Add(1)
			}
			// we don't actually care about the result
			// and this ensures we never block broadcasts
			go sendMsg(ctx, recipient, req)
		}
	}
	u.RUnlock()
	if cval != nil {
		go func() {
			wg.Wait()
			close(sink)
		}()
	}

}

func (u *unicastFiltered) SetMembership(recipients []*remoting.Endpoint) {
	u.Lock()
	if u.rand == nil {
		u.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	u.rand.Shuffle(len(recipients), func(i, j int) {
		recipients[i], recipients[j] = recipients[j], recipients[i]
	})
	u.members = recipients
	u.Unlock()
}

func (u *unicastFiltered) Start() {}
func (u *unicastFiltered) Stop()  {}
