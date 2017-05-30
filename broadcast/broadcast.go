package broadcast

import (
	"context"
	"math/rand"
	"sync"

	rapid "github.com/casualjim/go-rapid"
	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
)

// Broadcaster interface different broadcasting mechanisms can implement
type Broadcaster interface {
	BatchUpdate(context.Context, *remoting.BatchedLinkUpdateMessage)
	ConsensusProposal(context.Context, *remoting.ConsensusProposal)
	SetMembership([]node.Addr)
}

// Filter for deciding who to broadcast to
type Filter func(node.Addr) bool

// MatchAll filter for a broadcaster
func MatchAll(_ node.Addr) bool { return true }

// UnicastToAll broadcaster
func UnicastToAll(client rapid.Client, log rapid.Logger) Broadcaster {
	if log == nil {
		log = rapid.NOOPLogger
	}
	return &unicastFiltered{
		Filter: MatchAll,
		lock:   &sync.RWMutex{},
		client: client,
		log:    log,
	}
}

type unicastFiltered struct {
	Filter  Filter
	members []node.Addr
	lock    *sync.RWMutex
	client  rapid.Client
	log     rapid.Logger
}

func (u *unicastFiltered) BatchUpdate(ctx context.Context, msg *remoting.BatchedLinkUpdateMessage) {
	u.lock.RLock()
	for _, recipient := range u.members {
		if u.Filter(recipient) {
			if _, err := u.client.SendLinkUpdateMessage(ctx, recipient, msg); err != nil {
				u.log.Printf("failed to broadcast update to %q: %v", recipient, err)
			}
		}
	}
	u.lock.RUnlock()
}

func (u *unicastFiltered) ConsensusProposal(ctx context.Context, msg *remoting.ConsensusProposal) {
	u.lock.RLock()
	for _, recipient := range u.members {
		if u.Filter(recipient) {
			if _, err := u.client.SendConsensusProposal(ctx, recipient, msg); err != nil {
				u.log.Printf("failed to send consensus proposal to %q: %v", recipient, err)
			}
		}
	}
	u.lock.RUnlock()
}

func (u *unicastFiltered) SetMembership(recipients []node.Addr) {
	u.lock.Lock()
	for i := len(recipients) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		recipients[i], recipients[j] = recipients[j], recipients[i]
	}
	u.members = recipients
	u.lock.Unlock()
}
