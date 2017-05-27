package node

import (
	"errors"
	"sync"
)

// NodeMetadata per-node metadata which is immutable. These are simple tags like roles or other configuration parameters.
type NodeMetadata struct {
	lock  *sync.RWMutex
	table map[Addr]map[string]string
}

// NewNodeMetadata creates a new initialized NodeMetadata object
func NewNodeMetadata() *NodeMetadata {
	return &NodeMetadata{
		lock:  new(sync.RWMutex),
		table: make(map[Addr]map[string]string),
	}
}

// Get the metadata for the specified node
func (m *NodeMetadata) Get(node Addr) (map[string]string, bool, error) {
	if node.Host == "" {
		return nil, false, errors.New("node metadata get: node host and port values are required")
	}

	m.lock.RLock()
	var res map[string]string
	md, ok := m.table[node]
	if ok { // copy to avoid accidental modification
		res = make(map[string]string, len(md))
		for k, v := range md {
			res[k] = v
		}
	}

	m.lock.RUnlock()
	return res, ok, nil
}

// Set the meatadata for a node
func (m *NodeMetadata) Set(node Addr, data map[string]string) (bool, error) {
	if node.Host == "" {
		return false, errors.New("node metadata set: node host and port values are required")
	}
	if data == nil {
		return false, errors.New("node metadata set: data can't be nil")
	}
	m.lock.Lock()
	var updated bool
	if _, ok := m.table[node]; !ok {
		m.table[node] = data
		updated = true
	}
	m.lock.Unlock()
	return updated, nil
}

// Del the metadata for a node
func (m *NodeMetadata) Del(node Addr) error {
	if node.Host == "" {
		return errors.New("node metadata del: node host and port values are required")
	}
	m.lock.Lock()
	delete(m.table, node)
	m.lock.Unlock()
	return nil
}
