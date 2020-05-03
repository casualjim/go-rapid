package node

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"

	"github.com/casualjim/go-rapid/remoting"
)

func Endpoint(addr string) (*remoting.Endpoint, error) {
	h, p, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	pp, err := strconv.Atoi(p)
	if err != nil {
		return nil, err
	}

	return &remoting.Endpoint{Hostname: []byte(h), Port: int32(pp)}, nil
}

// MetadataRegistry per-node metadata which is immutable.
// These are simple tags like roles or other configuration parameters.
type MetadataRegistry struct {
	lock  sync.RWMutex
	//table palm.BTree
	table map[*remoting.Endpoint]*remoting.Metadata
}

// NewMetadataRegistry creates a new initialized Metadata object
func NewMetadataRegistry() *MetadataRegistry {
	return &MetadataRegistry{table: make(map[*remoting.Endpoint]*remoting.Metadata)}
}

// All the metadata known
func (m *MetadataRegistry) All() map[string]map[string][]byte {
	m.lock.RLock()
	defer m.lock.RUnlock()

	result := make(map[string]map[string][]byte, len(m.table))
	for k, v := range m.table {
		result[fmt.Sprintf("%s:%d", k.Hostname, k.Port)] = v.Metadata
	}
	return result
}

// All the metadata known
func (m *MetadataRegistry) AllMetadata() (keys []*remoting.Endpoint, values []*remoting.Metadata) {
	m.lock.RLock()
	defer m.lock.RUnlock()

	keys = make([]*remoting.Endpoint, len(m.table))
	values = make([]*remoting.Metadata, len(m.table))
	var i int
	for k, v := range m.table {
		keys[i] = k
		values[i] = v
		i++
	}
	return
}

func (m *MetadataRegistry) MustGet(node *remoting.Endpoint) *remoting.Metadata {
	v, _, err := m.Get(node)
	if err != nil {
		panic(err)
	}
	return v
}

// Get the metadata for the specified node
func (m *MetadataRegistry) Get(node *remoting.Endpoint) (*remoting.Metadata, bool, error) {
	if node == nil {
		return nil, false, errors.New("node metadata get: node host and port values are required")
	}

	m.lock.RLock()
	defer m.lock.RUnlock()

	md, ok := m.getU(node)
	return md, ok, nil
}

func (m *MetadataRegistry) getU(node *remoting.Endpoint) (*remoting.Metadata, bool) {
	res, ok := m.table[node]
	if !ok {
		return nil, ok
	}

	md := make(map[string][]byte, len(res.Metadata))
	for k, v := range res.Metadata {
		md[k] = append([]byte{}, v...)
	}
	return &remoting.Metadata{
		Metadata: md,
	}, ok
}

// Add the metadata for a node
func (m *MetadataRegistry) Add(node *remoting.Endpoint, data *remoting.Metadata) (bool, error) {
	if node == nil {
		return false, errors.New("node metadata set: node host and port values are required")
	}
	if data == nil {
		return false, errors.New("node metadata set: data can't be nil")
	}

	m.lock.RLock()
	if _, ok := m.getU(node); ok {
		m.lock.RUnlock()
		return false, nil
	}
	m.lock.RUnlock()

	m.lock.Lock()
	defer m.lock.Unlock()
	// copy data to avoid accidental modifications
	nd := make(map[string][]byte, len(data.Metadata))
	for k, v := range data.Metadata {
		nd[k] = append([]byte{}, v...)
	}
	m.table[node] = &remoting.Metadata{Metadata: nd}

	return true, nil
}

// Del the metadata for a node
func (m *MetadataRegistry) Del(node *remoting.Endpoint) error {
	if node == nil {
		return errors.New("node metadata del: node host and port values are required")
	}
	m.lock.Lock()
	defer m.lock.Unlock()

	delete(m.table, node)
	return nil
}

func (m *MetadataRegistry) Contains(node *remoting.Endpoint) bool {
	m.lock.RLock()
	defer m.lock.RUnlock()

	_, found := m.table[node]
	return found
}
