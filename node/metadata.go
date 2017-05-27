package node

import "errors"

// Metadata per-node metadata which is immutable.
// These are simple tags like roles or other configuration parameters.
type Metadata struct {
	table map[Addr]map[string]string
}

// NewMetadata creates a new initialized Metadata object
func NewMetadata() *Metadata {
	return &Metadata{
		table: make(map[Addr]map[string]string, 150),
	}
}

// Get the metadata for the specified node
func (m *Metadata) Get(node Addr) (map[string]string, bool, error) {
	if node.Host == "" {
		return nil, false, errors.New("node metadata get: node host and port values are required")
	}

	var res map[string]string
	md, ok := m.table[node]
	if ok && md != nil { // copy to avoid accidental modification
		res = make(map[string]string, len(md))
		for k, v := range md {
			res[k] = v
		}
	}

	return res, ok, nil
}

// Add the metadata for a node
func (m *Metadata) Add(node Addr, data map[string]string) (bool, error) {
	if node.Host == "" {
		return false, errors.New("node metadata set: node host and port values are required")
	}
	if data == nil {
		return false, errors.New("node metadata set: data can't be nil")
	}

	var updated bool
	if _, ok := m.table[node]; !ok {
		// copy data to avoid accidental modifications
		nd := make(map[string]string, len(data))
		for k, v := range data {
			nd[k] = v
		}
		m.table[node] = nd
		updated = true
	}
	return updated, nil
}

// Del the metadata for a node
func (m *Metadata) Del(node Addr) error {
	if node.Host == "" {
		return errors.New("node metadata del: node host and port values are required")
	}
	delete(m.table, node)
	return nil
}
