package membership

import (
	"bytes"
	"fmt"

	"github.com/OneOfOne/xxhash"
	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
	"github.com/emirpasic/gods/sets/treeset"
	"github.com/pborman/uuid"
)

func addressComparator(seed int) func(interface{}, interface{}) int {
	return func(left, right interface{}) int {
		l := left.(node.Addr)
		r := right.(node.Addr)

		hcl := l.Checksum(seed)
		hcr := r.Checksum(seed)

		if hcl > hcr {
			return 1
		}
		if hcl < hcr {
			return -1
		}

		return 0
	}
}

func uuidComparator(left, right interface{}) int {
	l := left.(uuid.UUID)
	r := right.(uuid.UUID)
	return bytes.Compare(l, r)
}

// NewView creates a new view
func NewView(k int, uuids []uuid.UUID, nodeAddrs []node.Addr) *View {
	seenIdentifiers := treeset.NewWith(uuidComparator)
	for _, uuid := range uuids {
		seenIdentifiers.Add(uuid)
	}

	rings := make(map[int]*treeset.Set, k)
	for i := 0; i < k; i++ {
		ts := treeset.NewWith(addressComparator(k))
		for _, n := range nodeAddrs {
			ts.Add(n)
		}
		rings[i] = ts
	}

	return &View{
		k:               k,
		rings:           rings,
		identifiersSeen: seenIdentifiers,
		configID:        -1,
		dirty:           true,
	}
}

// View hosts K permutations of the memberlist that represent the monitoring
// relationship between nodes; every node monitors its successor on each ring.
type View struct {
	k               int
	rings           map[int]*treeset.Set
	identifiersSeen *treeset.Set
	configID        int64
	dirty           bool
}

// MonitorsForNode checks if the node is present,
// when present it returns the known monitors of node
// when not present it will return the expected monitors of a node
func (v *View) MonitorsForNode(addr node.Addr) []node.Addr {
	if v.IsKnownMember(addr) {
		return v.knownMonitorsForNode(addr)
	}
	return v.ExpectedMonitorsForNode(addr)
}

// LinkStatusForNode checks if the node is present.
// When present it returns the link status as down
// When not present it return sthe link status as up
func (v *View) LinkStatusForNode(addr node.Addr) remoting.LinkStatus {
	if v.IsKnownMember(addr) {
		return remoting.LinkStatus_DOWN
	}
	return remoting.LinkStatus_UP
}

// IsSafeToJoin queries if a host with a logical identifier is safe to add to the network.
func (v *View) IsSafeToJoin(addr node.Addr, id uuid.UUID) remoting.JoinStatusCode {
	if v.IsKnownMember(addr) {
		return remoting.JoinStatusCode_HOSTNAME_ALREADY_IN_RING
	}
	if v.IsKnownIdentifier(id) {
		return remoting.JoinStatusCode_UUID_ALREADY_IN_RING
	}
	return remoting.JoinStatusCode_SAFE_TO_JOIN
}

// RingAdd a node to all K rings and records its unique identifier
func (v *View) RingAdd(addr node.Addr, id uuid.UUID) error {
	if v.IsKnownIdentifier(id) {
		return fmt.Errorf("host add attempt with identifier already seen: {host: %s, identifier: %s}", addr, id)
	}
	if v.IsKnownMember(addr) {
		return &NodeAlreadInRingError{Node: addr}
	}
	for k := 0; k < v.k; k++ {
		v.rings[k].Add(addr)
	}
	v.identifiersSeen.Add(id)
	v.dirty = true
	return nil
}

// RingDel a host from all K rings.
func (v *View) RingDel(addr node.Addr, id uuid.UUID) error {
	if !v.IsKnownMember(addr) {
		return &NodeNotInRingError{Node: addr}
	}
	for k := 0; k < v.k; k++ {
		v.rings[k].Remove(addr)
	}
	v.dirty = true
	return nil
}

// Size of the list of nodes currently in the membership
func (v *View) Size() int {
	return v.rings[0].Size()
}

// IsKnownIdentifier returns whether an identifier has been used by a node already or not
func (v *View) IsKnownIdentifier(id uuid.UUID) bool {
	return v.identifiersSeen.Contains(id)
}

// IsKnownMember returns whether a host is part of the current membership set or not
func (v *View) IsKnownMember(addr node.Addr) bool {
	return v.rings[0].Contains(addr)
}

// KnownMonitorsForNode returns the set of monitors for the specified node
func (v *View) KnownMonitorsForNode(addr node.Addr) ([]node.Addr, error) {
	if !v.IsKnownMember(addr) {
		return nil, &NodeNotInRingError{Node: addr}
	}
	return v.knownMonitorsForNode(addr), nil
}

func (v *View) knownMonitorsForNode(addr node.Addr) []node.Addr {
	if v.rings[0].Size() <= 1 {
		return nil
	}
	var monitors []node.Addr
RINGS:
	for k := 0; k < v.k; k++ {
		lst := v.rings[k]

		iter := lst.Iterator()
		for iter.Next() {
			if iter.Value() == addr {
				if iter.Next() {
					monitors = append(monitors, iter.Value().(node.Addr))
					continue RINGS // goto next ring, we got what we need from this one
				}
				break // we found our node but had no next value, bail and move to first
			}
		}
		if iter.First() {
			monitors = append(monitors, iter.Value().(node.Addr))
		}
	}
	return monitors
}

// KnownMonitoreesForNode returns the set of nodes monitored by the specified node
func (v *View) KnownMonitoreesForNode(addr node.Addr) ([]node.Addr, error) {
	if !v.IsKnownMember(addr) {
		return nil, &NodeNotInRingError{Node: addr}
	}
	return v.knownMonitoreesForNode(addr), nil
}

func (v *View) knownMonitoreesForNode(addr node.Addr) []node.Addr {
	if v.rings[0].Size() <= 1 {
		return nil
	}
	var monitorees []node.Addr
RINGS:
	for k := 0; k < v.k; k++ {
		lst := v.rings[k]

		iter := lst.Iterator()
		for iter.Next() {
			if iter.Value() == addr {
				if iter.Prev() {
					monitorees = append(monitorees, iter.Value().(node.Addr))
					continue RINGS // goto next ring, we got what we need from this one
				}
				break // we found our node but had no previous value, bail and move to last
			}
		}
		if iter.Last() {
			monitorees = append(monitorees, iter.Value().(node.Addr))
		}
	}
	return monitorees
}

// ExpectedMonitorsForNode returns the expected monitors of {@code node}, even before it is
// added to the ring. Used during the bootstrap protocol to identify
// the nodes responsible for gatekeeping a joining peer.
func (v *View) ExpectedMonitorsForNode(addr node.Addr) []node.Addr {
	if v.rings[0].Size() == 0 {
		return nil
	}
	var monitorees []node.Addr
RINGS:
	for k := 0; k < v.k; k++ {
		lst := v.rings[k]

		iter := lst.Iterator()
		for iter.Next() {
			if iter.Value() == addr {
				if iter.Prev() {
					monitorees = append(monitorees, iter.Value().(node.Addr))
					continue RINGS // goto next ring, we got what we need from this one
				}
				break // we found our node but had no previous value, bail and move to last
			}
		}
		if iter.Last() {
			monitorees = append(monitorees, iter.Value().(node.Addr))
		}
	}
	return monitorees
}

// Ring get the list of hosts in the k'th ring.
func (v *View) Ring(k int) []node.Addr {
	addrs := make([]node.Addr, v.rings[0].Size())
	v.rings[0].Each(func(i int, v interface{}) {
		addrs = append(addrs, v.(node.Addr))
	})
	return addrs
}

// RingNumbers of a monitor for a given monitoree
// such that monitoree is a successor of monitor on ring[k].
func (v *View) RingNumbers(monitor, monitoree node.Addr) []int {
	monitorees := v.knownMonitoreesForNode(monitor)
	if len(monitorees) == 0 {
		return nil
	}

	var result []int
	var ringNumber int
	for _, node := range monitorees {
		if node == monitoree {
			result = append(result, ringNumber)
		}
		ringNumber++
	}
	return result
}

// ConfigurationID for the current set of identifiers and nodes
func (v *View) ConfigurationID() int64 {
	if v.dirty {
		v.configID = configurationIDFromTreeset(v.identifiersSeen, v.rings[0])
		v.dirty = false
	}
	return v.configID
}

// Configuration object that contains the list of nodes in the membership view
// as well as the identifiers seen so far. These two lists suffice to bootstrap an
// identical copy of the MembershipView object.
func (v *View) Configuration() *Configuration {
	return NewConfiguration(v.identifiersSeen, v.rings[0])
}

func configurationIDFromTreeset(identifiers, nodes *treeset.Set) int64 {
	hash := int64(1)
	iiter := identifiers.Iterator()
	for iiter.Next() {
		hash = hash*37 + int64(xxhash.Checksum32(iiter.Value().(uuid.UUID)))
	}

	niter := nodes.Iterator()
	for niter.Next() {
		hash = hash*37 + niter.Value().(node.Addr).Hashcode()
	}
	return hash
}

// NewConfiguration initializes a new configuration object from identifiers and nodes
func NewConfiguration(identifiers, nodes *treeset.Set) *Configuration {
	idfs := make([]uuid.UUID, identifiers.Size())
	hash := int64(1)
	identifiers.Each(func(i int, v interface{}) {
		idfs[i] = v.(uuid.UUID)
		hash = hash*37 + int64(xxhash.Checksum32(idfs[i]))
	})
	nds := make([]node.Addr, nodes.Size())
	nodes.Each(func(i int, v interface{}) {
		nds[i] = v.(node.Addr)
		hash = hash*37 + nds[i].Hashcode()
	})

	return &Configuration{
		Identifiers: idfs,
		Nodes:       nds,
		ConfigID:    hash,
	}
}

// The Configuration object contains a list of nodes in the membership view as well as a list of UUIDs.
// An instance of this object created from one MembershipView object contains the necessary information
// to bootstrap an identical membership.View object.
type Configuration struct {
	Identifiers []uuid.UUID
	Nodes       []node.Addr
	ConfigID    int64
}

// NodeAlreadInRingError contains the node address that is already in the ring
type NodeAlreadInRingError struct {
	Node node.Addr
}

func (n *NodeAlreadInRingError) Error() string {
	return fmt.Sprintf("node already in ring: %s", n.Node)
}

// NodeNotInRingError contains the node address that can't be found in the ring
type NodeNotInRingError struct {
	Node node.Addr
}

func (n *NodeNotInRingError) Error() string {
	return fmt.Sprintf("node not in ring: %s", n.Node)
}
