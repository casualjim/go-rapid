package membership

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

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

func nodeIDComparator(left, right interface{}) int {
	l := left.(remoting.NodeId)
	r := right.(remoting.NodeId)

	// first comepare high bits
	if l.GetHigh() < r.GetHigh() {
		return -1
	}
	if l.GetHigh() > r.GetHigh() {
		return 1
	}

	// high bits are equal, try the low bits
	if l.GetLow() < r.GetLow() {
		return -1
	}
	if l.GetLow() > r.GetLow() {
		return 1
	}
	// ids are equal
	return 0
}

// NewView creates a new view
func NewView(k int, nodeIDs []remoting.NodeId, nodeAddrs []node.Addr) *View {
	seenIdentifiers := treeset.NewWith(nodeIDComparator)
	for _, nodeID := range nodeIDs {
		seenIdentifiers.Add(nodeID)
	}

	rings := make(map[int]*treeset.Set, k)
	for i := 0; i < k; i++ {
		ts := treeset.NewWith(addressComparator(i))
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
	lock            sync.Mutex
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
	v.lock.Lock()
	defer v.lock.Unlock()

	if v.IsKnownMember(addr) {
		return v.knownMonitorsForNode(addr)
	}
	return v.expectedMonitorsForNode(addr)
}

// LinkStatusForNode checks if the node is present.
// When present it returns the link status as down
// When not present it return sthe link status as up
func (v *View) LinkStatusForNode(addr node.Addr) remoting.LinkStatus {
	v.lock.Lock()
	defer v.lock.Unlock()

	if v.IsKnownMember(addr) {
		return remoting.LinkStatus_DOWN
	}
	return remoting.LinkStatus_UP
}

// IsSafeToJoin queries if a host with a logical identifier is safe to add to the network.
func (v *View) IsSafeToJoin(addr node.Addr, id remoting.NodeId) remoting.JoinStatusCode {
	v.lock.Lock()
	defer v.lock.Unlock()

	if v.IsKnownMember(addr) {
		return remoting.JoinStatusCode_HOSTNAME_ALREADY_IN_RING
	}
	if v.IsKnownIdentifier(id) {
		return remoting.JoinStatusCode_UUID_ALREADY_IN_RING
	}
	return remoting.JoinStatusCode_SAFE_TO_JOIN
}

// RingAdd a node to all K rings and records its unique identifier
func (v *View) RingAdd(addr node.Addr, id remoting.NodeId) error {
	v.lock.Lock()
	defer v.lock.Unlock()

	if v.IsKnownIdentifier(id) {
		return fmt.Errorf("host add attempt with identifier already seen: {host: %s, identifier: %v}", addr, id)
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
func (v *View) RingDel(addr node.Addr) error {
	v.lock.Lock()
	defer v.lock.Unlock()
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
	v.lock.Lock()
	defer v.lock.Unlock()
	return v.rings[0].Size()
}

// IsKnownIdentifier returns whether an identifier has been used by a node already or not
func (v *View) IsKnownIdentifier(id remoting.NodeId) bool {
	return v.identifiersSeen.Contains(id)
}

// IsKnownMember returns whether a host is part of the current membership set or not
func (v *View) IsKnownMember(addr node.Addr) bool {
	return v.rings[0].Contains(addr)
}

// KnownMonitorsForNode returns the set of monitors for the specified node
func (v *View) KnownMonitorsForNode(addr node.Addr) ([]node.Addr, error) {
	v.lock.Lock()
	defer v.lock.Unlock()

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
	for k := 0; k < v.k; k++ {
		lst := v.rings[k]
		val, found := higher(lst, addr)
		if found {
			monitors = append(monitors, val)
		}
	}
	return monitors
}

func higher(lst *treeset.Set, addr node.Addr) (successor node.Addr, found bool) {
	iter := lst.Iterator()
	for iter.Next() {
		if iter.Value() == addr {
			if iter.Next() {
				return iter.Value().(node.Addr), true
			}
			break
		}
	}
	if iter.First() {
		return iter.Value().(node.Addr), true
	}
	return node.Addr{}, false
}

func lower(lst *treeset.Set, addr node.Addr) (predecessor node.Addr, found bool) {
	iter := lst.Iterator()
	for iter.Next() {
		if iter.Value() == addr {
			if iter.Prev() { // peek for next
				return iter.Value().(node.Addr), true
			}
			break
		}
	}
	if iter.Last() {
		return iter.Value().(node.Addr), true
	}
	return node.Addr{}, false
}

// KnownMonitoreesForNode returns the set of nodes monitored by the specified node
func (v *View) KnownMonitoreesForNode(addr node.Addr) ([]node.Addr, error) {
	v.lock.Lock()
	defer v.lock.Unlock()

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
	for k := 0; k < v.k; k++ {
		lst := v.rings[k]

		predecessor, found := lower(lst, addr)
		if found {
			monitorees = append(monitorees, predecessor)
		}
	}
	return monitorees
}

// ExpectedMonitorsForNode returns the expected monitors of node at addr, even before it is
// added to the ring. Used during the bootstrap protocol to identify
// the nodes responsible for gatekeeping a joining peer.
func (v *View) ExpectedMonitorsForNode(addr node.Addr) []node.Addr {
	v.lock.Lock()
	defer v.lock.Unlock()
	return v.expectedMonitorsForNode(addr)
}

func (v *View) expectedMonitorsForNode(addr node.Addr) []node.Addr {
	if v.rings[0].Size() == 0 {
		return nil
	}
	var monitors []node.Addr
	for k := 0; k < v.k; k++ {
		lst := v.rings[k]

		predecessor, found := lower(lst, addr)
		if found {
			monitors = append(monitors, predecessor)
		}
	}
	return monitors
}

// GetRing gets the list of hosts in the k'th ring.
func (v *View) GetRing(k int) []node.Addr {
	v.lock.Lock()
	defer v.lock.Unlock()
	return v.getRing(k)
}

func (v *View) getRing(k int) []node.Addr {
	addrs := make([]node.Addr, v.rings[0].Size())
	v.rings[0].Each(func(i int, v interface{}) {
		addrs[i] = v.(node.Addr)
	})
	return addrs
}

// RingNumbers of a monitor for a given monitoree
// such that monitoree is a successor of monitor on ring[k].
func (v *View) RingNumbers(monitor, monitoree node.Addr) []int {
	v.lock.Lock()
	defer v.lock.Unlock()
	return v.ringNumbers(monitor, monitoree)
}

func (v *View) ringNumbers(monitor, monitoree node.Addr) []int {
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
	var hash uint64 = 1

	identifiers.Each(func(i int, v interface{}) {
		id := v.(remoting.NodeId)
		lb := make([]byte, 8)
		hb := make([]byte, 8)
		binary.BigEndian.PutUint64(hb, uint64(id.GetHigh()))
		binary.BigEndian.PutUint64(lb, uint64(id.GetLow()))

		hash = hash*37 + xxhash.Checksum64(hb)
		hash = hash*37 + xxhash.Checksum64(lb)
	})

	nodes.Each(func(i int, v interface{}) {
		addr := v.(node.Addr)
		prt := make([]byte, 4)
		binary.BigEndian.PutUint32(prt, uint32(addr.Port))

		hash = hash*37 + xxhash.ChecksumString64(addr.Host)
		hash = hash*37 + xxhash.Checksum64(prt)
	})
	return int64(hash)
}

// NewConfiguration initializes a new configuration object from identifiers and nodes
func NewConfiguration(identifiers, nodes *treeset.Set) *Configuration {
	idfs := make([]remoting.NodeId, identifiers.Size())
	var hash uint64 = 1
	identifiers.Each(func(i int, v interface{}) {
		idfs[i] = v.(remoting.NodeId)
		lb := make([]byte, 8)
		hb := make([]byte, 8)
		binary.BigEndian.PutUint64(hb, uint64(idfs[i].GetHigh()))
		binary.BigEndian.PutUint64(lb, uint64(idfs[i].GetLow()))
		hash = hash*37 + xxhash.Checksum64(hb)
		hash = hash*37 + xxhash.Checksum64(lb)
	})
	nds := make([]node.Addr, nodes.Size())
	nodes.Each(func(i int, v interface{}) {
		nds[i] = v.(node.Addr)
		hash = hash*37 + xxhash.ChecksumString64(nds[i].Host)
		prt := make([]byte, 4)
		binary.BigEndian.PutUint32(prt, uint32(nds[i].Port))
		hash = hash*37 + xxhash.Checksum64(prt)
	})

	return &Configuration{
		Identifiers: idfs,
		Nodes:       nds,
		ConfigID:    int64(hash),
	}
}

// The Configuration object contains a list of nodes in the membership view as well as a list of UUIDs.
// An instance of this object created from one MembershipView object contains the necessary information
// to bootstrap an identical membership.View object.
type Configuration struct {
	Identifiers []remoting.NodeId
	Nodes       []node.Addr
	ConfigID    int64
}

func (c *Configuration) String() string {
	if c == nil {
		return "null"
	}
	b, err := json.Marshal(c)
	if err != nil {
		return fmt.Sprintf(`{"error":%q}`, err.Error())
	}
	return string(b)
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

func nodeIDFromUUID(uid uuid.UUID) remoting.NodeId {
	var lsb, msb uint64
	for i := 0; i < 8; i++ {
		msb = (msb << 8) | (uint64(uid[i]) & 0xff)
	}
	for i := 8; i < 16; i++ {
		lsb = (lsb << 8) | (uint64(uid[i]) & 0xff)
	}
	return remoting.NodeId{
		High: int64(msb),
		Low:  int64(lsb),
	}
}
