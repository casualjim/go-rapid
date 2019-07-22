package membership

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Workiva/go-datastructures/common"

	"github.com/Workiva/go-datastructures/slice/skip"

	"github.com/casualjim/go-rapid/internal/epchecksum"

	"github.com/pkg/errors"

	"github.com/OneOfOne/xxhash"
	"github.com/casualjim/go-rapid/remoting"
)

func addressComparator(seed int) func(interface{}, interface{}) int {
	return func(left, right interface{}) int {
		l := left.(*remoting.Endpoint)
		r := right.(*remoting.Endpoint)

		hcl := epchecksum.Checksum(l, seed)
		hcr := epchecksum.Checksum(r, seed)

		if hcl > hcr {
			return 1
		}
		if hcl < hcr {
			return -1
		}

		return 0
	}
}

// NewView creates a new view
func NewView(k int, nodeIDs []*remoting.NodeId, nodeAddrs []*remoting.Endpoint) *View {
	seenIdentifiers := newNodeIDList()
	for _, nodeID := range nodeIDs {
		seenIdentifiers.Add(nodeID)
	}

	rings := make([]*endpointList, k)
	for i := 0; i < k; i++ {
		ts := newEndpointList(i)
		for _, n := range nodeAddrs {
			ts.Add(n)
		}
		rings[i] = ts
	}

	return &View{
		k:                           k,
		rings:                       rings,
		identifiersSeen:             seenIdentifiers,
		configID:                    -1,
		shouldUpdateConfigurationID: 1,
	}
}

// View hosts K permutations of the memberlist that represent the monitoring
// relationship between nodes; every node monitors its successor on each ring.
type View struct {
	k                           int
	rings                       []*endpointList
	lock                        sync.RWMutex
	identifiersSeen             *nodeIDList
	configID                    int64
	shouldUpdateConfigurationID uint32
}

// IsSafeToJoin queries if a host with a logical identifier is safe to add to the network.
//
// addr: the joining node
// id: the logical identifier for the node
//
// returns:
// * HOSTNAME_ALREADY_IN_RING when the hostname is already known
// * UUID_ALREADY_IN_RING when the identifier is already known
// * SAFE_TO_JOIN when the nde can join the network
func (v *View) IsSafeToJoin(addr *remoting.Endpoint, id *remoting.NodeId) remoting.JoinStatusCode {
	v.lock.RLock()
	defer v.lock.RUnlock()

	if v.isHostPresent(addr) {
		return remoting.JoinStatusCode_HOSTNAME_ALREADY_IN_RING
	}
	if v.isIdentifierPresent(id) {
		return remoting.JoinStatusCode_UUID_ALREADY_IN_RING
	}
	return remoting.JoinStatusCode_SAFE_TO_JOIN
}

// RingAdd a node to all K rings and records its unique identifier
func (v *View) RingAdd(addr *remoting.Endpoint, id *remoting.NodeId) error {
	if addr == nil {
		return errors.New("addr should not be nil")
	}
	if id == nil {
		return errors.New("id should not be nil")
	}

	v.lock.Lock()
	defer v.lock.Unlock()

	if v.isIdentifierPresent(id) {
		return &UUIDAlreadySeenError{Node: addr, ID: id}
	}

	if v.isHostPresent(addr) {
		return &NodeAlreadInRingError{Node: addr}
	}

	for k := 0; k < v.k; k++ {
		v.rings[k].Add(addr)
	}

	v.identifiersSeen.Add(id)
	atomic.StoreUint32(&v.shouldUpdateConfigurationID, 1)
	return nil
}

// RingDel a host from all K rings.
func (v *View) RingDel(addr *remoting.Endpoint) error {
	if addr == nil {
		return errors.New("addr should not be nil")
	}

	v.lock.Lock()
	defer v.lock.Unlock()
	if !v.isHostPresent(addr) {
		return &NodeNotInRingError{Node: addr}
	}
	for k := 0; k < v.k; k++ {
		v.rings[k].Remove(addr)
	}

	atomic.StoreUint32(&v.shouldUpdateConfigurationID, 1)
	return nil
}

// Size of the list of nodes currently in the membership
func (v *View) Size() int {
	v.lock.RLock()
	defer v.lock.RUnlock()

	return v.rings[0].Len()
}

// IsIdentifierPresent returns whether an identifier has been used by a node already or not
func (v *View) IsIdentifierPresent(id *remoting.NodeId) bool {
	if id == nil {
		return false
	}

	v.lock.RLock()
	defer v.lock.RUnlock()
	return v.isIdentifierPresent(id)
}

func (v *View) isIdentifierPresent(id *remoting.NodeId) bool {
	return v.identifiersSeen.Contains(id)
}

// IsHostPresent returns whether a host is part of the current membership set or not
func (v *View) IsHostPresent(addr *remoting.Endpoint) bool {
	if addr == nil {
		return false
	}

	v.lock.RLock()
	defer v.lock.RUnlock()

	return v.isHostPresent(addr)
}

func (v *View) isHostPresent(addr *remoting.Endpoint) bool {
	return v.rings[0].Contains(addr)
}

// ObserversForNode returns the set of observers for the specified node
func (v *View) ObserversForNode(addr *remoting.Endpoint) ([]*remoting.Endpoint, error) {
	if addr == nil {
		return nil, errors.New("addr should not be nil")
	}

	v.lock.RLock()
	defer v.lock.RUnlock()

	if !v.isHostPresent(addr) {
		return nil, &NodeNotInRingError{Node: addr}
	}
	return v.observersForNode(addr), nil
}

func (v *View) observersForNode(addr *remoting.Endpoint) []*remoting.Endpoint {
	if v.rings[0].Len() <= 1 {
		return nil
	}

	var observers []*remoting.Endpoint
	for k := 0; k < v.k; k++ {
		lst := v.rings[k]
		observers = append(observers, lst.Higher(addr))
	}
	return observers
}

// ExpectedObserversOf returns the expected monitors of node at addr, even before it is
// added to the ring. Used during the bootstrap protocol to identify
// the nodes responsible for gatekeeping a joining peer.
func (v *View) ExpectedObserversOf(addr *remoting.Endpoint) []*remoting.Endpoint {
	if addr == nil {
		return nil
	}
	v.lock.RLock()
	defer v.lock.RUnlock()

	return v.predecessorsOf(addr)
}

// KnownOrExpectedObserversFor checks if the node is present,
// when present it returns the known monitors of node
// when not present it will return the expected monitors of a node
func (v *View) KnownOrExpectedObserversFor(addr *remoting.Endpoint) []*remoting.Endpoint {
	v.lock.RLock()
	defer v.lock.RUnlock()

	if v.isHostPresent(addr) {
		return v.observersForNode(addr)
	}
	return v.predecessorsOf(addr)
}

// SubjectsOf returns the set of nodes monitored by the specified node
func (v *View) SubjectsOf(addr *remoting.Endpoint) ([]*remoting.Endpoint, error) {
	if addr == nil {
		return nil, errors.New("addr should not be nil")
	}

	v.lock.RLock()
	defer v.lock.RUnlock()

	if !v.isHostPresent(addr) {
		return nil, &NodeNotInRingError{Node: addr}
	}

	if v.rings[0].Len() <= 1 {
		return nil, nil
	}

	return v.predecessorsOf(addr), nil
}

func (v *View) predecessorsOf(addr *remoting.Endpoint) []*remoting.Endpoint {
	var subjects []*remoting.Endpoint
	for k := 0; k < v.k; k++ {
		lst := v.rings[k]
		subjects = append(subjects, lst.Lower(addr))
	}
	return subjects
}

// edgeStatusFor checks if the node is present.
// When present it returns the link status as down
// When not present it return sthe link status as up
func (v *View) edgeStatusFor(addr *remoting.Endpoint) remoting.EdgeStatus {
	v.lock.RLock()
	defer v.lock.RUnlock()

	if v.isHostPresent(addr) {
		return remoting.EdgeStatus_DOWN
	}
	return remoting.EdgeStatus_UP
}

// GetRing gets the list of hosts in the k'th ring.
func (v *View) GetRing(k int) []*remoting.Endpoint {
	v.lock.RLock()
	defer v.lock.RUnlock()
	return v.getRing(k)
}

func (v *View) getRing(k int) []*remoting.Endpoint {
	addrs := make([]*remoting.Endpoint, v.rings[k].Len())
	v.rings[k].Each(func(i int, v *remoting.Endpoint) bool {
		addrs[i] = v
		return true
	})
	return addrs
}

// RingNumbers of a monitor for a given monitoree
// such that monitoree is a successor of monitor on ring[k].
func (v *View) RingNumbers(monitor, monitoree *remoting.Endpoint) []int32 {
	if monitor == nil || monitoree == nil {
		return nil
	}

	v.lock.RLock()
	defer v.lock.RUnlock()
	return v.ringNumbers(monitor, monitoree)
}

func (v *View) ringNumbers(monitor, monitoree *remoting.Endpoint) []int32 {
	monitorees := v.predecessorsOf(monitor)
	if len(monitorees) == 0 {
		return nil
	}

	var result []int32
	var ringNumber int
	for _, node := range monitorees {
		if node == monitoree {
			result = append(result, int32(ringNumber))
		}
		ringNumber++
	}
	return result
}

// ConfigurationID for the current set of identifiers and nodes
func (v *View) ConfigurationID() int64 {
	if atomic.CompareAndSwapUint32(&v.shouldUpdateConfigurationID, 1, 0) {
		v.lock.RLock()
		atomic.StoreInt64(&v.configID, configurationIDFromTreeset(v.identifiersSeen, v.rings[0]))
		v.lock.RUnlock()
	}
	return atomic.LoadInt64(&v.configID)
}

// configuration object that contains the list of nodes in the membership view
// as well as the identifiers seen so far. These two lists suffice to bootstrap an
// identical copy of the MembershipView object.
func (v *View) configuration() *configuration {
	v.lock.RLock()
	c := newConfiguration(v.identifiersSeen, v.rings[0])
	v.lock.RUnlock()
	return c
}

func configurationIDFromTreeset(identifiers *nodeIDList, nodes *endpointList) int64 {
	var hash uint64 = 1

	identifiers.Each(func(i int, id *remoting.NodeId) bool {
		lb := make([]byte, 8)
		hb := make([]byte, 8)
		binary.LittleEndian.PutUint64(hb, uint64(id.GetHigh()))
		binary.LittleEndian.PutUint64(lb, uint64(id.GetLow()))

		hash = hash*37 + xxhash.Checksum64(hb)
		hash = hash*37 + xxhash.Checksum64(lb)
		return true
	})

	nodes.Each(func(i int, addr *remoting.Endpoint) bool {
		prt := make([]byte, 4)
		binary.LittleEndian.PutUint32(prt, uint32(addr.Port))

		hash = hash*37 + xxhash.ChecksumString64(addr.Hostname)
		hash = hash*37 + xxhash.Checksum64(prt)
		return true
	})
	return int64(hash)
}

// newConfiguration initializes a new configuration object from identifiers and nodes
func newConfiguration(identifiers *nodeIDList, nodes *endpointList) *configuration {
	idfs := make([]*remoting.NodeId, identifiers.Len())
	var hash uint64 = 1
	identifiers.Each(func(i int, id *remoting.NodeId) bool {
		idfs[i] = id
		lb := make([]byte, 8)
		hb := make([]byte, 8)
		binary.LittleEndian.PutUint64(hb, uint64(idfs[i].GetHigh()))
		binary.LittleEndian.PutUint64(lb, uint64(idfs[i].GetLow()))
		hash = hash*37 + xxhash.Checksum64(hb)
		hash = hash*37 + xxhash.Checksum64(lb)
		return true
	})
	nds := make([]*remoting.Endpoint, nodes.Len())
	nodes.Each(func(i int, addr *remoting.Endpoint) bool {
		nds[i] = addr
		hash = hash*37 + xxhash.ChecksumString64(nds[i].Hostname)
		prt := make([]byte, 4)
		binary.LittleEndian.PutUint32(prt, uint32(nds[i].Port))
		hash = hash*37 + xxhash.Checksum64(prt)
		return true
	})

	return &configuration{
		Identifiers: idfs,
		Nodes:       nds,
		ConfigID:    int64(hash),
	}
}

// The configuration object contains a list of nodes in the membership view as well as a list of UUIDs.
// An instance of this object created from one MembershipView object contains the necessary information
// to bootstrap an identical membership.View object.
type configuration struct {
	Identifiers []*remoting.NodeId
	Nodes       []*remoting.Endpoint
	ConfigID    int64
}

func (c *configuration) String() string {
	if c == nil {
		return "null"
	}
	b, err := json.Marshal(c)
	if err != nil {
		return fmt.Sprintf(`{"error":%q}`, err.Error())
	}
	return string(b)
}

type UUIDAlreadySeenError struct {
	ID   *remoting.NodeId
	Node *remoting.Endpoint
}

func (u *UUIDAlreadySeenError) Error() string {
	return fmt.Sprintf("id already in seen: {host: %s, identifier: %s}", u.Node, u.ID)
}

// NodeAlreadInRingError contains the node address that is already in the ring
type NodeAlreadInRingError struct {
	Node *remoting.Endpoint
}

func (n *NodeAlreadInRingError) Error() string {
	return fmt.Sprintf("node already in ring: %s", n.Node)
}

// NodeNotInRingError contains the node address that can't be found in the ring
type NodeNotInRingError struct {
	Node *remoting.Endpoint
}

func (n *NodeNotInRingError) Error() string {
	return fmt.Sprintf("node not in ring: %s", n.Node)
}

type nodeIdEntry struct {
	id *remoting.NodeId
}

func (n nodeIdEntry) Compare(cmp common.Comparator) int {
	other := cmp.(nodeIdEntry)

	l := n.id
	r := other.id
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

type endpointEntry struct {
	endpoint *remoting.Endpoint
	checksum uint64
}

func (e endpointEntry) Compare(cmp common.Comparator) int {
	hcl := e.checksum
	hcr := cmp.(endpointEntry).checksum

	if hcl > hcr {
		return 1
	}
	if hcl < hcr {
		return -1
	}

	return 0
}

func newEndpointList(seed int) *endpointList {
	return &endpointList{
		d:    skip.New(uint64(0)),
		seed: seed,
	}
}

type endpointList struct {
	d    *skip.SkipList
	seed int
}

func (e *endpointList) Len() int {
	return int(e.d.Len())
}

func (e *endpointList) entry(node *remoting.Endpoint) endpointEntry {
	return endpointEntry{
		endpoint: node,
		checksum: epchecksum.Checksum(node, e.seed),
	}
}

func (e *endpointList) Add(node *remoting.Endpoint) {
	e.d.Insert(e.entry(node))
}

func (e *endpointList) Remove(node *remoting.Endpoint) {
	e.d.Delete(e.entry(node))
}

func (e *endpointList) Each(iter func(int, *remoting.Endpoint) bool) {
	it := e.d.IterAtPosition(0)
	var i int
	for it.Next() {
		item := it.Value().(endpointEntry)
		iter(i, item.endpoint)
		i++
	}
}

func (e *endpointList) Contains(node *remoting.Endpoint) bool {
	v := e.d.Get(e.entry(node))
	return len(v) > 0 && v[0] != nil
}

func (e *endpointList) Higher(node *remoting.Endpoint) *remoting.Endpoint {
	v, pos := e.d.GetWithPosition(e.entry(node))
	if v == nil {
		return e.d.ByPosition(pos).(endpointEntry).endpoint
	}
	if e.d.Len() == pos+1 {
		return e.d.ByPosition(0).(endpointEntry).endpoint
	}
	return e.d.ByPosition(pos + 1).(endpointEntry).endpoint
}

func (e *endpointList) Lower(node *remoting.Endpoint) *remoting.Endpoint {
	last := e.d.Len() - 1
	_, pos := e.d.GetWithPosition(e.entry(node))
	if pos < 1 {
		return e.d.ByPosition(last).(endpointEntry).endpoint
	}
	return e.d.ByPosition(pos - 1).(endpointEntry).endpoint
}

func newNodeIDList() *nodeIDList {
	return &nodeIDList{
		list: skip.New(uint64(0)),
	}
}

type nodeIDList struct {
	list *skip.SkipList
}

func (n *nodeIDList) Len() int {
	return int(n.list.Len())
}

func (n *nodeIDList) Contains(id *remoting.NodeId) bool {
	v := n.list.Get(nodeIdEntry{id: id})
	return len(v) > 0 && v[0] != nil
}

func (n *nodeIDList) Add(id *remoting.NodeId) {
	n.list.Insert(nodeIdEntry{id: id})
}

func (n *nodeIDList) Each(iter func(int, *remoting.NodeId) bool) {
	it := n.list.IterAtPosition(0)
	var i int
	for it.Next() {
		item := it.Value()
		if !iter(i, item.(nodeIdEntry).id) {
			break
		}
		i++
	}
}
