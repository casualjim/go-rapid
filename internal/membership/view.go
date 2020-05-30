package membership

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"unsafe"

	"github.com/rs/zerolog"

	"google.golang.org/protobuf/proto"

	"github.com/Workiva/go-datastructures/common"

	"github.com/casualjim/go-rapid/internal/epchecksum"

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
		k:                         k,
		rings:                     rings,
		identifiersSeen:           seenIdentifiers,
		allNodes:                  make(map[uint64]bool, 150),
		cachedObservers:           make(map[uint64][]*remoting.Endpoint, 150),
		configurationUpdateReason: "initialize",
	}
}

// View hosts K permutations of the memberlist that represent the monitoring
// relationship between nodes; every node monitors its successor on each ring.
type View struct {
	k                         int
	rings                     []*endpointList
	lock                      sync.RWMutex
	identifiersSeen           nodeIDList
	cachedObservers           map[uint64][]*remoting.Endpoint
	allNodes                  map[uint64]bool
	config                    *Configuration
	configurationUpdateReason string
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
func (v *View) IsSafeToJoin(ctx context.Context, addr *remoting.Endpoint, id *remoting.NodeId) remoting.JoinStatusCode {
	lg := zerolog.Ctx(ctx).With().Str("addr", epStr(addr)).Str("nodeId", id.String()).Logger()
	lg.Debug().Msg("entering is safe to join")
	defer func() { lg.Debug().Msg("leaving is safe to join") }()

	v.lock.RLock()
	defer v.lock.RUnlock()

	if v.isHostPresent(lg, addr) {
		return remoting.JoinStatusCode_HOSTNAME_ALREADY_IN_RING
	}
	if v.isIdentifierPresent(id) {
		return remoting.JoinStatusCode_UUID_ALREADY_IN_RING
	}
	return remoting.JoinStatusCode_SAFE_TO_JOIN
}

// RingAdd a node to all K rings and records its unique identifier
func (v *View) RingAdd(ctx context.Context, addr *remoting.Endpoint, id *remoting.NodeId) error {
	if addr == nil {
		return errors.New("addr should not be nil")
	}
	if id == nil {
		return errors.New("id should not be nil")
	}
	lg := zerolog.Ctx(ctx).With().Str("addr", epStr(addr)).Str("nodeId", id.String()).Logger()
	lg.Debug().Msg("entering add to ring")
	defer func() { lg.Debug().Msg("leaving add to ring") }()

	v.lock.Lock()
	defer v.lock.Unlock()

	if v.isIdentifierPresent(id) {
		lg.Debug().Msg("identifier is present")
		return &UUIDAlreadySeenError{Node: addr, ID: id}
	}

	if v.isHostPresent(lg, addr) {
		lg.Debug().Msg("address is present")
		return &NodeAlreadInRingError{Node: addr}
	}

	cs := epchecksum.Checksum(addr, 0)
	affectedSubjects := make(map[uint64]bool)
	for k := 0; k < v.k; k++ {
		v.rings[k].Add(addr)
		subj := v.rings[k].Lower(addr)
		if subj != nil {
			affectedSubjects[epchecksum.Checksum(subj, 0)] = true
		}
	}

	lg.Debug().Str("cs", strconv.FormatUint(cs, 10)).Msg("adding checksum to all nodes")
	v.allNodes[cs] = true
	for k := range affectedSubjects {
		delete(v.cachedObservers, k)
	}
	v.identifiersSeen.Add(id)
	//atomic.StoreUint32(&v.configurationUpdateReason, 1)
	v.configurationUpdateReason = "added new element"
	return nil
}

// RingDel a host from all K rings.
func (v *View) RingDel(ctx context.Context, addr *remoting.Endpoint) error {
	if addr == nil {
		return errors.New("addr should not be nil")
	}
	//st := make([]byte, 8192)
	//runtime.Stack(st, false)
	//v.log.Debug().Str("stacktrace", string(bytes.Trim(st, "\x00"))).Msg("Deleting item from ring")
	lg := zerolog.Ctx(ctx).With().Str("addr", epStr(addr)).Logger()
	lg.Debug().Msg("entering delete from ring")
	defer func() { lg.Debug().Msg("leaving delete from ring") }()

	v.lock.Lock()
	defer v.lock.Unlock()

	if !v.isHostPresent(lg, addr) {
		return &NodeNotInRingError{Node: addr}
	}

	cs := epchecksum.Checksum(addr, 0)
	affectedSubjects := make(map[uint64]bool)
	for k := 0; k < v.k; k++ {
		endpoints := v.rings[k]

		oldSubject := endpoints.Lower(addr)
		if oldSubject != nil {
			affectedSubjects[epchecksum.Checksum(oldSubject, 0)] = true
		}
		endpoints.Remove(addr)

		delete(v.cachedObservers, cs)
	}

	delete(v.allNodes, epchecksum.Checksum(addr, 0))
	for k := range affectedSubjects {
		delete(v.cachedObservers, k)
	}

	v.configurationUpdateReason = "deleted element"
	//atomic.StoreUint32(&v.configurationUpdateReason, 1)
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
func (v *View) IsHostPresent(ctx context.Context, addr *remoting.Endpoint) bool {
	if addr == nil {
		return false
	}
	lg := zerolog.Ctx(ctx).With().Str("addr", epStr(addr)).Logger()
	lg.Debug().Msg("check if host is present")

	var result bool
	defer func() { lg.Debug().Bool("result", result).Msg("leaving check if host is present") }()
	v.lock.RLock()
	defer v.lock.RUnlock()

	result = v.isHostPresent(lg, addr)
	return result
}

func (v *View) isHostPresent(lg zerolog.Logger, addr *remoting.Endpoint) bool {
	cs := epchecksum.Checksum(addr, 0)
	lg.Debug().Str("cs", strconv.FormatUint(cs, 10)).Msg("checking in all nodes")
	return v.allNodes[cs]
}

// ObserversForNode returns the set of observers for the specified node
func (v *View) ObserversForNode(addr *remoting.Endpoint) ([]*remoting.Endpoint, error) {
	if addr == nil {
		return nil, errors.New("addr should not be nil")
	}

	v.lock.Lock()
	defer v.lock.Unlock()

	cs := epchecksum.Checksum(addr, 0)
	if !v.allNodes[cs] {
		return nil, &NodeNotInRingError{Node: addr}
	}

	if _, ok := v.cachedObservers[cs]; !ok {
		v.cachedObservers[cs] = v.observersForNode(addr)
	}
	return v.cachedObservers[cs], nil
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
func (v *View) ExpectedObserversOf(ctx context.Context, addr *remoting.Endpoint) []*remoting.Endpoint {
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
func (v *View) KnownOrExpectedObserversFor(ctx context.Context, addr *remoting.Endpoint) []*remoting.Endpoint {
	v.lock.RLock()
	defer v.lock.RUnlock()

	lg := zerolog.Ctx(ctx).With().Str("addr", epStr(addr)).Logger()
	if v.isHostPresent(lg, addr) {
		return v.observersForNode(addr)
	}
	return v.predecessorsOf(addr)
}

// SubjectsOf returns the set of nodes monitored by the specified node
func (v *View) SubjectsOf(ctx context.Context, addr *remoting.Endpoint) ([]*remoting.Endpoint, error) {
	if addr == nil {
		return nil, errors.New("addr should not be nil")
	}

	v.lock.RLock()
	defer v.lock.RUnlock()

	lg := zerolog.Ctx(ctx).With().Str("addr", epStr(addr)).Logger()
	if !v.isHostPresent(lg, addr) {
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
func (v *View) edgeStatusFor(ctx context.Context, addr *remoting.Endpoint) remoting.EdgeStatus {
	lg := zerolog.Ctx(ctx).With().Str("addr", epStr(addr)).Logger()
	lg.Debug().Msg("entering edgeStatusFor")
	defer func() { lg.Debug().Msg("leaving edgeStatusFor") }()

	v.lock.RLock()
	defer v.lock.RUnlock()

	if v.isHostPresent(lg, addr) {
		return remoting.EdgeStatus_DOWN
	}
	return remoting.EdgeStatus_UP
}

// GetRing gets the list of hosts in the k'th ring.
func (v *View) GetRing(ctx context.Context, k int) []*remoting.Endpoint {
	v.lock.RLock()
	defer v.lock.RUnlock()
	return v.getRing(k)
}

func (v *View) getRing(k int) []*remoting.Endpoint {
	addrs := make([]*remoting.Endpoint, v.rings[k].Len())
	v.rings[k].Each(func(i int, v *remoting.Endpoint) bool {
		addrs[i] = proto.Clone(v).(*remoting.Endpoint)
		return true
	})
	return addrs
}

// RingNumbers of a monitor for a given monitoree
// such that monitoree is a successor of monitor on ring[k].
func (v *View) RingNumbers(ctx context.Context, monitor, monitoree *remoting.Endpoint) []int32 {
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
func (v *View) ConfigurationID(ctx context.Context) int64 {
	return v.Configuration(ctx).ConfigID
}

// Configuration object that contains the list of nodes in the membership view
// as well as the identifiers seen so far. These two lists suffice to bootstrap an
// identical copy of the MembershipView object.
func (v *View) Configuration(ctx context.Context) *Configuration {
	v.lock.Lock()
	defer v.lock.Unlock()

	if v.configurationUpdateReason != "" {
		var prev int64
		if v.config != nil {
			prev = v.config.ConfigID
		}
		newConfig := newConfiguration(v.identifiersSeen, v.rings[0])
		v.config = newConfig
		zerolog.Ctx(ctx).Debug().
			Int64("previous_config", prev).
			Int64("new_config", newConfig.ConfigID).
			Str("reason", v.configurationUpdateReason).
			Msg("config update")
		v.configurationUpdateReason = ""
	}
	return v.config
}

// newConfiguration initializes a new Configuration object from identifiers and nodes
func newConfiguration(identifiers nodeIDList, nodes *endpointList) *Configuration {
	idfs := make([]*remoting.NodeId, identifiers.Len())
	var hash uint64 = 1
	identifiers.Each(func(i int, id *remoting.NodeId) bool {
		idfs[i] = id
		hid := id.GetHigh()
		lid := id.GetLow()
		hr := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&hid)), Len: 8, Cap: 8}
		lr := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&lid)), Len: 8, Cap: 8}
		hash = hash*37 + xxhash.Checksum64(*(*[]byte)(unsafe.Pointer(&hr)))
		hash = hash*37 + xxhash.Checksum64(*(*[]byte)(unsafe.Pointer(&lr)))
		return true
	})
	nds := make([]*remoting.Endpoint, nodes.Len())
	nodes.Each(func(i int, addr *remoting.Endpoint) bool {
		nds[i] = addr
		hash = hash*37 + xxhash.Checksum64(nds[i].Hostname)
		hdr := reflect.SliceHeader{Data: uintptr(unsafe.Pointer(&nds[i].Port)), Len: 4, Cap: 4}
		hash = hash*37 + xxhash.Checksum64(*(*[]byte)(unsafe.Pointer(&hdr)))
		return true
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
	Identifiers []*remoting.NodeId
	Nodes       []*remoting.Endpoint
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

func compareNodeId(l, r *remoting.NodeId) int {
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
	return compareEndpoints(hcl, hcr)
}

func compareEndpoints(l, r uint64) int {
	if l > r {
		return 1
	}
	if l < r {
		return -1
	}
	return 0
}

func newEndpointList(seed int) *endpointList {
	return &endpointList{
		seed: seed,
	}
}

type endpointList struct {
	eps  []endpointEntry
	seed int
}

func (e *endpointList) Len() int {
	return len(e.eps)
}

func (e *endpointList) entry(node *remoting.Endpoint) endpointEntry {
	return endpointEntry{
		endpoint: node,
		checksum: epchecksum.Checksum(node, e.seed),
	}
}

func (e *endpointList) Add(node *remoting.Endpoint) {
	idx := sort.Search(len(e.eps), func(i int) bool {
		return compareEndpoints(e.eps[i].checksum, epchecksum.Checksum(node, e.seed)) >= 0
	})
	if idx == len(e.eps) {
		e.eps = append(e.eps, e.entry(proto.Clone(node).(*remoting.Endpoint)))
	} else {
		e.eps = append(
			e.eps[:idx],
			append(
				[]endpointEntry{e.entry(proto.Clone(node).(*remoting.Endpoint))},
				e.eps[idx:]...)...)

	}
}

func (e *endpointList) Remove(node *remoting.Endpoint) {
	idx := sort.Search(len(e.eps), func(i int) bool {
		return compareEndpoints(e.eps[i].checksum, epchecksum.Checksum(node, e.seed)) >= 0
	})
	if idx < len(e.eps) && e.eps[idx].checksum == epchecksum.Checksum(node, e.seed) {
		copy(e.eps[idx:], e.eps[idx+1:])
		e.eps[len(e.eps)-1].endpoint = nil
		e.eps = e.eps[:len(e.eps)-1]
	}
}

func (e *endpointList) Each(iter func(int, *remoting.Endpoint) bool) {
	for i, v := range e.eps {
		if !iter(i, v.endpoint) {
			break
		}
	}
}

func (e *endpointList) Contains(node *remoting.Endpoint) bool {
	cs := epchecksum.Checksum(node, e.seed)
	idx := sort.Search(len(e.eps), func(i int) bool {
		return compareEndpoints(e.eps[i].checksum, cs) >= 0
	})
	return idx < len(e.eps) && e.eps[idx].checksum == cs
}

func (e *endpointList) Higher(node *remoting.Endpoint) *remoting.Endpoint {
	if len(e.eps) == 0 {
		return nil
	}

	cs := epchecksum.Checksum(node, e.seed)
	idx := sort.Search(len(e.eps), func(i int) bool {
		return compareEndpoints(e.eps[i].checksum, cs) > 0
	})

	if idx < len(e.eps) {
		return e.eps[idx].endpoint
	}
	return e.eps[0].endpoint
}

func (e *endpointList) Lower(node *remoting.Endpoint) *remoting.Endpoint {
	if len(e.eps) == 0 {
		return nil
	}

	cs := epchecksum.Checksum(node, e.seed)
	idx := sort.Search(len(e.eps), func(i int) bool {
		return compareEndpoints(e.eps[i].checksum, cs) >= 0
	})

	if idx < len(e.eps) {
		equals := e.eps[idx].checksum == cs
		if idx > 0 && equals {
			return e.eps[idx-1].endpoint
		} else if !equals {
			return e.eps[idx].endpoint
		}
	}
	return e.eps[len(e.eps)-1].endpoint
}

func newNodeIDList() nodeIDList {
	return nodeIDList{}
}

type nodeIDList []*remoting.NodeId

func (n nodeIDList) Less(i, j int) bool {
	return compareNodeId(n[i], n[j]) < 0
}

func (n nodeIDList) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}

func (n nodeIDList) Len() int {
	return len(n)
}

func (n nodeIDList) Contains(id *remoting.NodeId) bool {
	idx := sort.Search(len(n), func(i int) bool {
		other := n[i]
		cmp := compareNodeId(other, id)
		return cmp >= 0
	})
	lnok := idx < len(n)
	return lnok && proto.Equal(n[idx], id)
}

func (n *nodeIDList) Add(id *remoting.NodeId) {
	nn := *n
	idx := sort.Search(len(nn), func(i int) bool { return compareNodeId(nn[i], id) >= 0 })
	if idx == len(nn) {
		*n = append(nn, proto.Clone(id).(*remoting.NodeId))
	} else {
		*n = append(nn[:idx], append([]*remoting.NodeId{proto.Clone(id).(*remoting.NodeId)}, nn[idx:]...)...)
	}
}

func (n nodeIDList) Each(iter func(int, *remoting.NodeId) bool) {
	for i := range n {
		if !iter(i, n[i]) {
			break
		}
	}
}
