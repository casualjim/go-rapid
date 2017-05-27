package membership

import (
	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
)

// View interface for getting the monitors for a particular link
type View interface {
	MonitorsForNode(node.Addr) []node.Addr
	LinkStatusForNode(node.Addr) remoting.LinkStatus
}

type view struct {
}

func (v *view) MonitorsForNode(addr node.Addr) []node.Addr {
	if v.isHostPresent(addr) {
		return v.monitorsOfNode(addr)
	}
	return v.expectedMonitorsOfNode(addr)
}

func (v *view) LinkStatusForNode(addr node.Addr) remoting.LinkStatus {
	if v.isHostPresent(addr) {
		return remoting.LinkStatus_DOWN
	}
	return remoting.LinkStatus_UP
}

func (v *view) isHostPresent(addr node.Addr) bool {
	return false
}

func (v *view) monitorsOfNode(addr node.Addr) []node.Addr {
	return nil
}

func (v *view) expectedMonitorsOfNode(addr node.Addr) []node.Addr {
	return nil
}
