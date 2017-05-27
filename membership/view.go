package membership

import (
	"github.com/casualjim/go-rapid/node"
	"github.com/casualjim/go-rapid/remoting"
)

type View struct {
}

func (v *View) MonitorsForNode(addr node.Addr) []node.Addr {
	if v.isHostPresent(addr) {
		return v.monitorsOfNode(addr)
	}
	return v.expectedMonitorsOfNode(addr)
}

func (v *View) LinkStatusForNode(addr node.Addr) remoting.LinkStatus {
	if v.isHostPresent(addr) {
		return remoting.LinkStatus_DOWN
	}
	return remoting.LinkStatus_UP
}

func (v *View) isHostPresent(addr node.Addr) bool {
	return false
}

func (v *View) monitorsOfNode(addr node.Addr) []node.Addr {
	return nil
}

func (v *View) expectedMonitorsOfNode(addr node.Addr) []node.Addr {
	return nil
}
