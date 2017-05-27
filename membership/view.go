package membership

import (
	"github.com/casualjim/go-rapid/node"
)

type View struct {
}

func (v *View) MonitorsForNode(addr node.Addr) []node.Addr {
	if v.isHostPresent(addr) {
		return v.monitorsOfNode(addr)
	}
	return v.expectedMonitorsOfNode(addr)
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
