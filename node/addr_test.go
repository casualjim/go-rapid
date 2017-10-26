package node

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
var javaHashes = []int64{
	-8142702818976521972,
	-7147371770735187532,
	363079388246039928,
	7534573388476618709,
	4737150535496919548,
	4933803927329709993,
	-2015114153036544862,
	2561866043046712830,
	-4765086671522938790,
	4678032494214207661,
}


permutation 0 for 127.0.0.1:2375 -6970849018552849470
permutation 1 for 127.0.0.1:2375 -5466037957856086435
permutation 2 for 127.0.0.1:2375 4815649783673873042
permutation 3 for 127.0.0.1:2375 -2607551934136458376
permutation 4 for 127.0.0.1:2375 -1121938748156261858
permutation 5 for 127.0.0.1:2375 -161944288917418436
permutation 6 for 127.0.0.1:2375 1500223810733435098
permutation 7 for 127.0.0.1:2375 5755618641853339409
permutation 8 for 127.0.0.1:2375 5868315971009178676
permutation 9 for 127.0.0.1:2375 -7774102069163897405
*/

func TestNodeChecksums(t *testing.T) {
	n, err := ParseAddr("127.0.0.1:2375")
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1", n.Host)
	assert.EqualValues(t, 2375, n.Port)

	for i := 0; i < 10; i++ {
		cs := n.Checksum(i)
		t.Logf("permutation %d for 127.0.0.1:2375 %d\n", i, cs)
	}
}
