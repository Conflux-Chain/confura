//go:build !ci
// +build !ci

package metrics

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTrafficCollector(t *testing.T) {
	timeWin := NewTimeWindow(time.Second, 5)
	tc := &timeWindowTrafficCollector{window: timeWin}

	tvisitors := make([]Visitor, 100)
	for i := 0; i < 100; i++ {
		source := fmt.Sprintf("guest#%d", i)
		hits := 100 - i

		tvisitors[i] = Visitor{
			Hits: hits, Source: source,
		}

		for j := 0; j < hits; j++ {
			tc.MarkHit(source)
		}
	}

	for k := range []int{5, 10, 20} {
		topkVisitors := tc.TopkVisitors(k)
		assert.Equal(t, k, len(topkVisitors))

		for i := 0; i < k; i++ {
			assert.Equal(t, tvisitors[i], topkVisitors[i])
		}
	}

	time.Sleep(5 * time.Second)
	top10Visitors := tc.TopkVisitors(10)
	assert.Equal(t, 0, len(top10Visitors))
}
