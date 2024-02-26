//go:build !ci
// +build !ci

package metrics

import (
	"os"
	"testing"
	"time"

	"github.com/Conflux-Chain/confura/util/test"
	"github.com/stretchr/testify/assert"
)

var (
	timeWin *TimeWindow
)

func TestMain(m *testing.M) {
	timeWin = NewTimeWindow(time.Second, 5)
	os.Exit(m.Run())
}

func TestTimeWindowAddNewSlot(t *testing.T) {
	if test.CITestOnly() {
		t.Skip("Skipping CI-only test")
	}

	startT := time.Now()

	sdata := twPercentageData{total: 1}
	slot := timeWin.addNewSlot(startT, sdata)

	assert.Equal(t, 1, timeWin.slots.Len())
	assert.Equal(t, sdata, slot.data)

	assert.True(t, !slot.expired(startT))
	assert.True(t, !slot.outdated(startT))

	testT := startT.Add(time.Second)
	assert.True(t, slot.outdated(testT))
	assert.True(t, !slot.expired(testT))

	testT = startT.Add(time.Second * 5)
	assert.True(t, slot.outdated(testT))
	assert.True(t, slot.expired(testT))
}

func TestTimeWindowExpire(t *testing.T) {
	if test.CITestOnly() {
		t.Skip("Skipping CI-only test")
	}

	startT := time.Now()
	slot := timeWin.addNewSlot(startT, twPercentageData{})

	testT := startT.Add(time.Second * 5)
	expSlots := timeWin.expire(testT)

	assert.Equal(t, 1, len(expSlots))
	assert.Equal(t, expSlots[0], slot)

	assert.Equal(t, timeWin.slots.Len(), 0)
}

func TestTimeWindowAddOrUpdateSlot(t *testing.T) {
	if test.CITestOnly() {
		t.Skip("Skipping CI-only test")
	}

	startT := time.Now()

	sdata := twPercentageData{total: 1}
	slot1, added := timeWin.addOrUpdateSlot(startT, sdata)

	assert.True(t, added)
	assert.Equal(t, slot1.data, sdata)

	slot1, added = timeWin.addOrUpdateSlot(startT, sdata)
	assert.False(t, added)
	assert.Equal(t, slot1.data, sdata.Add(sdata))

	testT := startT.Add(time.Second)
	slot2, added := timeWin.addOrUpdateSlot(testT, sdata)
	assert.True(t, added)
	assert.NotEqual(t, slot1, slot2)
}

func TestTimeWindowAdd(t *testing.T) {
	if test.CITestOnly() {
		t.Skip("Skipping CI-only test")
	}

	startT := time.Now()

	sdata := twPercentageData{total: 1}
	timeWin.add(startT, sdata)
	assert.Equal(t, sdata, timeWin.data(startT))

	testT := startT.Add(time.Second * 2)

	sdata = twPercentageData{total: 1, marks: 1}
	timeWin.add(testT, sdata)
	assert.Equal(t, twPercentageData{total: 2, marks: 1}, timeWin.data(testT))

	testT = startT.Add(time.Second * 6)
	assert.Equal(t, twPercentageData{total: 1, marks: 1}, timeWin.data(testT))
}
