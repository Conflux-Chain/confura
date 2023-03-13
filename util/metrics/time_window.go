package metrics

import (
	"container/list"
	"sync"
	"time"
)

// SlotData aggregatable slot data for time window
type SlotData interface {
	Add(SlotData) SlotData // accumulate
	Sub(SlotData) SlotData // dissipate
}

// time window slot
type slot struct {
	data     SlotData  // slot data
	endTime  time.Time // end time for slot update
	expireAt time.Time // expiry time to remove
}

// check if slot expired (can be purged)
func (s slot) expired(now time.Time) bool {
	return s.expireAt.Before(now)
}

// check if slot outdated (not open for update)
func (s slot) outdated(now time.Time) bool {
	return s.endTime.Before(now)
}

// TimeWindow slices time window into slots and maintains slot expiry and creation
type TimeWindow struct {
	mu sync.RWMutex

	slots          *list.List    // double linked slots chronologically
	slotInterval   time.Duration // time interval per slot
	windowInterval time.Duration // time window interval
	aggData        SlotData      // aggregation data within the time window scope
}

func NewTimeWindow(slotInterval time.Duration, numSlots int) *TimeWindow {
	return &TimeWindow{
		slots:          list.New(),
		slotInterval:   slotInterval,
		windowInterval: slotInterval * time.Duration(numSlots),
	}
}

// Add adds data sample to time window
func (tw *TimeWindow) Add(sample SlotData) {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	now := time.Now()

	// expire outdated slots
	tw.expire(now)
	// add or update slot data
	tw.addOrUpdateSlot(now, sample)
}

// Data returns the aggregation data within the time window scope
func (tw *TimeWindow) Data() SlotData {
	tw.mu.RLock()
	defer tw.mu.RUnlock()

	return tw.aggData
}

// expire removes expired slots.
func (tw *TimeWindow) expire(now time.Time) {
	for {
		// time window is empty
		front := tw.slots.Front()
		if front == nil {
			return
		}

		// not expired yet
		s := front.Value.(*slot)
		if !s.expired(now) {
			return
		}

		// remove expired slot
		tw.slots.Remove(front)

		// dissipate expired slot data
		tw.aggData = tw.aggData.Sub(s.data)
	}
}

// addOrUpdateSlot adds a new slot with the provided slot data if no one exists or
// the last one is out of date; otherwise update the last slot with the provided data.
func (tw *TimeWindow) addOrUpdateSlot(now time.Time, data SlotData) *slot {
	defer func() { // update aggregation data
		tw.aggData = tw.aggData.Add(data)
	}()

	// time window is empty
	if tw.slots.Len() == 0 {
		return tw.addNewSlot(now, data)
	}

	// last slot is out of date
	lastSlot := tw.slots.Back().Value.(*slot)
	if lastSlot.outdated(now) {
		return tw.addNewSlot(now, data)
	}

	// otherwise, update the last slot with new data
	lastSlot.data = lastSlot.data.Add(data)
	return lastSlot
}

// addNewSlot always appends a new slot to time window.
func (tw *TimeWindow) addNewSlot(now time.Time, data SlotData) *slot {
	slotStartTime := now.Truncate(tw.slotInterval)

	newSlot := &slot{
		data:     data,
		endTime:  slotStartTime.Add(tw.slotInterval),
		expireAt: slotStartTime.Add(tw.windowInterval),
	}

	tw.slots.PushBack(newSlot)
	return newSlot
}
