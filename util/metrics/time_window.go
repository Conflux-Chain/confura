package metrics

import (
	"container/list"
	"time"
)

// time window slot data
type SlotData interface {
	Accumulate(SlotData) SlotData
	Dissipate(SlotData) SlotData
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
	slots          *list.List    // double linked slots chronologically
	slotInterval   time.Duration // time interval per slot
	windowInterval time.Duration // time window interval
	aggData        SlotData      // aggregation slot data
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
	now := time.Now()

	// expired slots
	tw.expire(now)

	// update slot data
	if slot, ok := tw.getOrAddSlot(now, sample); ok {
		slot.data = slot.data.Accumulate(sample)
	}

	// update aggregation data
	tw.aggData = tw.aggData.Accumulate(sample)
}

// Aggregate aggregates the time window
func (tw *TimeWindow) Aggregate() SlotData {
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
		tw.aggData = tw.aggData.Dissipate(s.data)
	}
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

// getOrAddSlot gets the last slot or adds a new slot if the last one out of date.
func (tw *TimeWindow) getOrAddSlot(now time.Time, data SlotData) (*slot, bool) {
	// time window is empty
	if tw.slots.Len() == 0 {
		return tw.addNewSlot(now, data), false
	}

	// last slot is not out of date
	lastSlot := tw.slots.Back().Value.(*slot)
	if !lastSlot.outdated(now) {
		return lastSlot, true
	}

	// otherwise, add new slot
	return tw.addNewSlot(now, data), false
}
