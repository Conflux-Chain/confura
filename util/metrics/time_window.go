package metrics

import (
	"container/list"
	"time"
)

type slotFactory func(ctx slotContext) slot

// slot slot sliced for time-based window
type slot interface {
	expired(now time.Time) bool // check if slot expired or not
	update(v interface{})       // update slot data
	snapshot() interface{}      // snapshot slot data
}

type slotContext struct {
	endTime  time.Time // slot end time
	expireAt time.Time // expiry time to remove
}

func (ctx slotContext) expired(now time.Time) bool {
	return !ctx.expireAt.After(now)
}

// slotter slot slices time window and maintains slot expiry and creation
type slotter struct {
	slots          *list.List    // double linked slots chronologically
	slotInterval   time.Duration // time interval per slot
	windowInterval time.Duration // time window interval
}

func newSlotter(slotInterval time.Duration, numSlots int) *slotter {
	return &slotter{
		slots:          list.New(),
		slotInterval:   slotInterval,
		windowInterval: slotInterval * time.Duration(numSlots),
	}
}

// expire removes expired slots.
func (sl *slotter) expire(now time.Time) (expired []slot) {
	for {
		// time window is empty
		front := sl.slots.Front()
		if front == nil {
			break
		}

		// not expired yet
		s := front.Value.(slot)
		if !s.expired(now) {
			break
		}

		// remove expired slot
		sl.slots.Remove(front)
		expired = append(expired, s)
	}

	return expired
}

// addNewSlot always appends a new slot to time window.
func (sl *slotter) addNewSlot(now time.Time, slotf slotFactory) slot {
	slotStartTime := now.Truncate(sl.slotInterval)

	slotCtx := slotContext{
		endTime:  slotStartTime.Add(sl.slotInterval),
		expireAt: slotStartTime.Add(sl.windowInterval),
	}

	newSlot := slotf(slotCtx)
	sl.slots.PushBack(newSlot)

	return newSlot
}

// getOrAddSlot gets the last slot or adds a new slot if the last one out of date.
func (s *slotter) getOrAddSlot(now time.Time, slotf slotFactory) slot {
	// time window is empty
	if s.slots.Len() == 0 {
		return s.addNewSlot(now, slotf)
	}

	// last slot is not out of date
	lastSlot := s.slots.Back().Value.(slot)
	if !lastSlot.expired(now) {
		return lastSlot
	}

	// otherwise, add new slot
	return s.addNewSlot(now, slotf)
}
