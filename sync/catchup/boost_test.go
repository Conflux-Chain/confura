package catchup

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSyncTaskPriorityQueue tests the priority queue with shrink strategy and normal operations
func TestSyncTaskPriorityQueue(t *testing.T) {
	// Initialize the priority queue
	pq := &syncTaskPriorityQueue{}

	// Test 1: Pushing items into the priority queue
	heap.Push(pq, &syncTaskItem{syncTask: newSyncTask(5, 10)})
	heap.Push(pq, &syncTaskItem{syncTask: newSyncTask(3, 7)})
	heap.Push(pq, &syncTaskItem{syncTask: newSyncTask(8, 15)})

	// Assert the priority queue has the correct length after pushing 3 tasks
	assert.Equal(t, 3, pq.Len())

	// Test 2: Ensure the items are ordered correctly by `From` field (min-heap)
	// The item with From=3 should be at the front
	item := heap.Pop(pq).(*syncTaskItem)
	assert.Equal(t, uint64(3), item.From)

	// The item with From=5 should now be at the front
	item = heap.Pop(pq).(*syncTaskItem)
	assert.Equal(t, uint64(5), item.From)

	// The item with From=8 should now be at the front
	item = heap.Pop(pq).(*syncTaskItem)
	assert.Equal(t, uint64(8), item.From)

	// Assert that the queue is empty after popping all tasks
	assert.Equal(t, 0, pq.Len())

	// Test 3: Ensure priority queue shrinks when the size drops below threshold
	// We need to fill the queue with more than 100 elements to trigger the shrink

	// Push `minPqShrinkCapacity+10` items into the priority queue
	for i := 0; i < minPqShrinkCapacity+10; i++ {
		heap.Push(pq, &syncTaskItem{syncTask: newSyncTask(uint64(i), uint64(i+1))})
	}

	// Assert that the queue's length is `minPqShrinkCapacity+10`
	assert.Equal(t, minPqShrinkCapacity+10, pq.Len())

	// Get the current capacity of the priority queue
	oldCap := cap(*pq)

	// Pop enough items so that the queue should shrink
	for i := 0; i < minPqShrinkCapacity+1; i++ {
		heap.Pop(pq)
	}

	// After popping `minPqShrinkCapacity+1` items, the length should be 9
	assert.Equal(t, 9, pq.Len())

	// Ensure that the underlying array has shrunk (length should be less than 100 capacity)
	// The doubling strategy should make the capacity after shrinking equal to about 2*len(*pq)
	assert.True(t, oldCap > cap(*pq), "Priority queue should have shrunk")

	// Test 4: Verify that the elements are still intact after shrink
	// Check that the remaining elements in the queue are ordered and correct
	for i := minPqShrinkCapacity + 1; i < minPqShrinkCapacity+10; i++ {
		item := heap.Pop(pq).(*syncTaskItem)
		assert.Equal(t, uint64(i), item.From)
	}

	// Assert that the queue is empty after all items are popped
	assert.Equal(t, 0, pq.Len())
}
