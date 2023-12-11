package lsp

import (
	"container/heap"
	"errors"
)

// PriorityQueue represents the queue
type PriorityQueue struct {
	itemHeap *itemHeap
}

// New initializes an empty priority queue.
func NewPriorityQueue() PriorityQueue {
	return PriorityQueue{
		itemHeap: &itemHeap{},
	}
}

// Len returns the number of elements in the queue.
func (p *PriorityQueue) Len() int {
	return p.itemHeap.Len()
}

// Insert inserts a new element into the queue. No action is performed on duplicate elements.
func (p *PriorityQueue) Insert(value interface{}, priority float64) {
	newItem := &item{
		value:    value,
		priority: priority,
	}
	heap.Push(p.itemHeap, newItem)
}

// Pop removes the element with the highest priority from the queue and returns it.
// In case of an empty queue, an error is returned.
func (p *PriorityQueue) Pop() (interface{}, error) {
	if len(*p.itemHeap) == 0 {
		return nil, errors.New("empty queue")
	}

	item := heap.Pop(p.itemHeap).(*item)
	return item.value, nil
}

func (p *PriorityQueue) Top() interface{} {
	item := p.itemHeap.Top()
	return item
}

// UpdatePriority changes the priority of a given item.
// If the specified item is not present in the queue, no action is performed.
func (p *PriorityQueue) UpdatePriority(value interface{}, newPriority float64) {
	for _, item := range *p.itemHeap {
		if item.value == value {
			item.priority = newPriority
			heap.Fix(p.itemHeap, item.index)
			return
		}
	}
}

type itemHeap []*item

type item struct {
	value    interface{}
	priority float64
	index    int
}

func (ih *itemHeap) Len() int {
	return len(*ih)
}

func (ih *itemHeap) Less(i, j int) bool {
	return (*ih)[i].priority < (*ih)[j].priority
}

func (ih *itemHeap) Swap(i, j int) {
	(*ih)[i], (*ih)[j] = (*ih)[j], (*ih)[i]
	(*ih)[i].index = i
	(*ih)[j].index = j
}

func (ih *itemHeap) Push(x interface{}) {
	it := x.(*item)
	it.index = len(*ih)
	*ih = append(*ih, it)
}

func (ih *itemHeap) Pop() interface{} {
	old := *ih
	item := old[len(old)-1]
	*ih = old[0 : len(old)-1]

	// item := (*ih)[0]
	// *ih = (*ih)[1:]
	return item
}

func (ih *itemHeap) Top() interface{} {
	return (*ih)[0].value
}
