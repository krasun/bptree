package bptree

// Iterator returns a stateful Iterator for traversing the tree
// in ascending key order.
type Iterator struct {
	next *node
	i    int
}

// Iterator returns a stateful iterator that traverses the tree
// in ascending key order.
func (t *BPTree) Iterator() *Iterator {
	return &Iterator{t.leftmost, 0}
}

// HasNext returns true if there is a next element to retrive.
func (it *Iterator) HasNext() bool {
	return it.next != nil && it.i < it.next.keyNum
}

// Next returns a key and a value at the current position of the iteration
// and advances the iterator.
// Caution! Next panics if called on the nil element.
func (it *Iterator) Next() ([]byte, []byte) {
	if !it.HasNext() {
		// to sleep well
		panic("there is no next node")
	}

	key, value := it.next.keys[it.i], it.next.pointers[it.i].asValue()

	it.i++
	if it.i == it.next.keyNum {
		nextPointer := it.next.lastPointer()
		if nextPointer != nil {
			it.next = nextPointer.asNode()
		} else {
			it.next = nil
		}

		it.i = 0
	}

	return key, value
}
