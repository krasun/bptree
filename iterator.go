package bptree

// Iterator returns a stateful Iterator for traversing the tree
// in ascending key order.
type Iterator struct {
	next *node
}

// Iterator returns a stateful iterator that traverses the tree
// in ascending key order.
func (t *BPTree) Iterator() *Iterator {
	next := t.root
	// if next != nil {
	// 	for next.left != nil {
	// 		next = next.left
	// 	}
	// }

	return &Iterator{next}
}

// HasNext returns true if there is a next element to retrive.
func (it *Iterator) HasNext() bool {
	return it.next != nil
}

// Next returns a key and a value at the current position of the iteration
// and advances the iterator.
// Caution! Next panics if called on the nil element.
func (it *Iterator) Next() (key, value []byte) {
	if !it.HasNext() {
		// to sleep well
		panic("there is no next node")
	}

	// current := it.next
	// if it.next.right != nil {
	// 	it.next = it.next.right
	// 	for it.next.left != nil {
	// 		it.next = it.next.left
	// 	}

	// 	return current.key, current.value
	// }

	// for {
	// 	if it.next.parent == nil {
	// 		it.next = nil

	// 		return current.key, current.value
	// 	}
	// 	if it.next.parent.left == it.next {
	// 		it.next = it.next.parent

	// 		return current.key, current.value
	// 	}
	// 	it.next = it.next.parent
	// }

	return nil, nil
}
