package bptree

import (
	"bytes"
	"fmt"
)

const (
	defaultOrder = 4
)

// Option option configuration for B+ tree.
type Option func(*BPTree)

// Order sets the B+ tree order. The minimum order is 2.
func Order(order int) func(*BPTree) {
	if order < 3 {
		panic("order must be >= 3")
	}

	return func(t *BPTree) {
		t.order = order
	}
}

// BPTree is an in-memory implementation of the B+ tree data structure.
// The tree is not goroutine-safe and access to it must be synchronized.
type BPTree struct {
	root *node

	// The pointer to the most leftmost leaf node
	// to simplify iteration over the leaf nodes.
	leftmost *node

	// The order or branching factor of a B+ tree measures the capacity of nodes
	// for internal nodes in the tree.
	order int

	// the number of keys in tree
	size int

	// minimum allowed number of keys in the tree ceil(order/2)-1
	minKeyNum int
}

// New returns a new instance of the B+ tree.
func New(options ...Option) *BPTree {
	t := &BPTree{order: defaultOrder}

	for _, option := range options {
		option(t)
	}

	t.minKeyNum = ceil(t.order, 2) - 1

	return t
}

// Get returns a value by the key. The second return
// value is a flag that determines if the key was found.
func (t *BPTree) Get(key []byte) ([]byte, bool) {
	if t.root == nil {
		return nil, false
	}

	leaf := t.findLeaf(key)
	for i := 0; i < leaf.keyNum; i++ {
		if compare(key, leaf.keys[i]) == 0 {
			return leaf.pointers[i].asValue(), true
		}
	}

	return nil, false
}

// findLeaf finds a leaf that might contain the key.
func (t *BPTree) findLeaf(key []byte) *node {
	current := t.root
	for !current.leaf {
		position := 0
		for position < current.keyNum {
			if less(key, current.keys[position]) {
				break
			} else {
				position += 1
			}
		}

		current = current.pointers[position].asNode()
	}

	return current
}

// Put inserts the value into the tree. If the key already exists,
// it overrides it.
// Returns true and the previous value if the value has been overridden,
// otherwise false.
func (t *BPTree) Put(key, value []byte) ([]byte, bool) {
	if t.root == nil {
		t.initializeRoot(key, value)

		return nil, false
	}

	leaf := t.findLeaf(key)

	return t.putIntoLeaf(leaf, key, value)
}

// initializeRoot initializes root in the empty tree.
func (t *BPTree) initializeRoot(key, value []byte) {
	// new tree
	keys := make([][]byte, t.order-1)
	keys[0] = copyBytes(key)

	pointers := make([]*pointer, t.order)
	pointers[0] = &pointer{value}

	t.root = &node{
		leaf:     true,
		parent:   nil,
		keys:     keys,
		keyNum:   1,
		pointers: pointers,
	}

	t.leftmost = t.root
	t.size++
}

// putIntoLeaf puts key and value into the node.
func (t *BPTree) putIntoLeaf(n *node, k, v []byte) ([]byte, bool) {
	insertPos := 0
	for insertPos < n.keyNum {
		cmp := compare(k, n.keys[insertPos])
		if cmp == 0 {
			// found the exact match
			oldValue := n.pointers[insertPos].overrideValue(v)

			return oldValue, true
		} else if cmp < 0 {
			// found the insert position,
			// can break the loop
			break
		}

		insertPos++
	}

	// if we did not find the same key, we continue to insert
	if n.keyNum < len(n.keys) {
		// if the node is not full

		// shift the keys and pointers
		for j := n.keyNum; j > insertPos; j-- {
			n.keys[j] = n.keys[j-1]
			n.pointers[j] = n.pointers[j-1]
		}

		// insert
		n.keys[insertPos] = k
		n.pointers[insertPos] = &pointer{v}
		// and update key num
		n.keyNum++
	} else {
		// if the node is full
		parent := n.parent
		left, right := t.putIntoLeafAndSplit(n, insertPos, k, v)
		insertKey := right.keys[0]

		for left != nil && right != nil {
			if parent == nil {
				t.putIntoNewRoot(insertKey, left, right)
				break
			} else {
				if parent.keyNum < len(parent.keys) {
					// if the parent is not full
					t.putIntoParent(parent, insertKey, left, right)
					break
				} else {
					// if the parent is full
					// split parent, insert into the new parent and continue
					insertKey, left, right = t.putIntoParentAndSplit(parent, insertKey, left, right)
				}
			}

			parent = parent.parent
		}
	}

	t.size++

	return nil, false
}

// putIntoParent puts the node into the parent and update the left and the right
// pointers.
func (t *BPTree) putIntoParent(parent *node, k []byte, l, r *node) {
	insertPos := 0
	for insertPos < parent.keyNum {
		if less(k, parent.keys[insertPos]) {
			// found the insert position,
			// can break the loop
			break
		}

		insertPos++
	}

	// shift the keys and pointers
	parent.pointers[parent.keyNum+1] = parent.pointers[parent.keyNum]
	for j := parent.keyNum; j > insertPos; j-- {
		parent.keys[j] = parent.keys[j-1]
		parent.pointers[j] = parent.pointers[j-1]
	}

	// insert
	parent.keys[insertPos] = k
	parent.pointers[insertPos] = &pointer{l}
	parent.pointers[insertPos+1] = &pointer{r}
	// and update key num
	parent.keyNum++

	l.parent = parent
	r.parent = parent
}

// putIntoNewRoot creates new root, inserts left and right entries
// and updates the tree.
func (t *BPTree) putIntoNewRoot(key []byte, l, r *node) {
	// new root
	newRoot := &node{
		leaf:     false,
		keys:     make([][]byte, t.order-1),
		pointers: make([]*pointer, t.order),
		parent:   nil,
		keyNum:   1, // we are going to put just one key
	}

	newRoot.keys[0] = key
	newRoot.pointers[0] = &pointer{l}
	newRoot.pointers[1] = &pointer{r}

	l.parent = newRoot
	r.parent = newRoot

	t.root = newRoot
}

// putIntoParentAndSplit puts key in the parent, splits the node and returns the splitten
// nodes with all fixed ponters.
func (t *BPTree) putIntoParentAndSplit(parent *node, k []byte, l, r *node) ([]byte, *node, *node) {
	insertPos := 0
	for insertPos < parent.keyNum {
		if less(k, parent.keys[insertPos]) {
			// found the insert position,
			// can break the loop
			break
		}

		insertPos++
	}

	right := &node{
		leaf:     false,
		keys:     make([][]byte, t.order-1),
		keyNum:   0,
		pointers: make([]*pointer, t.order),
		parent:   nil,
	}

	middlePos := ceil(len(parent.keys), 2)
	copyFrom := middlePos
	if insertPos < middlePos {
		// since the elements will be shifted
		copyFrom -= 1
	}

	copy(right.keys, parent.keys[copyFrom:])
	copy(right.pointers, parent.pointers[copyFrom:])
	// copy the pointer to the next node
	right.keyNum = len(right.keys) - copyFrom

	// the given node becomes the left node
	left := parent
	left.keyNum = copyFrom
	// clean up keys and pointers
	for i := len(left.keys) - 1; i >= copyFrom; i-- {
		left.keys[i] = nil
		left.pointers[i+1] = nil
	}

	insertNode := left
	if insertPos >= middlePos {
		insertNode = right
		insertPos -= middlePos
	}

	// insert into the node
	insertNode.pointers[insertNode.keyNum+1] = insertNode.pointers[insertNode.keyNum]
	for j := insertNode.keyNum; j > insertPos; j-- {
		insertNode.keys[j] = insertNode.keys[j-1]
		insertNode.pointers[j] = insertNode.pointers[j-1]
	}

	insertNode.keys[insertPos] = k
	insertNode.pointers[insertPos] = &pointer{l}
	insertNode.pointers[insertPos+1] = &pointer{r}
	insertNode.keyNum++

	l.parent = insertNode
	r.parent = insertNode

	middleKey := right.keys[0]

	// clean up the right node
	for i := 1; i < right.keyNum; i++ {
		right.keys[i-1] = right.keys[i]
		right.pointers[i-1] = right.pointers[i]
	}
	right.pointers[right.keyNum-1] = right.pointers[right.keyNum]
	right.pointers[right.keyNum] = nil
	right.keys[right.keyNum-1] = nil
	right.keyNum--

	// update the pointers
	// TODO: update only changed pointers, because for large branching factors
	// the performance will degrade
	for _, p := range left.pointers {
		if p != nil {
			p.asNode().parent = left
		}
	}
	for _, p := range right.pointers {
		if p != nil {
			p.asNode().parent = right
		}
	}

	return middleKey, left, right
}

// putIntoLeafAndSplit puts the new key and splits the node into the left and right nodes
// and returns the left and the right nodes.
// The given node becomes left node.
// The tree is right-biased, so the first element in
// the right node is the "middle" key.
func (t *BPTree) putIntoLeafAndSplit(n *node, insertPos int, k, v []byte) (*node, *node) {
	right := &node{
		leaf:     true,
		keys:     make([][]byte, t.order-1),
		keyNum:   0,
		pointers: make([]*pointer, t.order),
		parent:   nil,
	}

	middlePos := ceil(len(n.keys), 2)
	copyFrom := middlePos
	if insertPos < middlePos {
		// since the elements will be shifted
		copyFrom -= 1
	}

	copy(right.keys, n.keys[copyFrom:])
	copy(right.pointers, n.pointers[copyFrom:len(n.pointers)-1])

	// copy the pointer to the next node
	right.setNext(n.next())
	right.keyNum = len(right.keys) - copyFrom

	// the given node becomes the left node
	left := n
	left.parent = nil
	left.keyNum = copyFrom
	// clean up keys and pointers
	for i := len(left.keys) - 1; i >= copyFrom; i-- {
		left.keys[i] = nil
		left.pointers[i] = nil
	}
	left.setNext(&pointer{right})

	insertNode := left
	if insertPos >= middlePos {
		insertNode = right
		// normalize insert position
		insertPos -= middlePos
	}

	// insert into the node
	insertNode.insertAt(insertPos, k, &pointer{v})

	return left, right
}

// Delete deletes the key from the tree. Returns deleted value and true
// if the key exists, otherwise nil and false.
func (t *BPTree) Delete(key []byte) ([]byte, bool) {
	if t.root == nil {
		return nil, false
	}

	leaf := t.findLeaf(key)
	if leaf == nil {
		return nil, false
	}

	value, deleted := t.deleteAtLeafAndRebalance(leaf, key)
	if !deleted {
		return nil, false
	}

	// TODO: find internal node/parent node and remove the key from it

	// parent := leaf.parent
	// for parent != nil {
	// 	// TODO: implement delete from indices
	// 	t.deleteAtInternalAndRebalance(parent, key)
	// 	parent = parent.parent
	// }

	t.size--

	return value, true
}

// deleteAtInternalAndRebalance deletes the key from the given node and rebalances it.
func (t *BPTree) deleteAtInternalAndRebalance(n *node, key []byte) {

}

// deleteAtLeafAndRebalance deletes the key from the given node and rebalances it.
func (t *BPTree) deleteAtLeafAndRebalance(n *node, key []byte) ([]byte, bool) {
	keyPos := n.keyPosition(key)
	if keyPos == -1 {
		return nil, false
	}

	value := n.pointers[keyPos].asValue()

	leafPointerPos := n.parent.pointerPositionOf(n)
	leafKeyPosInParent := leafPointerPos - 1
	if leafKeyPosInParent < 0 {
		leafKeyPosInParent = 0
	}

	n.deleteAt(keyPos)

	parent := n.parent
	if parent == nil {
		// remove from root (as leaf)
		if n.keyNum == 0 {
			t.root = nil
		}

		return value, true
	}

	if n.keyNum < t.minKeyNum {
		// try redistribute nodes for the leaf

		// check left sibling
		leftSibPos := leafPointerPos - 1
		borrowFromRight := true
		if leftSibPos >= 0 {
			// if left sibling exists 

			// borrow from left sibling
			leftSib := parent.pointers[leftSibPos].asNode()
			if leftSib.keyNum > t.minKeyNum {
				// borrow from the left sibling
				k, v := leftSib.keys[leftSib.keyNum-1], leftSib.pointers[leftSib.keyNum-1]
				n.insertAt(0, k, v)
				leftSib.deleteAt(leftSib.keyNum - 1)
				parent.keys[leafKeyPosInParent] = n.keys[0]

				borrowFromRight = false
			}
		}

		rightSibPos := leafPointerPos + 1
		tryMerge := true
		if borrowFromRight && rightSibPos < parent.keyNum+1 {
			// if right sibling exists 

			rightSib := parent.pointers[rightSibPos].asNode()
			if rightSib.keyNum > t.minKeyNum {
				// borrow from the right sibling
				k, v := rightSib.keys[0], rightSib.pointers[0]
				n.append(k, v)
				rightSib.deleteAt(0)
				parent.keys[leafKeyPosInParent] = rightSib.keys[0]

				tryMerge = false
			}
		}

		if tryMerge {
			// remove from the parent
			parent.deleteAt(leafKeyPosInParent)

			if parent.keyNum < t.minKeyNum {
				fmt.Printf("parent.keyNum = %d, t.minKeyNum=%d\n", parent.keyNum, t.minKeyNum)
				// TODO: what to do with pointers, how to check them
				panic("not implemented parent.keyNum < t.minKeyNum")
			}

			if n == t.leftmost && parent.keyNum > 0 {
				t.leftmost = parent.pointers[0].asNode()
			}
		}
	}

	return value, true
}

// ForEach traverses tree in ascending key order.
func (t *BPTree) ForEach(action func(key []byte, value []byte)) {
	for it := t.Iterator(); it.HasNext(); {
		key, value := it.Next()
		action(key, value)
	}
}

// Size return the size of the tree.
func (t *BPTree) Size() int {
	return t.size
}

// node reprents a node in the B+ tree.
type node struct {
	// true for leaf node and root without children
	// and false for internal node and root with children
	leaf   bool
	parent *node

	// Real key number is stored under the keyNum.
	keys   [][]byte
	keyNum int

	// Leaf nodes can point to the value,
	// but internal nodes point to the nodes. So
	// to save space, we can use pointers abstraction.
	// The size of pointers equals to the size of keys + 1.
	// In the leaf node, the last pointers element points to
	// the next leaf node.
	pointers []*pointer
}

//  keyPosition returns the position of the key, but -1 if it is not present.
func (n *node) keyPosition(key []byte) int {
	keyPosition := 0
	for ; keyPosition < n.keyNum; keyPosition++ {
		if compare(key, n.keys[keyPosition]) == 0 {
			return keyPosition
		}

		if keyPosition == n.keyNum-1 {
			// reached the end, but the key is not found
			return -1
		}
	}

	return -1
}

// append apppends key and the pointer to the node
func (n *node) append(key []byte, p *pointer) {
	if n.leaf {
		n.keys[n.keyNum] = key
		n.pointers[n.keyNum] = p
		n.keyNum++
	} else {
		// TODO: implement
		panic("not implemented")
	}
}

// deleteAt deletes the entry at the position and shifts
// the keys and the pointers. The given position must be
// the position of the pointer. The position of the key will be
// computed automatically.
func (n *node) deleteAt(keyPosition int) {
	// delete the key and the pointer and shift elements
	pointerPosition := keyPosition
	if !n.leaf && keyPosition != 0 {
		pointerPosition++
	}

	// shift the keys
	for j := keyPosition; j < n.keyNum-1; j++ {
		n.keys[j] = n.keys[j+1]
	}
	n.keys[n.keyNum-1] = nil

	pointerNum := n.keyNum
	if !n.leaf {
		pointerNum++
	}
	// shift the pointers
	for j := pointerPosition; j < pointerNum-1; j++ {
		n.pointers[j] = n.pointers[j+1]
	}
	n.pointers[pointerNum-1] = nil

	n.keyNum--
}

// pointerPositionOf finds the pointer position of the given node.
// Returns -1 if it is not found.
func (n *node) pointerPositionOf(x *node) int {
	for position, pointer := range n.pointers {
		if pointer == nil {
			break
		}

		if pointer.asNode() == x {
			return position
		}
	}

	return -1
}

// insertAt inserts the specified key and pointer at the specified position.
// Only works with leaf nodes.
func (n *node) insertAt(position int, key []byte, pointer *pointer) {
	for j := n.keyNum; j > position; j-- {
		n.keys[j] = n.keys[j-1]
		n.pointers[j] = n.pointers[j-1]
	}

	n.keys[position] = key
	n.pointers[position] = pointer
	n.keyNum++
}

// setNext sets the "next" pointer (the last pointer) to the next node. Only relevant
// for the leaf nodes.
func (n *node) setNext(p *pointer) {
	n.pointers[len(n.pointers)-1] = p
}

// next returns the pointer to the next leaf node. Only relevant
// for the leaf nodes.
func (n *node) next() *pointer {
	return n.pointers[len(n.pointers)-1]
}

// pointer wraps the node or the value.
type pointer struct {
	value interface{}
}

// asNode returns a asNode instance of the pointer.
func (p *pointer) asNode() *node {
	return p.value.(*node)
}

// asValue returns a asValue instance of the value.
func (p *pointer) asValue() []byte {
	return p.value.([]byte)
}

// overrideValue overrides the value
func (p *pointer) overrideValue(newValue []byte) []byte {
	oldValue := p.value.([]byte)
	p.value = newValue

	return oldValue
}

func compare(x, y []byte) int {
	return bytes.Compare(x, y)
}

func less(x, y []byte) bool {
	return compare(x, y) < 0
}

func copyBytes(s []byte) []byte {
	c := make([]byte, len(s))
	copy(c, s)

	return c
}

func ceil(x, y int) int {
	d := (x / y)
	if x%y == 0 {
		return d
	}

	return d + 1
}
