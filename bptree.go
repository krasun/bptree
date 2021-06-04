package bptree

import (
	"bytes"
)

const (
	defaultOrder = 4
)

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

// Returns a new instance of the B+ tree.
func New(options ...Option) *BPTree {
	t := &BPTree{order: defaultOrder}

	for _, option := range options {
		option(t)
	}

	return t
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

// Delete deletes the key from the tree. Returns true
// if the key exists, otherwise false.
func (t *BPTree) Delete(key []byte) bool {
	return false
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

// findLeaf finds a leaf at which new key should be put.
func (t *BPTree) findLeaf(key []byte) *node {
	current := t.root
	for !current.leaf {
		i := 0
		for i = 0; i < current.keyNum; i++ {
			k := current.keys[i]
			if less(key, k) {
				// position found
				current = current.pointers[i].asNode()
				break
			}
		}

		if !current.leaf && i == current.keyNum {
			// reached the end
			current = current.pointers[i].asNode()
		}
	}

	return current
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
		t.size++

		return nil, false
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

		t.size++
	}

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

	copy(right.keys, parent.keys[middlePos:])
	copy(right.pointers, parent.pointers[middlePos:])
	for _, p := range right.pointers {
		if p != nil {
			p.asNode().parent = right
		}
	}
	// copy the pointer to the next node
	right.keyNum = len(right.keys) - middlePos

	// the given node becomes the left node
	left := parent
	left.keyNum = middlePos
	// clean up keys and pointers
	for i := len(left.keys) - 1; i >= middlePos; i-- {
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

	copy(right.keys, n.keys[middlePos:])
	copy(right.pointers, n.pointers[middlePos:len(n.pointers)-1])

	// copy the pointer to the next node
	right.setLastPointer(n.lastPointer())
	right.keyNum = len(right.keys) - middlePos

	// the given node becomes the left node
	left := n
	left.parent = nil
	left.keyNum = middlePos
	// clean up keys and pointers
	for i := len(left.keys) - 1; i >= middlePos; i-- {
		left.keys[i] = nil
		left.pointers[i] = nil
	}
	left.setLastPointer(&pointer{right})

	insertNode := left
	if insertPos >= middlePos {
		insertNode = right
		insertPos -= middlePos
	}

	// insert into the node
	for j := insertNode.keyNum; j > insertPos; j-- {
		insertNode.keys[j] = insertNode.keys[j-1]
		insertNode.pointers[j] = insertNode.pointers[j-1]
	}

	insertNode.keys[insertPos] = k
	insertNode.pointers[insertPos] = &pointer{v}
	insertNode.keyNum++

	return left, right
}

func (n *node) setLastPointer(p *pointer) {
	n.pointers[len(n.pointers)-1] = p
}

func (n *node) lastPointer() *pointer {
	return n.pointers[len(n.pointers)-1]
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
