package bptree

type nodeType byte

const (
	internal nodeType = iota + 1
	leaf
)

// BPTree is an in-memory implementation of the B+ tree data structure.
// The tree is not goroutine-safe and access to it must be synchronized.
type BPTree struct {
	root *node
}

// node reprents a node in the B+ tree.
type node struct {
	t    nodeType
	keys [][]byte
	// pointer to the next leaf node
	next *node

	// only relevant for leaf nodes
	value []byte
}

// Returns a new instance of the B+ tree.
func New() *BPTree {
	return &BPTree{}
}

// Put inserts the value into the tree. If the key already exists,
// it overrides it.
// Returns true and the previous value if the value has been overridden,
// otherwise false.
func (t *BPTree) Put(key, value []byte) ([]byte, bool) {

	return nil, false
}

// Get returns a value by the key. The second return
// value is a flag that determines if the key was found.
func (t *BPTree) Get(key []byte) ([]byte, bool) {

	return nil, false
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
	return 0
}
