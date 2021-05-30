package bptree

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"
)

func Example() {
	tree := New()

	tree.Put([]byte("apple"), []byte("sweet"))
	tree.Put([]byte("banana"), []byte("honey"))
	tree.Put([]byte("cinnamon"), []byte("savoury"))

	banana, ok := tree.Get([]byte("banana"))
	if ok {
		fmt.Printf("banana = %s\n", string(banana))
	} else {
		fmt.Println("value for banana not found")
	}

	tree.ForEach(func(key, value []byte) {
		fmt.Printf("key = %s, value = %s\n", string(key), string(value))
	})

	// Output:
	// banana = honey
	// key = apple, value = sweet
	// key = banana, value = honey
	// key = cinnamon, value = savoury
}

var treeCases = []struct {
	key   byte
	value string
}{
	{11, "11"},
	{18, "18"},
	{7, "7"},
	{15, "15"},
	{0, "0"},
	{16, "16"},
	{14, "14"},
	{33, "33"},
	{25, "25"},
	{42, "42"},
	{60, "60"},
	{2, "2"},
	{1, "1"},
	{74, "74"},
}

func TestNew(t *testing.T) {
	tree := New()
	if tree == nil {
		t.Fatal("expected new *BPTree instance, but got nil")
	}
}

func TestPutAndGet(t *testing.T) {
	tree := New()

	for _, c := range treeCases {
		prev, exists := tree.Put([]byte{c.key}, []byte(c.value))
		if prev != nil {
			t.Fatalf("the key already exists %v", c.key)
		}
		if exists {
			t.Fatalf("the key already exists %v", c.key)
		}
	}

	for _, c := range treeCases {
		value, ok := tree.Get([]byte{c.key})
		if !ok {
			t.Fatalf("failed to get value by key %d", c.key)
		}

		if string(value) != c.value {
			t.Fatalf("expected to get value %s fo key %d, but got %s", c.value, c.key, string(value))
		}
	}
}

func TestSize(t *testing.T) {
	tree := New()

	expected := 0
	for _, c := range treeCases {
		if expected != tree.Size() {
			t.Fatalf("actual size %d is not equal to expected size %d", tree.Size(), expected)
		}

		tree.Put([]byte{c.key}, []byte(c.value))
		expected++
	}
}

func TestNil(t *testing.T) {
	tree := New()

	tree.Put(nil, []byte{1})

	_, ok := tree.Get(nil)
	if !ok {
		t.Fatalf("key nil is not found")
	}
}

func TestPutOverrides(t *testing.T) {
	tree := New()

	prev, exists := tree.Put([]byte{1}, []byte{1})
	if prev != nil {
		t.Fatal("previous value must be nil for the new key")
	}
	if exists {
		t.Fatal("previous value must be nil for the new key")
	}

	prev, exists = tree.Put([]byte{1}, []byte{2})
	if !bytes.Equal(prev, []byte{1}) {
		t.Fatalf("previous value must be %v, but got %v", []byte{1}, prev)
	}
	if !exists {
		t.Fatalf("exists must be true for key %v", []byte{1})
	}

	value, ok := tree.Get([]byte{1})
	if !ok {
		t.Fatalf("key %d is not found, but must be overridden", 1)
	}

	if !bytes.Equal(value, []byte{2}) {
		t.Fatalf("key %d is not overridden", 1)
	}
}

func TestGetForNonExistentValue(t *testing.T) {
	tree := New()

	for _, c := range treeCases {
		tree.Put([]byte{c.key}, []byte(c.value))
	}

	value, ok := tree.Get([]byte{230})
	if value != nil {
		t.Fatalf("expected value to be nil, but got %s", value)
	}
	if ok {
		t.Fatalf("expected ok to be false, but got %v", ok)
	}
}

func TestGetForEmptyTree(t *testing.T) {
	tree := New()

	value, ok := tree.Get([]byte{1})
	if value != nil {
		t.Fatalf("expected value to be nil, but got %s", value)
	}
	if ok {
		t.Fatalf("expected ok to be false, but got %v", ok)
	}
}

func TestForEach(t *testing.T) {
	tree := New()
	for _, c := range treeCases {
		tree.Put([]byte{c.key}, []byte(c.value))
	}

	actual := make([]byte, 0)
	tree.ForEach(func(key []byte, value []byte) {
		actual = append(actual, key...)
	})

	isSorted := sort.SliceIsSorted(actual, func(i, j int) bool {
		return actual[i] < actual[j]
	})
	if !isSorted {
		t.Fatalf("each does not traverse in sorted order, produced result: %s", actual)
	}

	expected := make([]byte, 0)
	for _, c := range treeCases {
		expected = append(expected, c.key)
	}
	sort.Slice(expected, func(i, j int) bool {
		return expected[i] < expected[j]
	})

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("%s != %s", expected, actual)
	}
}

func TestForEachForEmptyTree(t *testing.T) {
	tree := New()

	tree.ForEach(func(key []byte, value []byte) {
		t.Fatal("call is not expected")
	})
}

func TestKeyOrder(t *testing.T) {
	tree := New()
	for _, c := range treeCases {
		tree.Put([]byte{c.key}, []byte(c.value))
	}

	keys := make([]byte, len(treeCases))
	tree.ForEach(func(key, value []byte) {
		keys = append(keys, key[0])
	})

	isSorted := sort.SliceIsSorted(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	if !isSorted {
		t.Fatal("keys are not sorted")
	}
}

// func TestRedBlackTreeProperties(t *testing.T) {
// 	tree := New()
// 	n := 256
// 	i := 0
// 	for k := n; k > 0; k-- {
// 		i++
// 		tree.Put([]byte{byte(k)}, []byte{byte(k)})
// 	}

// 	if tree.root.color != black {
// 		t.Fatal("tree root is not black")
// 	}

// 	if hasAdjacentRedNodes(tree.root) {
// 		t.Fatal("tree has adjacent red nodes")
// 	}

// 	h := height(tree.root)
// 	max := int(math.Floor(2 * math.Log2(float64(n+1))))
// 	if h > max {
// 		t.Fatalf("max height property has been violated: h=%d > max=2*log2(n+1)=%d", h, max)
// 	}

// 	valid := checkBlackNodes(tree.root)
// 	if !valid {
// 		t.Fatal("black nodes count on each path from root to any leaf must match")
// 	}
// }

// func countBlackNodes(node *node, count int, counters *[]int) {
// 	if node.left == nil && node.right == nil {
// 		*counters = append(*counters, count)
// 	}

// 	if node.left != nil {
// 		newCount := count
// 		if node.left.color == black {
// 			newCount++
// 		}

// 		countBlackNodes(node.left, newCount, counters)
// 	}

// 	if node.right != nil {
// 		newCount := count
// 		if node.right.color == black {
// 			newCount++
// 		}

// 		countBlackNodes(node.right, newCount, counters)
// 	}
// }

// func checkBlackNodes(node *node) bool {
// 	if node == nil {
// 		return true
// 	}

// 	counters := make([]int, 0)
// 	countBlackNodes(node, 0, &counters)

// 	prev := -1
// 	for _, count := range counters {
// 		if prev != -1 && count != prev {
// 			return false
// 		}
// 	}

// 	return true
// }

// func hasAdjacentRedNodes(node *node) bool {
// 	return node.parent != nil && node.parent.color == red && node.color == red
// }

// func height(node *node) int {
// 	if node == nil {
// 		return 0
// 	}

// 	l := height(node.left)
// 	r := height(node.right)

// 	if l > r {
// 		return l + 1
// 	}

// 	return r + 1
// }

const benchmarkKeyNum = 10000

// to avoid code elimination by compiler
var BenchmarkTree *BPTree
var BenchmarkValue []byte

// closest implementation to []byte is []string
var BenchmarkMap map[string][]byte

func BenchmarkTreePut(b *testing.B) {
	for n := 0; n < b.N; n++ {
		BenchmarkTree = New()

		for k := benchmarkKeyNum; k > 0; k-- {
			key := strconv.Itoa(k)
			BenchmarkTree.Put([]byte(key), []byte(key))
		}
	}
}

func BenchmarkMapPut(b *testing.B) {
	for n := 0; n < b.N; n++ {
		BenchmarkMap = make(map[string][]byte)

		for k := benchmarkKeyNum; k > 0; k-- {
			key := strconv.Itoa(k)
			BenchmarkMap[key] = []byte(key)
		}
	}
}

func BenchmarkTreePutRandomized(b *testing.B) {
	rand.Seed(time.Now().UnixNano())

	for n := 0; n < b.N; n++ {
		BenchmarkTree = New()

		for k := benchmarkKeyNum; k > 0; k-- {
			key := strconv.Itoa(rand.Intn(benchmarkKeyNum))
			BenchmarkTree.Put([]byte(key), []byte(key))
		}
	}
}

func BenchmarkMapPutRandomized(b *testing.B) {
	rand.Seed(time.Now().UnixNano())

	for n := 0; n < b.N; n++ {
		BenchmarkMap = make(map[string][]byte)

		for k := benchmarkKeyNum; k > 0; k-- {
			key := strconv.Itoa(rand.Intn(benchmarkKeyNum))
			BenchmarkMap[key] = []byte(key)
		}
	}
}

func BenchmarkMapGet(b *testing.B) {
	BenchmarkMap = make(map[string][]byte)

	for k := benchmarkKeyNum; k > 0; k-- {
		key := strconv.Itoa(k)
		BenchmarkMap[key] = []byte(key)
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		for k := 0; k < benchmarkKeyNum; k++ {
			key := strconv.Itoa(k)
			BenchmarkValue = BenchmarkMap[key]
		}
	}
}

func BenchmarkTreeGet(b *testing.B) {
	BenchmarkTree = New()
	for k := benchmarkKeyNum; k > 0; k-- {
		key := strconv.Itoa(k)
		BenchmarkTree.Put([]byte(key), []byte(key))
	}

	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		for k := 0; k < benchmarkKeyNum; k++ {
			key := strconv.Itoa(k)
			BenchmarkValue, _ = BenchmarkTree.Get([]byte(key))
		}
	}
}

func BenchmarkTreePutAndForEach(b *testing.B) {
	for n := 0; n < b.N; n++ {
		BenchmarkTree = New()

		for k := benchmarkKeyNum; k > 0; k-- {
			key := strconv.Itoa(k)
			BenchmarkTree.Put([]byte(key), []byte(key))
		}

		BenchmarkTree.ForEach(func(k, v []byte) {
			BenchmarkValue = v
		})
	}
}

func BenchmarkMapPutAndIterateAfterSort(b *testing.B) {
	for n := 0; n < b.N; n++ {
		BenchmarkMap = make(map[string][]byte)

		for k := benchmarkKeyNum; k > 0; k-- {
			key := strconv.Itoa(k)
			BenchmarkMap[key] = []byte(key)
		}

		keys := make([]string, 0)
		for key := range BenchmarkMap {
			keys = append(keys, key)
		}

		sort.Slice(keys, func(i, j int) bool {
			return keys[i] < keys[j]
		})

		for _, k := range keys {
			BenchmarkValue = BenchmarkMap[k]
		}
	}
}
