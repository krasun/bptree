package bptree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"
)

func TestOrderError(t *testing.T) {
	_, err := New(Order(2))
	if err == nil {
		t.Fatal("must return an error, but it does not")
	}
}

func Example() {
	tree, _ := New()

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
	tree, _ := New()
	if tree == nil {
		t.Fatal("expected new *BPTree instance, but got nil")
	}
}

func TestPutAndGet(t *testing.T) {
	for order := 3; order <= 7; order++ {
		tree, _ := New(Order(order))

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
}

func TestSize(t *testing.T) {
	tree, _ := New()

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
	tree, _ := New()

	tree.Put(nil, []byte{1})

	_, ok := tree.Get(nil)
	if !ok {
		t.Fatalf("key nil is not found")
	}
}

func TestPutOverrides(t *testing.T) {
	tree, _ := New()

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
	tree, _ := New()

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
	tree, _ := New()

	value, ok := tree.Get([]byte{1})
	if value != nil {
		t.Fatalf("expected value to be nil, but got %s", value)
	}
	if ok {
		t.Fatalf("expected ok to be false, but got %v", ok)
	}
}

func TestForEach(t *testing.T) {
	tree, _ := New()
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
		t.Fatalf("%v != %v", expected, actual)
	}
}

func TestForEachForEmptyTree(t *testing.T) {
	tree, _ := New()

	tree.ForEach(func(key []byte, value []byte) {
		t.Fatal("call is not expected")
	})
}

func TestKeyOrder(t *testing.T) {
	tree, _ := New()
	for _, c := range treeCases {
		tree.Put([]byte{c.key}, []byte(c.value))
	}

	keys := make([]byte, len(treeCases))
	tree.ForEach(func(key, value []byte) {
		keys = append(keys, key[0])
	})

	if len(keys) == 0 {
		t.Fatal("keys are empty")
	}	
	isSorted := sort.SliceIsSorted(keys, func(i, j int) bool {
		return keys[i] < keys[j]
	})
	if !isSorted {
		t.Fatal("keys are not sorted")
	}
}

func TestPutAndGetRandomized(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	size := 10000
	keys := r.Perm(size)

	for order := 3; order <= 7; order++ {
		tree, _ := New(Order(order))

		for i, k := range keys {
			key := make([]byte, 4)
			binary.LittleEndian.PutUint32(key, uint32(k))
			value := make([]byte, 4)
			binary.LittleEndian.PutUint32(value, uint32(i))

			prev, exists := tree.Put(key, value)
			if prev != nil {
				t.Fatalf("the key already exists %v", k)
			}
			if exists {
				t.Fatalf("the key already exists %v", k)
			}
		}

		for i, k := range keys {
			expectedValue := uint32(i)
			key := make([]byte, 4)
			binary.LittleEndian.PutUint32(key, uint32(k))

			v, ok := tree.Get(key)
			if !ok {
				t.Fatalf("failed to get value by key %d, tree size = %d, order = %d", k, tree.Size(), order)
			}

			actualValue := binary.LittleEndian.Uint32(v)
			if expectedValue != actualValue {
				t.Fatalf("expected to get value %d fo key %d, but got %d", expectedValue, k, actualValue)
			}
		}
	}
}

func TestPutAndDeleteRandomized(t *testing.T) {
	r := rand.New(rand.NewSource(time.Now().Unix()))
	size := 10000
	keys := r.Perm(size)

	for order := 3; order <= 7; order++ {
		tree, _ := New(Order(order))

		for i, k := range keys {
			key := make([]byte, 4)
			binary.LittleEndian.PutUint32(key, uint32(k))
			value := make([]byte, 4)
			binary.LittleEndian.PutUint32(value, uint32(i))

			prev, exists := tree.Put(key, value)
			if prev != nil {
				t.Fatalf("the key already exists %v", k)
			}
			if exists {
				t.Fatalf("the key already exists %v", k)
			}
		}

		for i, k := range keys {
			expectedValue := uint32(i)
			key := make([]byte, 4)
			binary.LittleEndian.PutUint32(key, uint32(k))

			v, ok := tree.Delete(key)
			if !ok {
				t.Fatalf("failed to delete value by key %d, tree size = %d, order = %d", k, tree.Size(), order)
			}

			actualValue := binary.LittleEndian.Uint32(v)
			if expectedValue != actualValue {
				t.Fatalf("expected to delete value %d by key %d, and got %d", expectedValue, k, actualValue)
			}
		}
	}
}

func TestDeleteFromEmptyTree(t *testing.T) {
	tree, _ := New(Order(3))

	value, deleted := tree.Delete([]byte{1})
	if deleted {
		t.Fatalf("key %d is deleted, but should not, order %d", 1, 3)
	}
	if value != nil {
		t.Fatalf("value for key %d is not nil: %v", 1, value)
	}
}

func TestDeleteNonExistentElement(t *testing.T) {
	tree, _ := New(Order(3))

	tree.Put([]byte{1}, []byte{2})
	tree.Put([]byte{2}, []byte{2})
	tree.Put([]byte{3}, []byte{3})

	value, deleted := tree.Delete([]byte{4})
	if deleted {
		t.Fatalf("key %d is deleted, but should not, order %d", 4, 3)
	}
	if value != nil {
		t.Fatalf("value for key %d is not nil: %v", 4, value)
	}
}

func TestDeleteMergingThreeTimes(t *testing.T) {
	keys := []byte{7, 8, 4, 3, 2, 6, 11, 9, 10, 1, 12, 0, 5}

	tree, _ := New(Order(3))
	for _, v := range keys {
		tree.Put([]byte{v}, []byte{v})
	}

	for _, k := range keys {
		value, deleted := tree.Delete([]byte{k})
		if !deleted {
			t.Fatalf("key %d is not deleted, order %d", k, 3)
		}
		if value == nil {
			t.Fatalf("value for key %d is nil: %v", k, value)
		}
	}
}

func TestDelete(t *testing.T) {
	for order := 3; order <= 7; order++ {
		tree, _ := New(Order(order))
		for _, c := range treeCases {
			tree.Put([]byte{c.key}, []byte(c.value))
		}

		expectedSize := len(treeCases)
		for _, c := range treeCases {
			value, deleted := tree.Delete([]byte{c.key})
			expectedSize--

			if !deleted {
				t.Fatalf("key %d is not deleted, order %d", c.key, order)
			}
			if value == nil {
				t.Fatalf("value for key %d is nil: %v", c.key, value)
			}
			if expectedSize != tree.Size() {
				t.Fatalf("the expected size != actual: %d != %d", expectedSize, tree.Size())
			}
		}
	}
}

func TestForEachAfterDeletion(t *testing.T) {
	keys := []byte{7, 8, 4, 3, 2, 6, 11, 9, 10, 1, 12, 0, 5}

	tree, _ := New(Order(3))
	for _, v := range keys {
		tree.Put([]byte{v}, []byte{v})
	}

	for i, k := range keys {
		value, deleted := tree.Delete([]byte{k})
		if !deleted {
			t.Fatalf("key %d is not deleted, order %d", k, 3)
		}
		if value == nil {
			t.Fatalf("value for key %d is nil: %v", k, value)
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
		for j, k := range keys {
			if j > i {
				expected = append(expected, k)
			}
		}
		sort.Slice(expected, func(i, j int) bool {
			return expected[i] < expected[j]
		})

		if !reflect.DeepEqual(expected, actual) {
			t.Fatalf("%v != %v for key %d (%d)", expected, actual, k, i)
		}
	}
}

func TestNonExistentPointerPositionOf(t *testing.T) {
	tree, _ := New(Order(3))

	tree.Put([]byte{1}, []byte{2})
	tree.Put([]byte{2}, []byte{2})
	tree.Put([]byte{3}, []byte{3})

	actual := tree.root.pointerPositionOf(tree.root)
	if actual != -1 {
		t.Fatalf("should not locate root in the root, but found it")
	}
}

const benchmarkKeyNum = 10000

// to avoid code elimination by compiler
var BenchmarkTree *BPTree
var BenchmarkValue []byte

// closest implementation to []byte is []string
var BenchmarkMap map[string][]byte

func BenchmarkTreePut(b *testing.B) {
	for n := 0; n < b.N; n++ {
		BenchmarkTree, _ = New()

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
		BenchmarkTree, _ = New()

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
	BenchmarkTree, _ = New()
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
		BenchmarkTree, _ = New()

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
