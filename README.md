# **bp**tree

[![Build Status](https://app.travis-ci.com/krasun/bptree.svg?branch=main)](https://app.travis-ci.com/krasun/bptree)
[![codecov](https://codecov.io/gh/krasun/bptree/branch/main/graph/badge.svg?token=8NU6LR4FQD)](https://codecov.io/gh/krasun/bptree)
[![Go Report Card](https://goreportcard.com/badge/github.com/krasun/bptree)](https://goreportcard.com/report/github.com/krasun/bptree)
[![GoDoc](https://godoc.org/https://godoc.org/github.com/krasun/bptree?status.svg)](https://godoc.org/github.com/krasun/bptree)

An in-memory [B+ tree](https://en.wikipedia.org/wiki/B%2B_tree) implementation for Go with byte-slice keys and values. 

## Installation 

To install, run:

```
go get github.com/krasun/bptree
```

## Quickstart

Feel free to play: 

```go
package main

import (
	"fmt"

	"github.com/krasun/bptree"
)

func main() {
	tree, err := bptree.New()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
        os.Exit(1)
	}

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
```

You can use an iterator: 

```go
package main

import (
	"fmt"

	"github.com/krasun/bptree"
)

func main() {
	tree, err := bptree.New(bptree.Order(3))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
        os.Exit(1)
	}

	tree.Put([]byte("apple"), []byte("sweet"))
	tree.Put([]byte("banana"), []byte("honey"))
	tree.Put([]byte("cinnamon"), []byte("savoury"))

	banana, ok := tree.Get([]byte("banana"))
	if ok {
		fmt.Printf("banana = %s\n", string(banana))
	} else {
		fmt.Println("value for banana not found")
	}

	for it := tree.Iterator(); it.HasNext(); {
		key, value := it.Next()
		fmt.Printf("key = %s, value = %s\n", string(key), string(value))
	}

	// Output: 
	// banana = honey
	// key = apple, value = sweet
	// key = banana, value = honey
	// key = cinnamon, value = savoury
}
```

An iterator is stateful. You can have multiple iterators without any impact on each other, but make sure to synchronize access to them and the tree in a concurrent environment.

Caution! `Next` panics if there is no next element. Make sure to test for the next element with `HasNext` before.

## Use cases 

1. When you want to use []byte as a key in the map. 
2. When you want to iterate over keys in map in sorted order.

## Limitations 

**Caution!** To guarantee that the B+ tree properties are not violated, keys are copied. 

You should clearly understand what []byte slice is and why it is dangerous to use it as a key. Go language authors do prohibit using byte slice ([]byte) as a map key for a reason. The point is that you can change the values of the key and thus violate the invariants of map: 

```go
// if it worked 
b := []byte{1}
m := make(map[[]byte]int)
m[b] = 1

b[0] = 2 // it would violate the invariants 
m[[]byte{1}] // what do you expect to receive?
```

So to make sure that this situation does not occur in the tree, the key is copied byte by byte.

## Benchmark

Regular Go map is as twice faster for put and get than B+ tree. But if you 
need to iterate over keys in sorted order, the picture is slightly different: 

```
$ go test -benchmem -bench .                                                                                            127 ↵
goos: darwin
goarch: amd64
pkg: github.com/krasun/bptree
BenchmarkTreePut-8                     	     187	   6423171 ns/op	 2825134 B/op	   99844 allocs/op
BenchmarkMapPut-8                      	     525	   2736062 ns/op	 1732158 B/op	   20150 allocs/op
BenchmarkTreePutRandomized-8           	     177	   6745088 ns/op	 1622519 B/op	   69431 allocs/op
BenchmarkMapPutRandomized-8            	     612	   1944303 ns/op	  981396 B/op	   20111 allocs/op
BenchmarkMapGet-8                      	    1484	    704045 ns/op	   38880 B/op	    9900 allocs/op
BenchmarkTreeGet-8                     	     505	   2184212 ns/op	   38880 B/op	    9900 allocs/op
BenchmarkTreePutAndForEach-8           	     181	   6958273 ns/op	 2825133 B/op	   99844 allocs/op
BenchmarkMapPutAndIterateAfterSort-8   	     205	   5473439 ns/op	 2558078 B/op	   20172 allocs/op
PASS
ok  	github.com/krasun/bptree	15.460s
```

## Tests

Run tests with: 

```
$ go test -cover .
ok  	github.com/krasun/bptree	0.468s	coverage: 100.0% of statements
```

## License 

**bp**tree is released under [the MIT license](LICENSE).