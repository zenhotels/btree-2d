package btree2d

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zenhotels/btree-2d/common"
	"github.com/zenhotels/btree-2d/secondary"
)

// coverage: 21.3% of statements
func TestBTree2DSync(t *testing.T) {
	assert := assert.New(t)

	next := getTree(1000)
	var added int
	var deleted int
	empty := NewBTree2D()
	empty.Sync(next, func(_, _ common.Comparable) {
		// onAdd
		added++
	}, func(_, _ common.Comparable) {
		// onDel
		deleted++
	})

	assert.Equal(1000*1000, added)
	assert.Equal(0, deleted)

	var layer1 int
	var layer2 int
	empty.ForEach(func(_ common.Comparable, layer secondary.Layer) bool {
		layer1++
		layer.ForEach(func(_ secondary.Key) bool {
			layer2++
			return false
		})
		return false
	})

	assert.Equal(1000, layer1)
	assert.Equal(1000*1000, layer2)
}

func getTree(nLimit int, callbacks ...func()) BTree2D {
	next := NewBTree2D()
	for i := 0; i < nLimit; i++ {
		for j := 0; j < nLimit; j++ {
			info := &routeInfo{
				Host: uint64((i + 1) * (j + 1)),
			}
			next.Put(ID(i), NewFinalizable(info))
		}
	}
	return next
}

func BenchmarkTreeSync(b *testing.B) {
	next := getTree(100, func() {})
	b.ResetTimer()
	b.ReportAllocs()

	var added int
	var deleted int
	for i := 0; i < b.N; i++ {
		empty := NewBTree2D()
		empty.Sync(next, func(_, _ common.Comparable) {
			// onAdd
			added++
		}, func(_, _ common.Comparable) {
			// onDel
			deleted++
		})
	}

	b.StopTimer()
	if added != 10000*b.N {
		b.Fatal("wrong added count", added)
	}
}

// func BenchmarkRegistrySync(b *testing.B) {
// 	next := getRegistry(100, func() {})
// 	b.ResetTimer()
// 	b.ReportAllocs()

// 	var added int
// 	var deleted int
// 	for i := 0; i < b.N; i++ {
// 		var empty route.Registry
// 		empty.Sync(next, func(_ uint64, _ route.RouteInfo) {
// 			// onAdd
// 			added++
// 		}, func(_ uint64, _ route.RouteInfo) {
// 			// onDel
// 			deleted++
// 		})
// 	}

// 	b.StopTimer()
// 	if added != 10000*b.N {
// 		b.Fatal("wrong added count", added)
// 	}
// }

// func getRegistry(nLimit int, callbacks ...func()) *route.Registry {
// 	next := new(route.Registry)
// 	for i := 0; i < nLimit; i++ {
// 		for j := 0; j < nLimit; j++ {
// 			info := route.RouteInfo{
// 				Host: uint64((i + 1) * (j + 1)),
// 			}
// 			next.Push(uint64(i), info)
// 		}
// 	}
// 	return next
// }

type ID uint64

func (u ID) Less(u2 common.Comparable) bool {
	return u < u2.(ID)
}

type routeInfo struct {
	Host     uint64
	Distance int
	Upstream *http.Transport
}

func (r routeInfo) String() string {
	return fmt.Sprintf("{%d<-%s:%d}", r.Host, r.Upstream, r.Distance)
}

func (r routeInfo) Less(r2 common.Comparable) bool {
	return r.Host < r2.(*routeInfo).Host
}

// func (r *routeInfo) AddFinalizer(fn func()) bool {
// 	return false
// }

// func (r *routeInfo) Finalize() {

// }
