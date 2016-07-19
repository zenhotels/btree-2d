package secondary

import (
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zenhotels/btree-2d/common"
)

type testKey struct {
	N int
}

func (k testKey) Less(k2 common.Comparable) bool {
	kk := k2.(*testKey)
	return k.N < kk.N
}

func (k testKey) String() string {
	return fmt.Sprintf("testKey: %d", k.N)
}

func (k *testKey) Finalize() {}

func (k *testKey) AddFinalizer(func()) bool { return false }

// coverage: 23.9% of statements
func TestSync(t *testing.T) {
	assert := assert.New(t)
	next := NewLayer()
	next.Set(Key{&testKey{1}})
	next.Set(Key{&testKey{2}})
	next.Set(Key{&testKey{3}})
	prev := NewLayer()
	prev.Set(Key{&testKey{1}})
	prev.Set(Key{&testKey{4}})

	added := make([]Key, 0, 3)
	deleted := make([]Key, 0, 3)
	prev.Sync(next, func(k Key) {
		added = append(added, k)
	}, func(k Key) {
		deleted = append(deleted, k)
	})

	assert.Len(added, 2)
	assert.Len(deleted, 1)
	log.Println("added:", added)
	log.Println("deleted:", deleted)
	assert.Equal(3, prev.Len())
}
