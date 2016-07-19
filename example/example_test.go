package example

import (
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xlab/treeprint"
	"github.com/zenhotels/btree-2d/example/btree2d"
	"github.com/zenhotels/btree-2d/example/secondary"
)

func cmpInt(a, b int) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	}
	return 0
}

func TestSync(t *testing.T) {
	assert := assert.New(t)

	prev := btree2d.New(cmpInt, strings.Compare)
	prev.Put(1, "hello world", func() {
		log.Println("bye cruel world")
	})
	prev.Put(2, "two")
	prev.Put(3, "three")
	log.Println("previous:", repr(prev))

	next := btree2d.New(cmpInt, strings.Compare)
	next.Put(3, "replaced")
	next.Put(4, "four")
	log.Println("next:", repr(next))

	added := make([]string, 0, 3)
	deleted := make([]string, 0, 3)
	prev.Sync(next, func(k1 int, k2 string) {
		text := fmt.Sprintf("%d(%s)", k1, k2)
		added = append(added, text)
	}, func(k1 int, k2 string) {
		text := fmt.Sprintf("%d(%s)", k1, k2)
		deleted = append(deleted, text)
	})

	assert.Len(added, 2)   // 1 replaced, 1 added
	assert.Len(deleted, 3) // 2 deleted, 1 replaced
	log.Println("added:", added)
	log.Println("deleted:", deleted)
	log.Println("after sync:", repr(prev))
}

func repr(t btree2d.BTree2D) string {
	tree := treeprint.New()
	t.ForEach(func(k int, layer secondary.Layer) bool {
		layerBranch := tree.AddBranch(k)
		layer.ForEach(func(key string, list *secondary.FinalizerList) bool {
			if list != nil {
				if funcs := list.Len(); funcs > 0 {
					funcsLabel := fmt.Sprintf("funcs: %d", funcs)
					layerBranch.AddMetaNode(funcsLabel, key)
					return false
				}
			}
			layerBranch.AddNode(key)
			return false
		})
		return false
	})
	return tree.String()
}
