package btree2d

import (
	"github.com/zenhotels/btree-2d/example/primary"
	"github.com/zenhotels/btree-2d/example/secondary"
)

type BTree2D interface {
	Sync(next BTree2D, onAdd, onDel func(key1 int, key2 string))
	GetLayer(key1 int) (secondary.Layer, bool)
	SetLayer(key1 int, layer secondary.Layer)
	ForEach(fn func(key int, layer secondary.Layer) bool)
	ForEach2(key1 int, fn func(key2 string) bool)
	Put(key1 int, key2 string, finalizers ...func())
	Delete(key1 int, key2 string) bool
	Drop(key1 int) bool
}

func New(cmp1 primary.CmpFunc, cmp2 secondary.CmpFunc) BTree2D {
	return btree2d{
		primary: primary.NewLayer(cmp1, cmp2),
	}
}

type btree2d struct {
	primary primary.Layer
}

func (prev btree2d) Sync(next BTree2D, onAdd, onDel func(key1 int, key2 string)) {
	nextBTree2D := next.(btree2d)
	prev.primary.Sync(nextBTree2D.primary, onAdd, onDel)
}

func (b btree2d) ForEach(fn func(key int, layer secondary.Layer) bool) {
	b.primary.ForEach(fn)
}

func (b btree2d) ForEach2(key1 int, fn func(key2 string) bool) {
	if layer2, ok := b.primary.Get(key1); ok {
		layer2.ForEach(func(k string, _ *secondary.FinalizerList) bool {
			return fn(k)
		})
	}
}

func (b btree2d) SetLayer(key1 int, layer secondary.Layer) {
	b.primary.Set(key1, layer)
}

func (b btree2d) GetLayer(key1 int) (secondary.Layer, bool) {
	return b.primary.Get(key1)
}

func (b btree2d) Drop(key1 int) bool {
	return b.primary.Drop(key1)
}

func (b btree2d) Put(key1 int, key2 string, finalizers ...func()) {
	b.primary.Put(key1, key2, finalizers...)
}

func (b btree2d) Delete(key1 int, key2 string) (ok bool) {
	layer2, ok := b.primary.Get(key1)
	if !ok {
		return false
	}
	return layer2.Delete(key2)
}
