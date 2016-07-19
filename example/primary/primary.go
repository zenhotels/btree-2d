package primary

import (
	"io"

	"github.com/zenhotels/btree-2d/example/secondary"
	"github.com/zenhotels/btree-2d/lockie"
	"github.com/zenhotels/btree-2d/util"
)

// CmpFunc compares a and b. Return value is:
//
//	< 0 if a <  b
//	  0 if a == b
//	> 0 if a >  b
//
type CmpFunc func(key1, key2 int) int

// Layer represents the primary layer,
// a tree holding comparable keys pointing to secondary layers.
type Layer struct {
	store  *Tree
	offset uint64
	synced *uint64 // id of the previously synced layer
	lock   lockie.Lockie
	cmp1   CmpFunc
	cmp2   secondary.CmpFunc
}

// NewLayer initializes a new primary layer handle.
func NewLayer(cmp1 CmpFunc, cmp2 secondary.CmpFunc) Layer {
	var synced uint64
	return Layer{
		synced: &synced,
		store:  NewTree(cmp1),
		offset: uint64(util.RevOffset()),
		lock:   lockie.NewLockie(),
		cmp1:   cmp1,
		cmp2:   cmp2,
	}
}

// Set just adds a secondary layer to the tree, overwriting the previous one.
// Note that this action would trigger the replaced layer finalizers.
func (l Layer) Set(k int, layer secondary.Layer) {
	l.lock.Lock()
	l.store.Put(k, func(oldLayer secondary.Layer, exists bool) (newLayer secondary.Layer, write bool) {
		if exists {
			oldLayer.Finalize()
		}
		return layer, true
	})
	l.lock.Unlock()
}

func (l Layer) Rev() uint64 {
	return l.store.Ver() + l.offset
}

// Put adds keys and callbacks to the secondary layer, which will be created
// if not yet existing.
func (l Layer) Put(k int, k2 string, finalizers ...func()) {
	l.lock.Lock()
	l.store.Put(k, func(oldLayer secondary.Layer, exists bool) (newLayer secondary.Layer, write bool) {
		if !exists {
			oldLayer = secondary.NewLayer(l.cmp2)
		}
		oldLayer.Put(k2, finalizers...)
		return oldLayer, true
	})
	l.lock.Unlock()
}

// Seek returns an Enumerator positioned on a secondary layer such that k >= layer's key.
func (l Layer) Seek(k int) (e *Enumerator, ok bool) {
	l.lock.Lock()
	e, ok = l.store.Seek(k)
	l.lock.Unlock()
	return
}

// SeekFirst returns an Enumerator positioned on the first secondary layer in the tree.
func (l Layer) SeekFirst() (e *Enumerator, err error) {
	l.lock.Lock()
	e, err = l.store.SeekFirst()
	l.lock.Unlock()
	return
}

// ForEach runs the provided function for every element in the layer,
// if function returns true, the loop stops.
func (l Layer) ForEach(fn func(key int, layer secondary.Layer) bool) {
	l.lock.Lock()
	e, err := l.store.SeekFirst()
	l.lock.Unlock()
	if err != io.EOF {
		k, layer, err := e.Next()
		for err != io.EOF {
			if stop := fn(k, layer); stop {
				return
			}
			l.lock.Lock()
			k, layer, err = e.Next()
			l.lock.Unlock()
		}
		e.Close()
	}
}

// Drop removes the whole secondary layer associated with the key,
// invokes all the finalizers associated with elements of this secondary layer.
func (l Layer) Drop(k int) (ok bool) {
	l.lock.Lock()
	v, found := l.store.Get(k)
	if found {
		ok = l.store.Delete(k)
	}
	l.lock.Unlock()
	if found {
		v.Finalize()
	}
	return
}

// Get returns the secondary layer associated with the key.
func (l Layer) Get(k int) (layer secondary.Layer, ok bool) {
	l.lock.Lock()
	v, ok := l.store.Get(k)
	l.lock.Unlock()
	return v, ok
}

func (prev Layer) Sync(next Layer, onAdd, onDel func(key1 int, key2 string)) {
	if prev.store == next.store {
		return
	}
	// TODO(xlab): primary cannot handle changes on secondary layers.
	// Disable this feature for now
	//
	// nextRev := next.Rev()
	// if prevRev := atomic.LoadUint64(prev.synced); prevRev == nextRev {
	// 	log.Println()
	// 	return
	// }
	prev.lock.Lock()
	prevIter, prevErr := prev.store.SeekFirst()
	prev.lock.Unlock()
	next.lock.Lock()
	nextIter, nextErr := next.store.SeekFirst()
	next.lock.Unlock()

	switch {
	case prevErr == io.EOF && nextErr == io.EOF:
		// do nothing, both are empty
		return
	case prevErr == io.EOF:
		// previous storage is empty, everything is added
		addAll(prev, next.lock, nextIter, onAdd)
		nextIter.Close()
		return
	case nextErr == io.EOF:
		// next storage is empty, everything is deleted
		deleteAll(prev, prev.lock, prevIter, onDel)
		prevIter.Close()
		return
	default:
		// do sync and trigger the corresponding callbacks
		syncAll(prev, next, prevIter, nextIter, onAdd, onDel)
		prevIter.Close()
		nextIter.Close()
		return
	}
}

func addAll(prev Layer, nextLock lockie.Lockie, nextIter *Enumerator,
	onAdd func(key1 int, key2 string)) {

	nextLock.Lock()
	nextK, nextLayer, err := nextIter.Next()
	nextLock.Unlock()

	for err != io.EOF {
		if nextLayer.Len() > 0 {
			// create a new layer to set into prev
			newLayer := secondary.NewLayer(prev.cmp2)

			// fills layer while calling the onAdd callback
			if onAdd != nil {
				newLayer.Sync(nextLayer, func(k2 string) {
					onAdd(nextK, k2)
				}, nil)
			} else {
				newLayer.Sync(nextLayer, nil, nil)
			}

			// set the new layer into prev
			prev.lock.Lock()
			prev.store.Set(nextK, newLayer)
			prev.lock.Unlock()
		}
		// advance next iter
		nextLock.Lock()
		nextK, nextLayer, err = nextIter.Next()
		nextLock.Unlock()
	}
}

func deleteAll(prev Layer, prevLock lockie.Lockie, prevIter *Enumerator,
	onDel func(key1 int, key2 string)) {

	prevLock.Lock()
	prevK, prevLayer, err := prevIter.Next()
	prevLock.Unlock()

	for err != io.EOF {
		// nukes the prevLayer yet calling the onDel callback
		if onDel != nil {
			prevLayer.Sync(secondary.NewLayer(prev.cmp2), nil, func(k2 string) {
				onDel(prevK, k2)
			})
		} else {
			prevLayer.Sync(secondary.NewLayer(prev.cmp2), nil, nil)
		}

		// advance next iter
		prevLock.Lock()
		prevK, prevLayer, err = prevIter.Next()
		prevLock.Unlock()
	}
	// finally clear the store
	prevLock.Lock()
	prev.store.Clear()
	prevLock.Unlock()
}

func syncAll(prev, next Layer, prevIter, nextIter *Enumerator,
	onAdd, onDel func(k1 int, k2 string)) {

	prev.lock.Lock()
	prevK, prevLayer, prevErr := prevIter.Next()
	prev.lock.Unlock()
	next.lock.Lock()
	nextK, nextLayer, nextErr := nextIter.Next()
	next.lock.Unlock()

	for {
		switch {
		case prevErr == io.EOF:
			if nextErr == io.EOF {
				return // we're done
			}
			// at this point prev is ended, so nextK is added
			if nextLayer.Len() > 0 {
				// create a new layer to set into prev
				newLayer := secondary.NewLayer(prev.cmp2)

				// fills layer while calling the onAdd callback
				if onAdd != nil {
					newLayer.Sync(nextLayer, func(k2 string) {
						onAdd(nextK, k2)
					}, nil)
				} else {
					newLayer.Sync(nextLayer, nil, nil)
				}

				// set the new layer into prev
				prev.lock.Lock()
				prev.store.Set(nextK, newLayer)
				prev.lock.Unlock()
			}
			// move next iterator
			next.lock.Lock()
			nextK, nextLayer, nextErr = nextIter.Next()
			next.lock.Unlock()
			continue

		case nextErr == io.EOF:
			if prevErr == io.EOF {
				return // we're done
			}
			// at this point next is ended, so prevK is deleted
			if onDel != nil {
				prevLayer.ForEach(func(k2 string, v2 *secondary.FinalizerList) bool {
					if onDel != nil {
						onDel(prevK, k2)
					}
					if v2 != nil {
						v2.Finalize()
					}
					return false
				})
			} else {
				prevLayer.Finalize()
			}
			// delete prevK in prev
			prev.lock.Lock()
			prev.store.Delete(prevK)
			// move prev iterator
			prevK, prevLayer, prevErr = prevIter.Next()
			prev.lock.Unlock()
			continue
		}

		prevCmp := prev.cmp1(prevK, nextK)
		switch {
		case prevCmp < 0: // prevK < nextK
			// old prevK has been deleted apparently
			if onDel != nil {
				prevLayer.ForEach(func(k2 string, v2 *secondary.FinalizerList) bool {
					if onDel != nil {
						onDel(prevK, k2)
					}
					if v2 != nil {
						v2.Finalize()
					}
					return false
				})
			} else {
				prevLayer.Finalize()
			}

			// delete prevK in prev
			prev.lock.Lock()
			prev.store.Delete(prevK)
			// move prev iterator
			prevK, prevLayer, prevErr = prevIter.Next()
			prev.lock.Unlock()

		case prevCmp > 0: // nextK < prevK
			// new nextK has been inserted apparently
			if nextLayer.Len() > 0 {
				// create a new layer to set into prev
				newLayer := secondary.NewLayer(prev.cmp2)

				// fills layer while calling the onAdd callback
				if onAdd != nil {
					newLayer.Sync(nextLayer, func(k2 string) {
						onAdd(nextK, k2)
					}, nil)
				} else {
					newLayer.Sync(nextLayer, nil, nil)
				}

				// set the new layer into prev
				prev.lock.Lock()
				prev.store.Set(nextK, newLayer)
				prev.lock.Unlock()
			}
			// move next iterator
			next.lock.Lock()
			nextK, nextLayer, nextErr = nextIter.Next()
			next.lock.Unlock()

		default:
			// we're on the same keys, sync the layers
			switch {
			case onAdd != nil && onDel != nil:
				prevLayer.Sync(nextLayer, func(k2 string) {
					onAdd(nextK, k2)
				}, func(k2 string) {
					onDel(prevK, k2)
				})
			case onAdd != nil:
				prevLayer.Sync(nextLayer, func(k2 string) {
					onAdd(nextK, k2)
				}, nil)
			case onDel != nil:
				prevLayer.Sync(nextLayer, nil, func(k2 string) {
					onDel(prevK, k2)
				})
			default: // no callbacks
				prevLayer.Sync(nextLayer, nil, nil)
			}

			// move both iterators
			prev.lock.Lock()
			prevK, prevLayer, prevErr = prevIter.Next()
			prev.lock.Unlock()
			next.lock.Lock()
			nextK, nextLayer, nextErr = nextIter.Next()
			next.lock.Unlock()
		}
	}
}

func (l Layer) Len() int {
	l.lock.Lock()
	count := l.store.Len()
	l.lock.Unlock()
	return count
}
