package lockie

import (
	"sync"
	"testing"
)

var (
	mux = &TestItem{
		mux: &sync.Mutex{},
		m:   make(map[string]int),
	}

	rwmux = &TestItem{
		mux: &sync.RWMutex{},
		m:   make(map[string]int),
	}

	lkie = &TestItem{
		mux: NewLockie(),
		m:   make(map[string]int),
	}
)

type Locker interface {
	Lock()
	Unlock()
}

type TestItem struct {
	mux Locker
	m   map[string]int
}

type RWMuxTestItem struct {
	mux sync.RWMutex
	m   map[string]int
}

func (t *TestItem) Get(k string) (v int) {
	t.mux.Lock()
	v = t.m[k]
	t.mux.Unlock()
	return v
}

func (t *TestItem) Put(k string, v int) {
	t.mux.Lock()
	t.m[k] = v
	t.mux.Unlock()
}

func (t *RWMuxTestItem) Get(k string) (v int) {
	t.mux.RLock()
	v = t.m[k]
	t.mux.RUnlock()
	return v
}

func (t *RWMuxTestItem) Put(k string, v int) {
	t.mux.Lock()
	t.m[k] = v
	t.mux.Unlock()
}

type TestDB interface {
	Get(string) int
	Put(string, int)
}

func RBench(b *testing.B, db TestDB) {
	b.SetParallelism(4)
	b.RunParallel(func(p *testing.PB) {
		var v int
		for p.Next() {
			v = db.Get("hello")
		}

		if v == -1 {
			return
		}
	})

	b.ReportAllocs()
}

func WBench(b *testing.B, db TestDB) {
	b.SetParallelism(4)
	b.RunParallel(func(p *testing.PB) {
		for p.Next() {
			db.Put("hello", 46)
		}
	})

	b.ReportAllocs()
}

func RWBench(b *testing.B, db TestDB) {
	b.SetParallelism(4)
	b.RunParallel(func(p *testing.PB) {
		var v int
		for p.Next() {
			v = db.Get("hello")
			db.Put("hello", v)
		}
	})

	b.ReportAllocs()
}

func TestMain(t *testing.T) {
	mux.Put("hello", 46)
	rwmux.Put("hello", 46)
	lkie.Put("hello", 46)
}

func BenchmarkMuxR(b *testing.B) {
	RBench(b, mux)
}

func BenchmarkMuxW(b *testing.B) {
	WBench(b, mux)
}

func BenchmarkMuxRW(b *testing.B) {
	RWBench(b, mux)
}

func BenchmarkRWMuxR(b *testing.B) {
	RBench(b, rwmux)
}

func BenchmarkRWMuxW(b *testing.B) {
	WBench(b, rwmux)
}

func BenchmarkRWMuxRW(b *testing.B) {
	RWBench(b, rwmux)
}

func BenchmarkLockieR(b *testing.B) {
	RBench(b, lkie)
}

func BenchmarkLockieW(b *testing.B) {
	WBench(b, lkie)
}

func BenchmarkLockieRW(b *testing.B) {
	RWBench(b, lkie)
}
