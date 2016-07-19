package common

type Comparable interface {
	Less(other Comparable) bool
}

type FinalizableComparable interface {
	Comparable

	Finalize()
	AddFinalizer(fn func()) bool
}
