package txmap

import (
	"sync"
	"testing"
)

// BenchmarkConvertSyncMapToUint32Slice measures the performance of converting
// a sync.Map to a slice of uint32 values.
func BenchmarkConvertSyncMapToUint32Slice(b *testing.B) {
	var sm sync.Map
	for i := 0; i < 1000; i++ {
		sm.Store(uint32(i), struct{}{}) //nolint:gosec // safe cast, i < 1000
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, ok := ConvertSyncMapToUint32Slice(&sm); !ok {
			b.Fatal("map should contain elements")
		}
	}
}

// BenchmarkConvertSyncedMapToUint32Slice measures the performance of converting
// a SyncedMap to a slice of uint32 values.
func BenchmarkConvertSyncedMapToUint32Slice(b *testing.B) {
	sm := NewSyncedMap[int, []uint32]()
	for i := 0; i < 1000; i++ {
		sm.Set(i, []uint32{uint32(i)}) //nolint:gosec // safe cast, i < 1000
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, ok := ConvertSyncedMapToUint32Slice[int](sm); !ok {
			b.Fatal("map should contain elements")
		}
	}
}
