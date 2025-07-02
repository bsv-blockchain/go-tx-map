package txmap

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// BenchmarkBytes2Uint16Buckets measures the performance of Bytes2Uint16Buckets.
func BenchmarkBytes2Uint16Buckets(b *testing.B) {
	hash := chainhash.Hash{0x01, 0x02}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_ = Bytes2Uint16Buckets(hash, 1024)
	}
}

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

// BenchmarkNewSplitSwissLockFreeMapUint64 measures constructing a
// SplitSwissLockFreeMapUint64.
func BenchmarkNewSplitSwissLockFreeMapUint64(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if NewSplitSwissLockFreeMapUint64(1000) == nil {
			b.Fatal("map should not be nil")
		}
	}
}

// BenchmarkNewSplitSwissMap measures constructing a SplitSwissMap.
func BenchmarkNewSplitSwissMap(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if NewSplitSwissMap(1000) == nil {
			b.Fatal("map should not be nil")
		}
	}
}

// BenchmarkNewSplitSwissMapUint64 measures constructing a SplitSwissMapUint64.
func BenchmarkNewSplitSwissMapUint64(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if NewSplitSwissMapUint64(1000) == nil {
			b.Fatal("map should not be nil")
		}
	}
}

// BenchmarkNewSwissLockFreeMapUint64 measures constructing a SwissLockFreeMapUint64.
func BenchmarkNewSwissLockFreeMapUint64(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if NewSwissLockFreeMapUint64(1000) == nil {
			b.Fatal("map should not be nil")
		}
	}
}

// BenchmarkNewSwissMap measures constructing a SwissMap.
func BenchmarkNewSwissMap(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if NewSwissMap(1000) == nil {
			b.Fatal("map should not be nil")
		}
	}
}

// BenchmarkNewSwissMapUint64 measures constructing a SwissMapUint64.
func BenchmarkNewSwissMapUint64(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if NewSwissMapUint64(1000) == nil {
			b.Fatal("map should not be nil")
		}
	}
}
