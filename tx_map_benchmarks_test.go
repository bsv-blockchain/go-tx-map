package txmap

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

const errMapShouldNotBeNil = "map should not be nil"

// memoryTestSize is the number of entries used for memory footprint measurement.
const memoryTestSize = 1_000_000

// testHashesCacheDir is the directory for cached hash fixtures.
const testHashesCacheDir = "testdata"

// maxHashesCacheSize is the maximum number of entries stored in the cache file.
// A single file holds this many hashes; smaller requests use the first N entries.
const maxHashesCacheSize = 1_000_000

// getTestHashes returns size chainhash.Hash values. It loads from a single cache file
// (testdata/hashes.bin) if it exists and has enough entries; otherwise generates
// up to maxHashesCacheSize entries and writes the cache file.
//
//nolint:gocognit // cache load/generate logic with multiple branches
func getTestHashes(size int) []chainhash.Hash {
	path := filepath.Join(testHashesCacheDir, "hashes.bin")
	if data, err := os.ReadFile(path); err == nil { //nolint:gosec // G304: path from constant, not user input
		n := len(data) / 32
		if n >= size {
			hashes := make([]chainhash.Hash, size)
			for i := 0; i < size; i++ {
				copy(hashes[i][:], data[i*32:(i+1)*32])
			}
			return hashes
		}
	}

	// Generate: if size > max, generate size; else generate max for cache
	genSize := size
	if size < maxHashesCacheSize {
		genSize = maxHashesCacheSize
	}
	hashes := make([]chainhash.Hash, genSize)
	for i := 0; i < genSize; i++ {
		hashes[i] = chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
	}

	if err := os.MkdirAll(testHashesCacheDir, 0o750); err == nil {
		data := make([]byte, genSize*32)
		for i := 0; i < genSize; i++ {
			copy(data[i*32:], hashes[i][:])
		}
		_ = os.WriteFile(path, data, 0o600)
	}

	// Return only the requested size (first size entries)
	if genSize > size {
		return hashes[:size]
	}
	return hashes
}

// TestMemoryConsumption measures heap memory used by each map implementation
// when populated with a static number of entries (memoryTestSize).
// Run with: go test -v -run TestMemoryConsumption
//
//nolint:gocognit,gocyclo // repetitive measure calls for each map type
func TestMemoryConsumption(t *testing.T) {
	hashes := getTestHashes(memoryTestSize)

	var keepAlive []interface{} // prevent GC of maps so heap only grows
	runtime.GC()
	var mstats runtime.MemStats
	runtime.ReadMemStats(&mstats)
	prevHeap := mstats.HeapAlloc

	measure := func(name string, populate func() interface{}) {
		m := populate()
		keepAlive = append(keepAlive, m)

		runtime.GC()
		runtime.ReadMemStats(&mstats)
		used := mstats.HeapAlloc - prevHeap
		if mstats.HeapAlloc < prevHeap {
			used = 0 // GC reclaimed more than we allocated; report 0
		}
		prevHeap = mstats.HeapAlloc

		t.Logf("%s: %s (%d bytes, %.2f MB)", name, formatBytes(used), used, float64(used)/(1024*1024))
	}

	// Map: chainhash.Hash key, no value
	t.Run("Map/dolthub", func(_ *testing.T) {
		measure("Map/dolthub", func() interface{} {
			m := NewSwissMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		})
	})
	t.Run("Map/native", func(_ *testing.T) {
		measure("Map/native", func() interface{} {
			m := NewNativeMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		})
	})
	// SplitMap
	t.Run("SplitMap/dolthub", func(_ *testing.T) {
		measure("SplitMap/dolthub", func() interface{} {
			m := NewSplitSwissMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
			}
			return m
		})
	})
	t.Run("SplitMap/native", func(_ *testing.T) {
		measure("SplitMap/native", func() interface{} {
			m := NewNativeSplitMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
			}
			return m
		})
	})
	// MapUint64
	t.Run("MapUint64/dolthub", func(_ *testing.T) {
		measure("MapUint64/dolthub", func() interface{} {
			m := NewSwissMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
			}
			return m
		})
	})
	t.Run("MapUint64/native", func(_ *testing.T) {
		measure("MapUint64/native", func() interface{} {
			m := NewNativeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
			}
			return m
		})
	})
	// SplitMapUint64
	t.Run("SplitMapUint64/dolthub", func(_ *testing.T) {
		measure("SplitMapUint64/dolthub", func() interface{} {
			m := NewSplitSwissMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
			}
			return m
		})
	})
	t.Run("SplitMapUint64/native", func(_ *testing.T) {
		measure("SplitMapUint64/native", func() interface{} {
			m := NewNativeSplitMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
			}
			return m
		})
	})
	// LockFreeMapUint64
	t.Run("LockFreeMapUint64/dolthub", func(_ *testing.T) {
		measure("LockFreeMapUint64/dolthub", func() interface{} {
			m := NewSwissLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, memoryTestSize]
			}
			return m
		})
	})
	t.Run("LockFreeMapUint64/native", func(_ *testing.T) {
		measure("LockFreeMapUint64/native", func() interface{} {
			m := NewNativeLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, memoryTestSize]
			}
			return m
		})
	})
	// SplitLockFreeMapUint64
	t.Run("SplitLockFreeMapUint64/dolthub", func(_ *testing.T) {
		measure("SplitLockFreeMapUint64/dolthub", func() interface{} {
			m := NewSplitSwissLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, memoryTestSize]
			}
			return m
		})
	})
	t.Run("SplitLockFreeMapUint64/native", func(_ *testing.T) {
		measure("SplitLockFreeMapUint64/native", func() interface{} {
			m := NewNativeSplitLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, memoryTestSize]
			}
			return m
		})
	})
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

// BenchmarkSwissMapPut measures Put performance for SwissMap.
// BenchmarkPut measures Put performance for all map types.
// Organized by map type, then by implementation (dolthub, native).
// Run with: go test -bench=BenchmarkPut -benchmem -benchtime=100000x
//
//nolint:gocognit,gocyclo // benchmark structure with many map type variants
func BenchmarkPut(b *testing.B) {
	const size = 100000
	hashes := getTestHashes(size)

	// Map: chainhash.Hash key, no value (existence only)
	b.Run("Map/dolthub", func(b *testing.B) {
		m := NewSwissMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size])
		}
	})
	b.Run("Map/native", func(b *testing.B) {
		m := NewNativeMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size])
		}
	})
	// SplitMap: chainhash.Hash key, uint64 value
	b.Run("SplitMap/dolthub", func(b *testing.B) {
		m := NewSplitSwissMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	b.Run("SplitMap/native", func(b *testing.B) {
		m := NewNativeSplitMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	// MapUint64: chainhash.Hash key, uint64 value
	b.Run("MapUint64/dolthub", func(b *testing.B) {
		m := NewSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	b.Run("MapUint64/native", func(b *testing.B) {
		m := NewNativeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	// SplitMapUint64: chainhash.Hash key, uint64 value
	b.Run("SplitMapUint64/dolthub", func(b *testing.B) {
		m := NewSplitSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	b.Run("SplitMapUint64/native", func(b *testing.B) {
		m := NewNativeSplitMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	// LockFreeMapUint64: uint64 key, uint64 value
	b.Run("LockFreeMapUint64/dolthub", func(b *testing.B) {
		m := NewSwissLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i%size), uint64(i)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	b.Run("LockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i%size), uint64(i)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	// SplitLockFreeMapUint64: uint64 key, uint64 value
	b.Run("SplitLockFreeMapUint64/dolthub", func(b *testing.B) {
		m := NewSplitSwissLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i%size), uint64(i)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	b.Run("SplitLockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeSplitLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i%size), uint64(i)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
}

// BenchmarkGet measures Get performance for all map types.
// Organized by map type, then by implementation (dolthub, native).
// Run with: go test -bench=BenchmarkGet -benchmem -benchtime=100000x
//
//nolint:gocognit,gocyclo // benchmark structure with many map type variants
func BenchmarkGet(b *testing.B) {
	const size = 100000
	hashes := getTestHashes(size)

	// Map: chainhash.Hash key, no value (existence only) - uses Exists instead of Get
	b.Run("Map/dolthub", func(b *testing.B) {
		m := NewSwissMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i])
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	b.Run("Map/native", func(b *testing.B) {
		m := NewNativeMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i])
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	// SplitMap: chainhash.Hash key, uint64 value
	b.Run("SplitMap/dolthub", func(b *testing.B) {
		m := NewSplitSwissMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(hashes[i%size])
		}
	})
	b.Run("SplitMap/native", func(b *testing.B) {
		m := NewNativeSplitMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(hashes[i%size])
		}
	})
	// MapUint64: chainhash.Hash key, uint64 value
	b.Run("MapUint64/dolthub", func(b *testing.B) {
		m := NewSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(hashes[i%size])
		}
	})
	b.Run("MapUint64/native", func(b *testing.B) {
		m := NewNativeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(hashes[i%size])
		}
	})
	// SplitMapUint64: chainhash.Hash key, uint64 value
	b.Run("SplitMapUint64/dolthub", func(b *testing.B) {
		m := NewSplitSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(hashes[i%size])
		}
	})
	b.Run("SplitMapUint64/native", func(b *testing.B) {
		m := NewNativeSplitMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(hashes[i%size])
		}
	})
	// LockFreeMapUint64: uint64 key, uint64 value
	b.Run("LockFreeMapUint64/dolthub", func(b *testing.B) {
		m := NewSwissLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(uint64(i % size)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	b.Run("LockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(uint64(i % size)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	// SplitLockFreeMapUint64: uint64 key, uint64 value
	b.Run("SplitLockFreeMapUint64/dolthub", func(b *testing.B) {
		m := NewSplitSwissLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(uint64(i % size)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	b.Run("SplitLockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeSplitLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(uint64(i % size)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
}

// BenchmarkExists measures Exists performance for all map types.
// Organized by map type, then by implementation (dolthub, native).
// Run with: go test -bench=BenchmarkExists -benchmem -benchtime=100000x
//
//nolint:gocognit,gocyclo // benchmark structure with many map type variants
func BenchmarkExists(b *testing.B) {
	const size = 100000
	hashes := getTestHashes(size)

	// Map: chainhash.Hash key, no value (existence only)
	b.Run("Map/dolthub", func(b *testing.B) {
		m := NewSwissMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i])
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	b.Run("Map/native", func(b *testing.B) {
		m := NewNativeMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i])
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	// SplitMap: chainhash.Hash key, uint64 value
	b.Run("SplitMap/dolthub", func(b *testing.B) {
		m := NewSplitSwissMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	b.Run("SplitMap/native", func(b *testing.B) {
		m := NewNativeSplitMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	// MapUint64: chainhash.Hash key, uint64 value
	b.Run("MapUint64/dolthub", func(b *testing.B) {
		m := NewSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	b.Run("MapUint64/native", func(b *testing.B) {
		m := NewNativeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	// SplitMapUint64: chainhash.Hash key, uint64 value
	b.Run("SplitMapUint64/dolthub", func(b *testing.B) {
		m := NewSplitSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	b.Run("SplitMapUint64/native", func(b *testing.B) {
		m := NewNativeSplitMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	// LockFreeMapUint64: uint64 key, uint64 value
	b.Run("LockFreeMapUint64/dolthub", func(b *testing.B) {
		m := NewSwissLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(uint64(i % size)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	b.Run("LockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(uint64(i % size)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	// SplitLockFreeMapUint64: uint64 key, uint64 value
	b.Run("SplitLockFreeMapUint64/dolthub", func(b *testing.B) {
		m := NewSplitSwissLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(uint64(i % size)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
	b.Run("SplitLockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeSplitLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(uint64(i % size)) //nolint:gosec // G115: i from benchmark loop, always >= 0
		}
	})
}

// BenchmarkDelete measures Delete performance for all map types that support it.
// LockFree map types do not have Delete method.
// Organized by map type, then by implementation (dolthub, native).
// Run with: go test -bench=BenchmarkDelete -benchmem -benchtime=100000x
//
//nolint:gocognit,gocyclo // benchmark structure with many map type variants
func BenchmarkDelete(b *testing.B) {
	const size = 100000
	hashes := getTestHashes(size)

	// Map: chainhash.Hash key, no value (existence only)
	b.Run("Map/dolthub", func(b *testing.B) {
		m := NewSwissMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i])
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
	b.Run("Map/native", func(b *testing.B) {
		m := NewNativeMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i])
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
	// SplitMap: chainhash.Hash key, uint64 value
	b.Run("SplitMap/dolthub", func(b *testing.B) {
		m := NewSplitSwissMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
	b.Run("SplitMap/native", func(b *testing.B) {
		m := NewNativeSplitMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
	// MapUint64: chainhash.Hash key, uint64 value
	b.Run("MapUint64/dolthub", func(b *testing.B) {
		m := NewSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
	b.Run("MapUint64/native", func(b *testing.B) {
		m := NewNativeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
	// SplitMapUint64: chainhash.Hash key, uint64 value
	b.Run("SplitMapUint64/dolthub", func(b *testing.B) {
		m := NewSplitSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
	b.Run("SplitMapUint64/native", func(b *testing.B) {
		m := NewNativeSplitMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
}

// BenchmarkSwissMapUint64Iter measures Iter performance for SwissMapUint64.
func BenchmarkSwissMapUint64Iter(b *testing.B) {
	const size = 10000
	hashes := getTestHashes(size)

	b.Run("dolthub", func(b *testing.B) {
		m := NewSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			m.Iter(func(_ chainhash.Hash, _ uint64) bool {
				return false // continue
			})
		}
	})

	b.Run("native", func(b *testing.B) {
		m := NewNativeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			m.Iter(func(_ chainhash.Hash, _ uint64) bool {
				return false // continue
			})
		}
	})
}

// BenchmarkSwissMapUint64Delete measures Delete performance for SwissMapUint64.
//
//nolint:gocognit // benchmark with dolthub/native variants
func BenchmarkSwissMapUint64Delete(b *testing.B) {
	const size = 10000
	hashes := getTestHashes(size)

	b.Run("dolthub", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			m := NewSwissMapUint64(size)
			for j := 0; j < size; j++ {
				_ = m.Put(hashes[j], uint64(j)) //nolint:gosec // G115: j in range [0, size]
			}
			b.StartTimer()

			for j := 0; j < size; j++ {
				_ = m.Delete(hashes[j])
			}
		}
	})

	b.Run("native", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			m := NewNativeMapUint64(size)
			for j := 0; j < size; j++ {
				_ = m.Put(hashes[j], uint64(j)) //nolint:gosec // G115: j in range [0, size]
			}
			b.StartTimer()

			for j := 0; j < size; j++ {
				_ = m.Delete(hashes[j])
			}
		}
	})
}

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
	b.Run("dolthub", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewSplitSwissLockFreeMapUint64(1000) == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})

	b.Run("native", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewNativeSplitLockFreeMapUint64(1000) == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})
}

// BenchmarkNewSplitSwissMap measures constructing a SplitSwissMap.
func BenchmarkNewSplitSwissMap(b *testing.B) {
	b.Run("dolthub", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewSplitSwissMap(1000) == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})

	b.Run("native", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewNativeSplitMap(1000) == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})
}

// BenchmarkNewSplitSwissMapUint64 measures constructing a SplitSwissMapUint64.
func BenchmarkNewSplitSwissMapUint64(b *testing.B) {
	b.Run("dolthub", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewSplitSwissMapUint64(1000) == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})

	b.Run("native", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewNativeSplitMapUint64(1000) == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})
}

// BenchmarkNewSwissLockFreeMapUint64 measures constructing a SwissLockFreeMapUint64.
func BenchmarkNewSwissLockFreeMapUint64(b *testing.B) {
	b.Run("dolthub", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewSwissLockFreeMapUint64(1000) == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})

	b.Run("native", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewNativeLockFreeMapUint64(1000) == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})
}

// BenchmarkNewSwissMap measures constructing a SwissMap.
func BenchmarkNewSwissMap(b *testing.B) {
	b.Run("dolthub", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewSwissMap(1000) == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})

	b.Run("native", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewNativeMap(1000) == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})
}

// BenchmarkNewSwissMapUint64 measures constructing a SwissMapUint64.
func BenchmarkNewSwissMapUint64(b *testing.B) {
	b.Run("dolthub", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewSwissMapUint64(1000) == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})

	b.Run("native", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewNativeMapUint64(1000) == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})
}
