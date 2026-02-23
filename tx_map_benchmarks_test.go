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
const memoryTestSize = 100_000_000

// testHashesCacheDir is the directory for cached hash fixtures.
const testHashesCacheDir = "testdata"

// maxHashesCacheSize is the maximum number of entries stored in the cache file.
// A single file holds this many hashes; smaller requests use the first N entries.
const maxHashesCacheSize = 100_000_000

// getTestHashes returns size chainhash.Hash values. It loads from a single cache file
// (testdata/hashes.bin) if it exists and has enough entries; otherwise generates
// up to maxHashesCacheSize entries and writes the cache file.
func getTestHashes(size int) []chainhash.Hash {
	path := filepath.Join(testHashesCacheDir, "hashes.bin")
	if data, err := os.ReadFile(path); err == nil {
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

	if err := os.MkdirAll(testHashesCacheDir, 0755); err == nil {
		data := make([]byte, genSize*32)
		for i := 0; i < genSize; i++ {
			copy(data[i*32:], hashes[i][:])
		}
		_ = os.WriteFile(path, data, 0644)
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
	t.Run("Map/dolthub", func(t *testing.T) {
		measure("Map/dolthub", func() interface{} {
			m := NewSwissMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		})
	})
	t.Run("Map/cockroachdb", func(t *testing.T) {
		measure("Map/cockroachdb", func() interface{} {
			m := NewCRSwissMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		})
	})
	t.Run("Map/native", func(t *testing.T) {
		measure("Map/native", func() interface{} {
			m := NewNativeMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		})
	})
	t.Run("Map/tidwall", func(t *testing.T) {
		measure("Map/tidwall", func() interface{} {
			m := NewTidwallMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		})
	})

	// SplitMap
	t.Run("SplitMap/dolthub", func(t *testing.T) {
		measure("SplitMap/dolthub", func() interface{} {
			m := NewSplitSwissMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i))
			}
			return m
		})
	})
	t.Run("SplitMap/cockroachdb", func(t *testing.T) {
		measure("SplitMap/cockroachdb", func() interface{} {
			m := NewCRSplitSwissMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i))
			}
			return m
		})
	})
	t.Run("SplitMap/native", func(t *testing.T) {
		measure("SplitMap/native", func() interface{} {
			m := NewNativeSplitMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i))
			}
			return m
		})
	})
	t.Run("SplitMap/tidwall", func(t *testing.T) {
		measure("SplitMap/tidwall", func() interface{} {
			m := NewTidwallSplitMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i))
			}
			return m
		})
	})

	// MapUint64
	t.Run("MapUint64/dolthub", func(t *testing.T) {
		measure("MapUint64/dolthub", func() interface{} {
			m := NewSwissMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i))
			}
			return m
		})
	})
	t.Run("MapUint64/cockroachdb", func(t *testing.T) {
		measure("MapUint64/cockroachdb", func() interface{} {
			m := NewCRSwissMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i))
			}
			return m
		})
	})
	t.Run("MapUint64/native", func(t *testing.T) {
		measure("MapUint64/native", func() interface{} {
			m := NewNativeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i))
			}
			return m
		})
	})
	t.Run("MapUint64/tidwall", func(t *testing.T) {
		measure("MapUint64/tidwall", func() interface{} {
			m := NewTidwallMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i))
			}
			return m
		})
	})

	// SplitMapUint64
	t.Run("SplitMapUint64/dolthub", func(t *testing.T) {
		measure("SplitMapUint64/dolthub", func() interface{} {
			m := NewSplitSwissMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i))
			}
			return m
		})
	})
	t.Run("SplitMapUint64/cockroachdb", func(t *testing.T) {
		measure("SplitMapUint64/cockroachdb", func() interface{} {
			m := NewCRSplitSwissMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i))
			}
			return m
		})
	})
	t.Run("SplitMapUint64/native", func(t *testing.T) {
		measure("SplitMapUint64/native", func() interface{} {
			m := NewNativeSplitMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i))
			}
			return m
		})
	})
	t.Run("SplitMapUint64/tidwall", func(t *testing.T) {
		measure("SplitMapUint64/tidwall", func() interface{} {
			m := NewTidwallSplitMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i))
			}
			return m
		})
	})

	// LockFreeMapUint64
	t.Run("LockFreeMapUint64/dolthub", func(t *testing.T) {
		measure("LockFreeMapUint64/dolthub", func() interface{} {
			m := NewSwissLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i))
			}
			return m
		})
	})
	t.Run("LockFreeMapUint64/cockroachdb", func(t *testing.T) {
		measure("LockFreeMapUint64/cockroachdb", func() interface{} {
			m := NewCRSwissLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i))
			}
			return m
		})
	})
	t.Run("LockFreeMapUint64/native", func(t *testing.T) {
		measure("LockFreeMapUint64/native", func() interface{} {
			m := NewNativeLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i))
			}
			return m
		})
	})
	t.Run("LockFreeMapUint64/tidwall", func(t *testing.T) {
		measure("LockFreeMapUint64/tidwall", func() interface{} {
			m := NewTidwallLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i))
			}
			return m
		})
	})

	// SplitLockFreeMapUint64
	t.Run("SplitLockFreeMapUint64/dolthub", func(t *testing.T) {
		measure("SplitLockFreeMapUint64/dolthub", func() interface{} {
			m := NewSplitSwissLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i))
			}
			return m
		})
	})
	t.Run("SplitLockFreeMapUint64/cockroachdb", func(t *testing.T) {
		measure("SplitLockFreeMapUint64/cockroachdb", func() interface{} {
			m := NewCRSplitSwissLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i))
			}
			return m
		})
	})
	t.Run("SplitLockFreeMapUint64/native", func(t *testing.T) {
		measure("SplitLockFreeMapUint64/native", func() interface{} {
			m := NewNativeSplitLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i))
			}
			return m
		})
	})
	t.Run("SplitLockFreeMapUint64/tidwall", func(t *testing.T) {
		measure("SplitLockFreeMapUint64/tidwall", func() interface{} {
			m := NewTidwallSplitLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i))
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
// Organized by map type, then by implementation (dolthub, cockroachdb, native, tidwall).
// Run with: go test -bench=BenchmarkPut -benchmem -benchtime=100000x
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
	b.Run("Map/cockroachdb", func(b *testing.B) {
		m := NewCRSwissMap(size)
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
	b.Run("Map/tidwall", func(b *testing.B) {
		m := NewTidwallMap(size)
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
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
	b.Run("SplitMap/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
	b.Run("SplitMap/native", func(b *testing.B) {
		m := NewNativeSplitMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
	b.Run("SplitMap/tidwall", func(b *testing.B) {
		m := NewTidwallSplitMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})

	// MapUint64: chainhash.Hash key, uint64 value
	b.Run("MapUint64/dolthub", func(b *testing.B) {
		m := NewSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
	b.Run("MapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
	b.Run("MapUint64/native", func(b *testing.B) {
		m := NewNativeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
	b.Run("MapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})

	// SplitMapUint64: chainhash.Hash key, uint64 value
	b.Run("SplitMapUint64/dolthub", func(b *testing.B) {
		m := NewSplitSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
	b.Run("SplitMapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
	b.Run("SplitMapUint64/native", func(b *testing.B) {
		m := NewNativeSplitMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
	b.Run("SplitMapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallSplitMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})

	// LockFreeMapUint64: uint64 key, uint64 value
	b.Run("LockFreeMapUint64/dolthub", func(b *testing.B) {
		m := NewSwissLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i%size), uint64(i))
		}
	})
	b.Run("LockFreeMapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSwissLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i%size), uint64(i))
		}
	})
	b.Run("LockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i%size), uint64(i))
		}
	})
	b.Run("LockFreeMapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i%size), uint64(i))
		}
	})

	// SplitLockFreeMapUint64: uint64 key, uint64 value
	b.Run("SplitLockFreeMapUint64/dolthub", func(b *testing.B) {
		m := NewSplitSwissLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i%size), uint64(i))
		}
	})
	b.Run("SplitLockFreeMapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i%size), uint64(i))
		}
	})
	b.Run("SplitLockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeSplitLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i%size), uint64(i))
		}
	})
	b.Run("SplitLockFreeMapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallSplitLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i%size), uint64(i))
		}
	})
}

// BenchmarkGet measures Get performance for all map types.
// Organized by map type, then by implementation (dolthub, cockroachdb, native, tidwall).
// Run with: go test -bench=BenchmarkGet -benchmem -benchtime=100000x
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
	b.Run("Map/cockroachdb", func(b *testing.B) {
		m := NewCRSwissMap(size)
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
	b.Run("Map/tidwall", func(b *testing.B) {
		m := NewTidwallMap(size)
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(hashes[i%size])
		}
	})
	b.Run("SplitMap/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(hashes[i%size])
		}
	})
	b.Run("SplitMap/tidwall", func(b *testing.B) {
		m := NewTidwallSplitMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(hashes[i%size])
		}
	})
	b.Run("MapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(hashes[i%size])
		}
	})
	b.Run("MapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(hashes[i%size])
		}
	})
	b.Run("SplitMapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(hashes[i%size])
		}
	})
	b.Run("SplitMapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallSplitMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(uint64(i % size))
		}
	})
	b.Run("LockFreeMapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSwissLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(uint64(i % size))
		}
	})
	b.Run("LockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(uint64(i % size))
		}
	})
	b.Run("LockFreeMapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(uint64(i % size))
		}
	})

	// SplitLockFreeMapUint64: uint64 key, uint64 value
	b.Run("SplitLockFreeMapUint64/dolthub", func(b *testing.B) {
		m := NewSplitSwissLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(uint64(i % size))
		}
	})
	b.Run("SplitLockFreeMapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(uint64(i % size))
		}
	})
	b.Run("SplitLockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeSplitLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(uint64(i % size))
		}
	})
	b.Run("SplitLockFreeMapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallSplitLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_, _ = m.Get(uint64(i % size))
		}
	})
}

// BenchmarkExists measures Exists performance for all map types.
// Organized by map type, then by implementation (dolthub, cockroachdb, native, tidwall).
// Run with: go test -bench=BenchmarkExists -benchmem -benchtime=100000x
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
	b.Run("Map/cockroachdb", func(b *testing.B) {
		m := NewCRSwissMap(size)
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
	b.Run("Map/tidwall", func(b *testing.B) {
		m := NewTidwallMap(size)
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	b.Run("SplitMap/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	b.Run("SplitMap/tidwall", func(b *testing.B) {
		m := NewTidwallSplitMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	b.Run("MapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	b.Run("MapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	b.Run("SplitMapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(hashes[i%size])
		}
	})
	b.Run("SplitMapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallSplitMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(uint64(i % size))
		}
	})
	b.Run("LockFreeMapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSwissLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(uint64(i % size))
		}
	})
	b.Run("LockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(uint64(i % size))
		}
	})
	b.Run("LockFreeMapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(uint64(i % size))
		}
	})

	// SplitLockFreeMapUint64: uint64 key, uint64 value
	b.Run("SplitLockFreeMapUint64/dolthub", func(b *testing.B) {
		m := NewSplitSwissLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(uint64(i % size))
		}
	})
	b.Run("SplitLockFreeMapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(uint64(i % size))
		}
	})
	b.Run("SplitLockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeSplitLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(uint64(i % size))
		}
	})
	b.Run("SplitLockFreeMapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallSplitLockFreeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Exists(uint64(i % size))
		}
	})
}

// BenchmarkDelete measures Delete performance for all map types that support it.
// Note: LockFree map types do not have Delete method.
// Organized by map type, then by implementation (dolthub, cockroachdb, native, tidwall).
// Run with: go test -bench=BenchmarkDelete -benchmem -benchtime=100000x
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
	b.Run("Map/cockroachdb", func(b *testing.B) {
		m := NewCRSwissMap(size)
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
	b.Run("Map/tidwall", func(b *testing.B) {
		m := NewTidwallMap(size)
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
	b.Run("SplitMap/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
	b.Run("SplitMap/tidwall", func(b *testing.B) {
		m := NewTidwallSplitMap(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
	b.Run("MapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
	b.Run("MapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
	b.Run("SplitMapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Delete(hashes[i%size])
		}
	})
	b.Run("SplitMapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallSplitMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
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
			_ = m.Put(hashes[i], uint64(i))
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			m.Iter(func(hash chainhash.Hash, value uint64) bool {
				return false // continue
			})
		}
	})

	b.Run("cockroachdb", func(b *testing.B) {
		m := NewCRSwissMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			m.Iter(func(hash chainhash.Hash, value uint64) bool {
				return false // continue
			})
		}
	})

	b.Run("native", func(b *testing.B) {
		m := NewNativeMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			m.Iter(func(hash chainhash.Hash, value uint64) bool {
				return false // continue
			})
		}
	})

	b.Run("tidwall", func(b *testing.B) {
		m := NewTidwallMapUint64(size)
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}

		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			m.Iter(func(hash chainhash.Hash, value uint64) bool {
				return false // continue
			})
		}
	})
}

// BenchmarkSwissMapUint64Delete measures Delete performance for SwissMapUint64.
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
				_ = m.Put(hashes[j], uint64(j))
			}
			b.StartTimer()

			for j := 0; j < size; j++ {
				_ = m.Delete(hashes[j])
			}
		}
	})

	b.Run("cockroachdb", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			m := NewCRSwissMapUint64(size)
			for j := 0; j < size; j++ {
				_ = m.Put(hashes[j], uint64(j))
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
				_ = m.Put(hashes[j], uint64(j))
			}
			b.StartTimer()

			for j := 0; j < size; j++ {
				_ = m.Delete(hashes[j])
			}
		}
	})

	b.Run("tidwall", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			b.StopTimer()
			m := NewTidwallMapUint64(size)
			for j := 0; j < size; j++ {
				_ = m.Put(hashes[j], uint64(j))
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

	b.Run("cockroachdb", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewCRSplitSwissLockFreeMapUint64(1000) == nil {
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

	b.Run("tidwall", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewTidwallSplitLockFreeMapUint64(1000) == nil {
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

	b.Run("cockroachdb", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewCRSplitSwissMap(1000) == nil {
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

	b.Run("tidwall", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewTidwallSplitMap(1000) == nil {
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

	b.Run("cockroachdb", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewCRSplitSwissMapUint64(1000) == nil {
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

	b.Run("tidwall", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewTidwallSplitMapUint64(1000) == nil {
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

	b.Run("cockroachdb", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewCRSwissLockFreeMapUint64(1000) == nil {
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

	b.Run("tidwall", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewTidwallLockFreeMapUint64(1000) == nil {
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

	b.Run("cockroachdb", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewCRSwissMap(1000) == nil {
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

	b.Run("tidwall", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewTidwallMap(1000) == nil {
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

	b.Run("cockroachdb", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewCRSwissMapUint64(1000) == nil {
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

	b.Run("tidwall", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if NewTidwallMapUint64(1000) == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})
}
