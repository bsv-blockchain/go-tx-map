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

// memoryCase holds a test name and populate function for memory consumption tests.
type memoryCase struct {
	name     string
	populate func() interface{}
}

// TestMemoryConsumption measures heap memory used by each map implementation
// when populated with a static number of entries (memoryTestSize).
// Run with: go test -v -run TestMemoryConsumption
//
//nolint:gocognit,gocyclo // table-driven with many map type variants
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

	cases := []memoryCase{
		{"Map/dolthub", func() interface{} {
			m := NewSwissMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		}},
		{"Map/native", func() interface{} {
			m := NewNativeMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		}},
		{"SplitMap/dolthub", func() interface{} {
			m := NewSplitSwissMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
			}
			return m
		}},
		{"SplitMap/native", func() interface{} {
			m := NewNativeSplitMap(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
			}
			return m
		}},
		{"MapUint64/dolthub", func() interface{} {
			m := NewSwissMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
			}
			return m
		}},
		{"MapUint64/native", func() interface{} {
			m := NewNativeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
			}
			return m
		}},
		{"SplitMapUint64/dolthub", func() interface{} {
			m := NewSplitSwissMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
			}
			return m
		}},
		{"SplitMapUint64/native", func() interface{} {
			m := NewNativeSplitMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i is in range [0, memoryTestSize]
			}
			return m
		}},
		{"LockFreeMapUint64/dolthub", func() interface{} {
			m := NewSwissLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, memoryTestSize]
			}
			return m
		}},
		{"LockFreeMapUint64/native", func() interface{} {
			m := NewNativeLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, memoryTestSize]
			}
			return m
		}},
		{"SplitLockFreeMapUint64/dolthub", func() interface{} {
			m := NewSplitSwissLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, memoryTestSize]
			}
			return m
		}},
		{"SplitLockFreeMapUint64/native", func() interface{} {
			m := NewNativeSplitLockFreeMapUint64(memoryTestSize)
			for i := 0; i < memoryTestSize; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, memoryTestSize]
			}
			return m
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(_ *testing.T) {
			measure(c.name, c.populate)
		})
	}
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

// benchCase holds a benchmark name and its run function for table-driven benchmarks.
type benchCase struct {
	name string
	run  func(b *testing.B)
}

// runBenchCases executes each benchmark case under b.
func runBenchCases(b *testing.B, cases []benchCase) {
	for _, c := range cases {
		b.Run(c.name, c.run)
	}
}

// putBenchCase builds a benchCase for Put benchmarks: create map, then run put loop.
func putBenchCase(name string, setup func() interface{}, putOne func(m interface{}, i int)) benchCase {
	return benchCase{name, func(b *testing.B) {
		m := setup()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			putOne(m, i)
		}
	}}
}

// populateBenchCase builds a benchCase for Get/Exists/Delete: create+populate map, then run loop.
func populateBenchCase(name string, setup func() interface{}, runOne func(m interface{}, i int)) benchCase {
	return benchCase{name, func(b *testing.B) {
		m := setup()
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			runOne(m, i)
		}
	}}
}

// runNewBench runs a constructor benchmark for dolthub and native variants.
func runNewBench(b *testing.B, newDolthub, newNative func() interface{}) {
	b.Run("dolthub", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if newDolthub() == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})
	b.Run("native", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if newNative() == nil {
				b.Fatal(errMapShouldNotBeNil)
			}
		}
	})
}

// BenchmarkPut measures Put performance for all map types.
// Run with: go test -bench=BenchmarkPut -benchmem -benchtime=100000x
func BenchmarkPut(b *testing.B) {
	const size = 100000
	hashes := getTestHashes(size)

	cases := []benchCase{
		putBenchCase("Map/dolthub", func() interface{} { return NewSwissMap(size) }, func(m interface{}, i int) {
			_ = m.(*SwissMap).Put(hashes[i%size])
		}),
		putBenchCase("Map/native", func() interface{} { return NewNativeMap(size) }, func(m interface{}, i int) {
			_ = m.(*NativeMap).Put(hashes[i%size])
		}),
		putBenchCase("SplitMap/dolthub", func() interface{} { return NewSplitSwissMap(size) }, func(m interface{}, i int) {
			_ = m.(*SplitSwissMap).Put(hashes[i%size], uint64(i)) //nolint:gosec // G115: i from benchmark loop
		}),
		putBenchCase("SplitMap/native", func() interface{} { return NewNativeSplitMap(size) }, func(m interface{}, i int) {
			_ = m.(*NativeSplitMap).Put(hashes[i%size], uint64(i)) //nolint:gosec // G115: i from benchmark loop
		}),
		putBenchCase("MapUint64/dolthub", func() interface{} { return NewSwissMapUint64(size) }, func(m interface{}, i int) {
			_ = m.(*SwissMapUint64).Put(hashes[i%size], uint64(i)) //nolint:gosec // G115: i from benchmark loop
		}),
		putBenchCase("MapUint64/native", func() interface{} { return NewNativeMapUint64(size) }, func(m interface{}, i int) {
			_ = m.(*NativeMapUint64).Put(hashes[i%size], uint64(i)) //nolint:gosec // G115: i from benchmark loop
		}),
		putBenchCase("SplitMapUint64/dolthub", func() interface{} { return NewSplitSwissMapUint64(size) }, func(m interface{}, i int) {
			_ = m.(*SplitSwissMapUint64).Put(hashes[i%size], uint64(i)) //nolint:gosec // G115: i from benchmark loop
		}),
		putBenchCase("SplitMapUint64/native", func() interface{} { return NewNativeSplitMapUint64(size) }, func(m interface{}, i int) {
			_ = m.(*NativeSplitMapUint64).Put(hashes[i%size], uint64(i)) //nolint:gosec // G115: i from benchmark loop
		}),
		putBenchCase("LockFreeMapUint64/dolthub", func() interface{} { return NewSwissLockFreeMapUint64(size) }, func(m interface{}, i int) {
			_ = m.(*SwissLockFreeMapUint64).Put(uint64(i%size), uint64(i)) //nolint:gosec // G115: i from benchmark loop
		}),
		putBenchCase("LockFreeMapUint64/native", func() interface{} { return NewNativeLockFreeMapUint64(size) }, func(m interface{}, i int) {
			_ = m.(*NativeLockFreeMapUint64).Put(uint64(i%size), uint64(i)) //nolint:gosec // G115: i from benchmark loop
		}),
		putBenchCase("SplitLockFreeMapUint64/dolthub", func() interface{} { return NewSplitSwissLockFreeMapUint64(size) }, func(m interface{}, i int) {
			_ = m.(*SplitSwissLockFreeMapUint64).Put(uint64(i%size), uint64(i)) //nolint:gosec // G115: i from benchmark loop
		}),
		putBenchCase("SplitLockFreeMapUint64/native", func() interface{} { return NewNativeSplitLockFreeMapUint64(size) }, func(m interface{}, i int) {
			_ = m.(*NativeSplitLockFreeMapUint64).Put(uint64(i%size), uint64(i)) //nolint:gosec // G115: i from benchmark loop
		}),
	}
	runBenchCases(b, cases)
}

// BenchmarkGet measures Get performance for all map types.
// Run with: go test -bench=BenchmarkGet -benchmem -benchtime=100000x
//
//nolint:gocognit,gocyclo // table-driven with many map type variants
func BenchmarkGet(b *testing.B) {
	const size = 100000
	hashes := getTestHashes(size)

	cases := []benchCase{
		populateBenchCase("Map/dolthub", func() interface{} {
			m := NewSwissMap(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		}, func(m interface{}, i int) {
			_ = m.(*SwissMap).Exists(hashes[i%size])
		}),
		populateBenchCase("Map/native", func() interface{} {
			m := NewNativeMap(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		}, func(m interface{}, i int) {
			_ = m.(*NativeMap).Exists(hashes[i%size])
		}),
		populateBenchCase("SplitMap/dolthub", func() interface{} {
			m := NewSplitSwissMap(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, func(m interface{}, i int) {
			_, _ = m.(*SplitSwissMap).Get(hashes[i%size])
		}),
		populateBenchCase("SplitMap/native", func() interface{} {
			m := NewNativeSplitMap(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, func(m interface{}, i int) {
			_, _ = m.(*NativeSplitMap).Get(hashes[i%size])
		}),
		populateBenchCase("MapUint64/dolthub", func() interface{} {
			m := NewSwissMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, func(m interface{}, i int) {
			_, _ = m.(*SwissMapUint64).Get(hashes[i%size])
		}),
		populateBenchCase("MapUint64/native", func() interface{} {
			m := NewNativeMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, func(m interface{}, i int) {
			_, _ = m.(*NativeMapUint64).Get(hashes[i%size])
		}),
		populateBenchCase("SplitMapUint64/dolthub", func() interface{} {
			m := NewSplitSwissMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, func(m interface{}, i int) {
			_, _ = m.(*SplitSwissMapUint64).Get(hashes[i%size])
		}),
		populateBenchCase("SplitMapUint64/native", func() interface{} {
			m := NewNativeSplitMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, func(m interface{}, i int) {
			_, _ = m.(*NativeSplitMapUint64).Get(hashes[i%size])
		}),
		populateBenchCase("LockFreeMapUint64/dolthub", func() interface{} {
			m := NewSwissLockFreeMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, func(m interface{}, i int) {
			_, _ = m.(*SwissLockFreeMapUint64).Get(uint64(i % size)) //nolint:gosec // G115: i from benchmark loop
		}),
		populateBenchCase("LockFreeMapUint64/native", func() interface{} {
			m := NewNativeLockFreeMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, func(m interface{}, i int) {
			_, _ = m.(*NativeLockFreeMapUint64).Get(uint64(i % size)) //nolint:gosec // G115: i from benchmark loop
		}),
		populateBenchCase("SplitLockFreeMapUint64/dolthub", func() interface{} {
			m := NewSplitSwissLockFreeMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, func(m interface{}, i int) {
			_, _ = m.(*SplitSwissLockFreeMapUint64).Get(uint64(i % size)) //nolint:gosec // G115: i from benchmark loop
		}),
		populateBenchCase("SplitLockFreeMapUint64/native", func() interface{} {
			m := NewNativeSplitLockFreeMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, func(m interface{}, i int) {
			_, _ = m.(*NativeSplitLockFreeMapUint64).Get(uint64(i % size)) //nolint:gosec // G115: i from benchmark loop
		}),
	}
	runBenchCases(b, cases)
}

// BenchmarkExists measures Exists performance for all map types.
// Run with: go test -bench=BenchmarkExists -benchmem -benchtime=100000x
//
//nolint:gocognit,gocyclo // table-driven with many map type variants
func BenchmarkExists(b *testing.B) {
	const size = 100000
	hashes := getTestHashes(size)

	type existsHashT interface {
		Exists(hash chainhash.Hash) bool
	}
	type existsUint64T interface{ Exists(k uint64) bool }
	existsHash := func(m interface{}, i int) { _ = m.(existsHashT).Exists(hashes[i%size]) }
	existsUint64 := func(m interface{}, i int) { _ = m.(existsUint64T).Exists(uint64(i % size)) } //nolint:gosec // G115: i from benchmark loop

	cases := []benchCase{
		populateBenchCase("Map/dolthub", func() interface{} {
			m := NewSwissMap(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		}, existsHash),
		populateBenchCase("Map/native", func() interface{} {
			m := NewNativeMap(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		}, existsHash),
		populateBenchCase("SplitMap/dolthub", func() interface{} {
			m := NewSplitSwissMap(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, existsHash),
		populateBenchCase("SplitMap/native", func() interface{} {
			m := NewNativeSplitMap(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, existsHash),
		populateBenchCase("MapUint64/dolthub", func() interface{} {
			m := NewSwissMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, existsHash),
		populateBenchCase("MapUint64/native", func() interface{} {
			m := NewNativeMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, existsHash),
		populateBenchCase("SplitMapUint64/dolthub", func() interface{} {
			m := NewSplitSwissMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, existsHash),
		populateBenchCase("SplitMapUint64/native", func() interface{} {
			m := NewNativeSplitMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, existsHash),
		populateBenchCase("LockFreeMapUint64/dolthub", func() interface{} {
			m := NewSwissLockFreeMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, existsUint64),
		populateBenchCase("LockFreeMapUint64/native", func() interface{} {
			m := NewNativeLockFreeMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, existsUint64),
		populateBenchCase("SplitLockFreeMapUint64/dolthub", func() interface{} {
			m := NewSplitSwissLockFreeMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, existsUint64),
		populateBenchCase("SplitLockFreeMapUint64/native", func() interface{} {
			m := NewNativeSplitLockFreeMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(uint64(i), uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, existsUint64),
	}
	runBenchCases(b, cases)
}

// BenchmarkDelete measures Delete performance for all map types that support it.
// LockFree map types do not have Delete method.
// Run with: go test -bench=BenchmarkDelete -benchmem -benchtime=100000x
//
//nolint:gocognit // table-driven with many map type variants
func BenchmarkDelete(b *testing.B) {
	const size = 100000
	hashes := getTestHashes(size)

	type deleteHashT interface {
		Delete(hash chainhash.Hash) error
	}
	deleteHash := func(m interface{}, i int) { _ = m.(deleteHashT).Delete(hashes[i%size]) }

	cases := []benchCase{
		populateBenchCase("Map/dolthub", func() interface{} {
			m := NewSwissMap(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		}, deleteHash),
		populateBenchCase("Map/native", func() interface{} {
			m := NewNativeMap(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i])
			}
			return m
		}, deleteHash),
		populateBenchCase("SplitMap/dolthub", func() interface{} {
			m := NewSplitSwissMap(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, deleteHash),
		populateBenchCase("SplitMap/native", func() interface{} {
			m := NewNativeSplitMap(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, deleteHash),
		populateBenchCase("MapUint64/dolthub", func() interface{} {
			m := NewSwissMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, deleteHash),
		populateBenchCase("MapUint64/native", func() interface{} {
			m := NewNativeMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, deleteHash),
		populateBenchCase("SplitMapUint64/dolthub", func() interface{} {
			m := NewSplitSwissMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, deleteHash),
		populateBenchCase("SplitMapUint64/native", func() interface{} {
			m := NewNativeSplitMapUint64(size)
			for i := 0; i < size; i++ {
				_ = m.Put(hashes[i], uint64(i)) //nolint:gosec // G115: i in range [0, size]
			}
			return m
		}, deleteHash),
	}
	runBenchCases(b, cases)
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

// BenchmarkNewSplitSwissLockFreeMapUint64 measures constructing a SplitSwissLockFreeMapUint64.
func BenchmarkNewSplitSwissLockFreeMapUint64(b *testing.B) {
	runNewBench(b, func() interface{} { return NewSplitSwissLockFreeMapUint64(1000) }, func() interface{} { return NewNativeSplitLockFreeMapUint64(1000) })
}

// BenchmarkNewSplitSwissMap measures constructing a SplitSwissMap.
func BenchmarkNewSplitSwissMap(b *testing.B) {
	runNewBench(b, func() interface{} { return NewSplitSwissMap(1000) }, func() interface{} { return NewNativeSplitMap(1000) })
}

// BenchmarkNewSplitSwissMapUint64 measures constructing a SplitSwissMapUint64.
func BenchmarkNewSplitSwissMapUint64(b *testing.B) {
	runNewBench(b, func() interface{} { return NewSplitSwissMapUint64(1000) }, func() interface{} { return NewNativeSplitMapUint64(1000) })
}

// BenchmarkNewSwissLockFreeMapUint64 measures constructing a SwissLockFreeMapUint64.
func BenchmarkNewSwissLockFreeMapUint64(b *testing.B) {
	runNewBench(b, func() interface{} { return NewSwissLockFreeMapUint64(1000) }, func() interface{} { return NewNativeLockFreeMapUint64(1000) })
}

// BenchmarkNewSwissMap measures constructing a SwissMap.
func BenchmarkNewSwissMap(b *testing.B) {
	runNewBench(b, func() interface{} { return NewSwissMap(1000) }, func() interface{} { return NewNativeMap(1000) })
}

// BenchmarkNewSwissMapUint64 measures constructing a SwissMapUint64.
func BenchmarkNewSwissMapUint64(b *testing.B) {
	runNewBench(b, func() interface{} { return NewSwissMapUint64(1000) }, func() interface{} { return NewNativeMapUint64(1000) })
}
