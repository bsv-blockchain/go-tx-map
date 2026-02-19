package txmap

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

const errMapShouldNotBeNil = "map should not be nil"

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

// BenchmarkSwissMapPut measures Put performance for SwissMap.
// BenchmarkPut measures Put performance for all map types.
// Organized by map type, then by implementation (dolthub, cockroachdb, native, tidwall).
func BenchmarkPut(b *testing.B) {
	const size = 100000
	hashes := make([]chainhash.Hash, size)
	for i := 0; i < size; i++ {
		hashes[i] = chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
	}

	// Map: chainhash.Hash key, no value (existence only)
	b.Run("Map/dolthub", func(b *testing.B) {
		m := NewSwissMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i])
		}
	})
	b.Run("Map/cockroachdb", func(b *testing.B) {
		m := NewCRSwissMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i])
		}
	})
	b.Run("Map/native", func(b *testing.B) {
		m := NewNativeMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i])
		}
	})
	b.Run("Map/tidwall", func(b *testing.B) {
		m := NewTidwallMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i])
		}
	})

	// SplitMap: chainhash.Hash key, uint64 value
	b.Run("SplitMap/dolthub", func(b *testing.B) {
		m := NewSplitSwissMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}
	})
	b.Run("SplitMap/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}
	})
	b.Run("SplitMap/native", func(b *testing.B) {
		m := NewNativeSplitMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}
	})
	b.Run("SplitMap/tidwall", func(b *testing.B) {
		m := NewTidwallSplitMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}
	})

	// MapUint64: chainhash.Hash key, uint64 value
	b.Run("MapUint64/dolthub", func(b *testing.B) {
		m := NewSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}
	})
	b.Run("MapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}
	})
	b.Run("MapUint64/native", func(b *testing.B) {
		m := NewNativeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}
	})
	b.Run("MapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}
	})

	// SplitMapUint64: chainhash.Hash key, uint64 value
	b.Run("SplitMapUint64/dolthub", func(b *testing.B) {
		m := NewSplitSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}
	})
	b.Run("SplitMapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}
	})
	b.Run("SplitMapUint64/native", func(b *testing.B) {
		m := NewNativeSplitMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}
	})
	b.Run("SplitMapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallSplitMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(hashes[i], uint64(i))
		}
	})

	// LockFreeMapUint64: uint64 key, uint64 value
	b.Run("LockFreeMapUint64/dolthub", func(b *testing.B) {
		m := NewSwissLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})
	b.Run("LockFreeMapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSwissLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})
	b.Run("LockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})
	b.Run("LockFreeMapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})

	// SplitLockFreeMapUint64: uint64 key, uint64 value
	b.Run("SplitLockFreeMapUint64/dolthub", func(b *testing.B) {
		m := NewSplitSwissLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})
	b.Run("SplitLockFreeMapUint64/cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})
	b.Run("SplitLockFreeMapUint64/native", func(b *testing.B) {
		m := NewNativeSplitLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})
	b.Run("SplitLockFreeMapUint64/tidwall", func(b *testing.B) {
		m := NewTidwallSplitLockFreeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < size; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})
}

// =============================================================================
// LEGACY PUT BENCHMARKS (kept for backward compatibility)
// =============================================================================

// BenchmarkSwissMapPut measures Put performance for SwissMap (legacy name).
func BenchmarkSwissMapPut(b *testing.B) {
	const size = 100000
	hashes := make([]chainhash.Hash, size)
	for i := 0; i < size; i++ {
		hashes[i] = chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
	}

	b.Run("dolthub", func(b *testing.B) {
		m := NewSwissMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size])
		}
	})
	b.Run("cockroachdb", func(b *testing.B) {
		m := NewCRSwissMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size])
		}
	})
	b.Run("native", func(b *testing.B) {
		m := NewNativeMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size])
		}
	})
	b.Run("tidwall", func(b *testing.B) {
		m := NewTidwallMap(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size])
		}
	})
}

// BenchmarkSwissMapUint64Put measures Put performance for SwissMapUint64 (legacy name).
func BenchmarkSwissMapUint64Put(b *testing.B) {
	const size = 100000
	hashes := make([]chainhash.Hash, size)
	for i := 0; i < size; i++ {
		hashes[i] = chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
	}

	b.Run("dolthub", func(b *testing.B) {
		m := NewSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
	b.Run("cockroachdb", func(b *testing.B) {
		m := NewCRSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
	b.Run("native", func(b *testing.B) {
		m := NewNativeMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
	b.Run("tidwall", func(b *testing.B) {
		m := NewTidwallMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
}

// BenchmarkSwissMapUint64Get measures Get performance for SwissMapUint64.
func BenchmarkSwissMapUint64Get(b *testing.B) {
	const size = 10000
	hashes := make([]chainhash.Hash, size)
	for i := 0; i < size; i++ {
		hashes[i] = chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
	}

	b.Run("dolthub", func(b *testing.B) {
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

	b.Run("cockroachdb", func(b *testing.B) {
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

	b.Run("native", func(b *testing.B) {
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

	b.Run("tidwall", func(b *testing.B) {
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
}

// BenchmarkSwissMapUint64Exists measures Exists performance for SwissMapUint64.
func BenchmarkSwissMapUint64Exists(b *testing.B) {
	const size = 10000
	hashes := make([]chainhash.Hash, size)
	for i := 0; i < size; i++ {
		hashes[i] = chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
	}

	b.Run("dolthub", func(b *testing.B) {
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

	b.Run("cockroachdb", func(b *testing.B) {
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

	b.Run("native", func(b *testing.B) {
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

	b.Run("tidwall", func(b *testing.B) {
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
}

// BenchmarkSplitSwissMapUint64Put measures Put performance for SplitSwissMapUint64.
func BenchmarkSplitSwissMapUint64Put(b *testing.B) {
	const size = 100000
	hashes := make([]chainhash.Hash, size)
	for i := 0; i < size; i++ {
		hashes[i] = chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
	}

	b.Run("dolthub", func(b *testing.B) {
		m := NewSplitSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})

	b.Run("cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})

	b.Run("native", func(b *testing.B) {
		m := NewNativeSplitMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})

	b.Run("tidwall", func(b *testing.B) {
		m := NewTidwallSplitMapUint64(size)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = m.Put(hashes[i%size], uint64(i))
		}
	})
}

// BenchmarkSplitSwissMapUint64Get measures Get performance for SplitSwissMapUint64.
func BenchmarkSplitSwissMapUint64Get(b *testing.B) {
	const size = 10000
	hashes := make([]chainhash.Hash, size)
	for i := 0; i < size; i++ {
		hashes[i] = chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
	}

	b.Run("dolthub", func(b *testing.B) {
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

	b.Run("cockroachdb", func(b *testing.B) {
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

	b.Run("native", func(b *testing.B) {
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

	b.Run("tidwall", func(b *testing.B) {
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
}

// BenchmarkSwissLockFreeMapUint64Put measures Put performance for SwissLockFreeMapUint64.
func BenchmarkSwissLockFreeMapUint64Put(b *testing.B) {
	b.Run("dolthub", func(b *testing.B) {
		m := NewSwissLockFreeMapUint64(b.N)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})

	b.Run("cockroachdb", func(b *testing.B) {
		m := NewCRSwissLockFreeMapUint64(b.N)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})

	b.Run("native", func(b *testing.B) {
		m := NewNativeLockFreeMapUint64(b.N)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})

	b.Run("tidwall", func(b *testing.B) {
		m := NewTidwallLockFreeMapUint64(b.N)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})
}

// BenchmarkSwissLockFreeMapUint64Get measures Get performance for SwissLockFreeMapUint64.
func BenchmarkSwissLockFreeMapUint64Get(b *testing.B) {
	const size = 10000

	b.Run("dolthub", func(b *testing.B) {
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

	b.Run("cockroachdb", func(b *testing.B) {
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

	b.Run("native", func(b *testing.B) {
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

	b.Run("tidwall", func(b *testing.B) {
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
}

// BenchmarkSplitSwissLockFreeMapUint64Put measures Put performance for SplitSwissLockFreeMapUint64.
func BenchmarkSplitSwissLockFreeMapUint64Put(b *testing.B) {
	b.Run("dolthub", func(b *testing.B) {
		m := NewSplitSwissLockFreeMapUint64(b.N)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})

	b.Run("cockroachdb", func(b *testing.B) {
		m := NewCRSplitSwissLockFreeMapUint64(b.N)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})

	b.Run("native", func(b *testing.B) {
		m := NewNativeSplitLockFreeMapUint64(b.N)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})

	b.Run("tidwall", func(b *testing.B) {
		m := NewTidwallSplitLockFreeMapUint64(b.N)
		b.ReportAllocs()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			_ = m.Put(uint64(i), uint64(i))
		}
	})
}

// BenchmarkSplitSwissLockFreeMapUint64Get measures Get performance for SplitSwissLockFreeMapUint64.
func BenchmarkSplitSwissLockFreeMapUint64Get(b *testing.B) {
	const size = 10000

	b.Run("dolthub", func(b *testing.B) {
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

	b.Run("cockroachdb", func(b *testing.B) {
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

	b.Run("native", func(b *testing.B) {
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

	b.Run("tidwall", func(b *testing.B) {
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

// BenchmarkSwissMapUint64Iter measures Iter performance for SwissMapUint64.
func BenchmarkSwissMapUint64Iter(b *testing.B) {
	const size = 10000
	hashes := make([]chainhash.Hash, size)
	for i := 0; i < size; i++ {
		hashes[i] = chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
	}

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
	hashes := make([]chainhash.Hash, size)
	for i := 0; i < size; i++ {
		hashes[i] = chainhash.Hash{byte(i), byte(i >> 8), byte(i >> 16), byte(i >> 24)}
	}

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
