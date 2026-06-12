package txmap

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

// hashN returns a deterministic chainhash.Hash derived from i. i is stored
// big-endian in the first two bytes to match Bytes2Uint16Buckets, which reads
// b[0] as the high byte and b[1] as the low byte: bucket = (b[0]<<8 | b[1]) %
// nrOfBuckets. So consecutive i map to bucket i%nrOfBuckets and spread evenly
// across buckets, exercising split-bucket behaviour. Inputs in these tests stay
// below 65536, so both bytes are exact.
func hashN(i int) chainhash.Hash {
	var h chainhash.Hash
	h[0] = byte((i >> 8) & 0xff)
	h[1] = byte(i & 0xff)

	return h
}

// txMapImpls returns a fresh instance of every concrete type that implements
// TxMap, keyed by type name.
func txMapImpls() map[string]func() TxMap {
	return map[string]func() TxMap{
		"SwissMapUint64":       func() TxMap { return NewSwissMapUint64(1024) },
		"SplitSwissMap":        func() TxMap { return NewSplitSwissMap(1024) },
		"SplitSwissMapUint64":  func() TxMap { return NewSplitSwissMapUint64(1024) },
		"NativeMapUint64":      func() TxMap { return NewNativeMapUint64(1024) },
		"NativeSplitMap":       func() TxMap { return NewNativeSplitMap(1024) },
		"NativeSplitMapUint64": func() TxMap { return NewNativeSplitMapUint64(1024) },
	}
}

// txHashMapImpls returns a fresh instance of every concrete type that
// implements TxHashMap, keyed by type name.
func txHashMapImpls() map[string]func() TxHashMap {
	return map[string]func() TxHashMap{
		"SwissMap":  func() TxHashMap { return NewSwissMap(1024) },
		"NativeMap": func() TxHashMap { return NewNativeMap(1024) },
	}
}

// uint64Impls returns a fresh instance of every concrete type that implements
// Uint64, keyed by type name.
func uint64Impls() map[string]func() Uint64 {
	return map[string]func() Uint64{
		"SwissLockFreeMapUint64":       func() Uint64 { return NewSwissLockFreeMapUint64(1024) },
		"NativeLockFreeMapUint64":      func() Uint64 { return NewNativeLockFreeMapUint64(1024) },
		"SplitSwissLockFreeMapUint64":  func() Uint64 { return NewSplitSwissLockFreeMapUint64(1024) },
		"NativeSplitLockFreeMapUint64": func() Uint64 { return NewNativeSplitLockFreeMapUint64(1024) },
	}
}

// TestTxMapFreeze verifies the Freeze/Clear contract for every TxMap
// implementation: reads keep working while frozen, every write method fails
// with ErrMapFrozen, and Clear empties the map and un-freezes it for reuse.
func TestTxMapFreeze(t *testing.T) {
	for name, factory := range txMapImpls() {
		t.Run(name, func(t *testing.T) {
			m := factory()

			h1 := hashN(1)
			require.NoError(t, m.Put(h1, 42))

			m.Freeze()

			// Reads still work while frozen.
			v, ok := m.Get(h1)
			require.True(t, ok)
			require.Equal(t, uint64(42), v)
			require.True(t, m.Exists(h1))
			require.Equal(t, 1, m.Length())

			// Every write fails loudly with ErrMapFrozen.
			h2 := hashN(2)
			require.ErrorIs(t, m.Put(h2, 7), ErrMapFrozen)
			require.ErrorIs(t, m.PutMulti([]chainhash.Hash{h2}, 7), ErrMapFrozen)
			require.ErrorIs(t, m.Set(h1, 9), ErrMapFrozen)

			_, err := m.SetIfExists(h1, 9)
			require.ErrorIs(t, err, ErrMapFrozen)

			_, err = m.SetIfNotExists(h2, 7)
			require.ErrorIs(t, err, ErrMapFrozen)

			require.ErrorIs(t, m.Delete(h1), ErrMapFrozen)

			// The frozen write must not have mutated the map.
			require.Equal(t, 1, m.Length())

			// Clear empties and un-freezes: writes work again.
			m.Clear()
			require.Equal(t, 0, m.Length())
			require.NoError(t, m.Put(h2, 7))

			v, ok = m.Get(h2)
			require.True(t, ok)
			require.Equal(t, uint64(7), v)
		})
	}
}

// TestTxHashMapFreeze verifies the Freeze/Clear contract for every TxHashMap
// implementation.
func TestTxHashMapFreeze(t *testing.T) {
	for name, factory := range txHashMapImpls() {
		t.Run(name, func(t *testing.T) {
			m := factory()

			h1 := hashN(1)
			require.NoError(t, m.Put(h1))

			m.Freeze()

			require.True(t, m.Exists(h1))
			require.Equal(t, 1, m.Length())

			h2 := hashN(2)
			require.ErrorIs(t, m.Put(h2), ErrMapFrozen)
			require.ErrorIs(t, m.PutMulti([]chainhash.Hash{h2}), ErrMapFrozen)
			require.ErrorIs(t, m.Delete(h1), ErrMapFrozen)

			require.Equal(t, 1, m.Length())

			m.Clear()
			require.Equal(t, 0, m.Length())
			require.NoError(t, m.Put(h2))
			require.True(t, m.Exists(h2))
		})
	}
}

// TestUint64MapFreeze verifies the Freeze/Clear contract for every Uint64
// (lock-free) implementation. Reads remain lock-free; Freeze only guards writes.
func TestUint64MapFreeze(t *testing.T) {
	for name, factory := range uint64Impls() {
		t.Run(name, func(t *testing.T) {
			m := factory()

			require.NoError(t, m.Put(1, 42))

			m.Freeze()

			v, ok := m.Get(1)
			require.True(t, ok)
			require.Equal(t, uint64(42), v)
			require.True(t, m.Exists(1))
			require.Equal(t, 1, m.Length())

			require.ErrorIs(t, m.Put(2, 7), ErrMapFrozen)
			require.Equal(t, 1, m.Length())

			m.Clear()
			require.Equal(t, 0, m.Length())
			require.NoError(t, m.Put(2, 7))

			v, ok = m.Get(2)
			require.True(t, ok)
			require.Equal(t, uint64(7), v)
		})
	}
}

// readAllFrozen reads keys 0..n-1 from a frozen map and reports any mismatch.
// Extracted from TestFrozenConcurrentReads so each goroutine body stays simple.
func readAllFrozen(t *testing.T, m TxMap, n int) {
	t.Helper()

	for i := 0; i < n; i++ {
		v, ok := m.Get(hashN(i))
		if !ok || v != uint64(i) {
			t.Errorf("frozen read mismatch at %d: got (%d,%v)", i, v, ok)
			return
		}
	}
}

// TestFrozenConcurrentReads asserts that, once frozen, a TxMap can be read
// concurrently from many goroutines without a data race. This is the whole
// point of Freeze: the read path skips the per-bucket RWMutex. Run under -race.
func TestFrozenConcurrentReads(t *testing.T) {
	const (
		n       = 2000
		readers = 8
	)

	for name, factory := range txMapImpls() {
		t.Run(name, func(t *testing.T) {
			m := factory()
			for i := 0; i < n; i++ {
				require.NoError(t, m.Put(hashN(i), uint64(i)))
			}

			m.Freeze()

			var wg sync.WaitGroup

			wg.Add(readers)

			for r := 0; r < readers; r++ {
				go func() {
					defer wg.Done()
					readAllFrozen(t, m, n)
				}()
			}

			wg.Wait()
		})
	}
}
