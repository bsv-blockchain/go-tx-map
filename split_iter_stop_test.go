package txmap

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

const splitIterEntries = 3000

func splitIterHash(i uint64) chainhash.Hash {
	var b [8]byte

	binary.LittleEndian.PutUint64(b[:], i)

	return chainhash.Hash(sha256.Sum256(b[:]))
}

// hashIterMap and uint64IterMap are the common iteration surfaces of the split maps under test.
type hashIterMap interface {
	Put(hash chainhash.Hash, n uint64) error
	Iter(f func(hash chainhash.Hash, value uint64) bool)
}

type uint64IterMap interface {
	Put(hash, n uint64) error
	IterAll(f func(k, v uint64) (stop bool))
}

func splitIterStopCounts() []int {
	return []int{1, 2, 7, 500, splitIterEntries - 1, splitIterEntries}
}

func checkHashSplitIter(t *testing.T, m hashIterMap) {
	t.Helper()

	for i := uint64(0); i < splitIterEntries; i++ {
		require.NoError(t, m.Put(splitIterHash(i), i))
	}

	t.Run("false visits every entry once", func(t *testing.T) {
		seen := map[chainhash.Hash]int{}

		m.Iter(func(h chainhash.Hash, _ uint64) bool {
			seen[h]++
			return false
		})

		require.Len(t, seen, splitIterEntries)

		for _, c := range seen {
			require.Equal(t, 1, c)
		}
	})

	for _, k := range splitIterStopCounts() {
		t.Run(fmt.Sprintf("stop after %d visits", k), func(t *testing.T) {
			visits := 0

			m.Iter(func(chainhash.Hash, uint64) bool {
				visits++
				return visits >= k
			})

			require.Equal(t, k, visits)
		})
	}
}

func checkUint64SplitIter(t *testing.T, m uint64IterMap) {
	t.Helper()

	for i := uint64(0); i < splitIterEntries; i++ {
		require.NoError(t, m.Put(i, i))
	}

	t.Run("false visits every entry once", func(t *testing.T) {
		seen := map[uint64]int{}

		m.IterAll(func(k, _ uint64) bool {
			seen[k]++
			return false
		})

		require.Len(t, seen, splitIterEntries)

		for _, c := range seen {
			require.Equal(t, 1, c)
		}
	})

	for _, k := range splitIterStopCounts() {
		t.Run(fmt.Sprintf("stop after %d visits", k), func(t *testing.T) {
			visits := 0

			m.IterAll(func(_, _ uint64) bool {
				visits++
				return visits >= k
			})

			require.Equal(t, k, visits)
		})
	}
}

func TestSplitHashMapsIterHonourStop(t *testing.T) {
	maps := map[string]hashIterMap{
		"SplitSwissMap":        NewSplitSwissMap(splitIterEntries),
		"SplitSwissMapUint64":  NewSplitSwissMapUint64(splitIterEntries),
		"NativeSplitMap":       NewNativeSplitMap(splitIterEntries),
		"NativeSplitMapUint64": NewNativeSplitMapUint64(splitIterEntries),
	}

	for name, m := range maps {
		t.Run(name, func(t *testing.T) { checkHashSplitIter(t, m) })
	}
}

func TestSplitLockFreeMapsIterAllHonourStop(t *testing.T) {
	maps := map[string]uint64IterMap{
		"SplitSwissLockFreeMapUint64":  NewSplitSwissLockFreeMapUint64(splitIterEntries),
		"NativeSplitLockFreeMapUint64": NewNativeSplitLockFreeMapUint64(splitIterEntries),
	}

	for name, m := range maps {
		t.Run(name, func(t *testing.T) { checkUint64SplitIter(t, m) })
	}
}
