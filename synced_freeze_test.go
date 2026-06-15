package txmap

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

// frozenContractMap is the Freeze-related method set shared by SyncedMap and
// SyncedSwissMap, so one set of assertions can cover both. Clear is omitted
// because the two types return different things from it (bool vs nothing); it
// is exercised via the per-type tests below.
type frozenContractMap[K comparable, V any] interface {
	Get(key K) (V, bool)
	Set(key K, value V)
	Delete(key K) bool
	Length() int
	Range() map[K]V
	Freeze()
}

// assertFrozenContract freezes an already-populated map holding {"a":a, "b":b}
// and verifies the shared contract: reads still return correct values and the
// Set/Delete writes panic without mutating the map.
func assertFrozenContract[V comparable](t *testing.T, m frozenContractMap[string, V], a, b V) {
	t.Helper()

	m.Freeze()

	v, ok := m.Get("a")
	require.True(t, ok)
	require.Equal(t, a, v)

	_, ok = m.Get("z")
	require.False(t, ok)

	require.Equal(t, 2, m.Length())

	items := m.Range()
	require.Equal(t, 2, len(items)) //nolint:testifylint // require.Len doesn't work with generic maps
	require.Equal(t, a, items["a"])

	require.Panics(t, func() { m.Set("b", b) })
	require.Panics(t, func() { m.Delete("a") })
	require.Equal(t, 2, m.Length(), "frozen map must be unmodified after panicking writes")
}

// assertFrozenConcurrentReads populates a map, freezes it, then reads it from
// many goroutines. Meant to be run under -race to prove the frozen fast path
// (which skips the RWMutex) is race-free for concurrent readers.
func assertFrozenConcurrentReads(t *testing.T, m frozenContractMap[int, int]) {
	t.Helper()

	const (
		n       = 500
		readers = 64
	)

	for i := 0; i < n; i++ {
		m.Set(i, i*10)
	}

	m.Freeze()

	var wg sync.WaitGroup

	wg.Add(readers)

	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()

			for i := 0; i < n; i++ {
				if v, ok := m.Get(i); !ok || v != i*10 {
					t.Errorf("frozen read mismatch at %d: got (%d, %v)", i, v, ok)
					return
				}
			}
		}()
	}

	wg.Wait()
}

// TestSyncedMapFreeze covers the shared frozen contract plus the SyncedMap-only
// read methods and write variants, and Clear's un-freeze.
func TestSyncedMapFreeze(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("a", 1)
	m.Set("b", 2)

	assertFrozenContract(t, m, 1, 2)

	// SyncedMap-specific reads remain correct while frozen.
	require.True(t, m.Exists("a"))
	require.False(t, m.Exists("z"))
	require.Equal(t, 2, len(m.Keys())) //nolint:testifylint // require.Len doesn't work with generic maps

	count := 0
	m.Iterate(func(_ string, _ int) bool {
		count++
		return true
	})
	require.Equal(t, 2, count)

	// SyncedMap-specific writes also panic while frozen.
	require.Panics(t, func() { m.SetIfNotExists("c", 3) })
	require.Panics(t, func() { m.SetMulti([]string{"x", "y"}, 9) })
	require.Panics(t, func() { m.SetIfNotExistsMulti([]string{"x"}, []int{1}) })

	// Clear un-freezes and empties; writes succeed again afterwards.
	require.True(t, m.Clear())
	require.Equal(t, 0, m.Length())

	m.Set("b", 2)
	v, ok := m.Get("b")
	require.True(t, ok)
	require.Equal(t, 2, v)
}

// TestSyncedSwissMapFreeze covers the shared frozen contract plus the
// SyncedSwissMap-only DeleteBatch write and Clear's un-freeze.
func TestSyncedSwissMapFreeze(t *testing.T) {
	m := NewSyncedSwissMap[string, int](16)
	m.Set("a", 1)
	m.Set("b", 2)

	assertFrozenContract(t, m, 1, 2)

	require.Panics(t, func() { m.DeleteBatch([]string{"a", "b"}) })

	// Clear un-freezes and empties; writes succeed again afterwards.
	m.Clear()
	require.Equal(t, 0, m.Length())

	m.Set("b", 2)
	v, ok := m.Get("b")
	require.True(t, ok)
	require.Equal(t, 2, v)
}

func TestSyncedMapFrozenConcurrentReads(t *testing.T) {
	assertFrozenConcurrentReads(t, NewSyncedMap[int, int]())
}

func TestSyncedSwissMapFrozenConcurrentReads(t *testing.T) {
	assertFrozenConcurrentReads(t, NewSyncedSwissMap[int, int](500))
}
