package txmap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestSyncedMapLength tests the Length method of SyncedMap.
func TestSyncedMapLength(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	m.Set("key2", 2)
	assert.Equal(t, 2, m.Length())
}

// TestSyncedMapExists tests the Exists method of SyncedMap.
func TestSyncedMapExists(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	assert.True(t, m.Exists("key1"))
	assert.False(t, m.Exists("key2"))
}

// TestSyncedMapGet tests the Get method of SyncedMap.
func TestSyncedMapGet(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	val, ok := m.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, 1, val)
}

// TestSyncedMapGetWithLimit tests the behavior of SyncedMap when a limit is set on the number of items.
func TestSyncedMapGetWithLimit(t *testing.T) {
	t.Run("limit 1", func(t *testing.T) {
		m := NewSyncedMap[string, int](1)
		m.Set("key1", 1)
		m.Set("key2", 2)

		_, ok1 := m.Get("key1")
		_, ok2 := m.Get("key2")
		assert.False(t, ok1 && ok2)
	})

	t.Run("limit multiple", func(t *testing.T) {
		m := NewSyncedMap[string, int](2)
		m.SetMulti([]string{"key1", "key2", "key3"}, 1)

		_, ok1 := m.Get("key1")
		_, ok2 := m.Get("key2")
		_, ok3 := m.Get("key3")
		assert.False(t, ok1 && ok2 && ok3)
	})
}

// TestSyncedMapRange tests the Range method of SyncedMap.
func TestSyncedMapRange(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	m.Set("key2", 2)
	items := m.Range()
	assert.Equal(t, 2, len(items)) //nolint:testifylint // assert.Len doesn't work with the map
	assert.Equal(t, 1, items["key1"])
	assert.Equal(t, 2, items["key2"])
}

// TestSyncedMapKeys tests the Keys method of SyncedMap.
func TestSyncedMapKeys(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	m.Set("key2", 2)
	keys := m.Keys()
	assert.Equal(t, 2, len(keys)) //nolint:testifylint // assert.Len doesn't work with the map
	assert.Contains(t, keys, "key1")
	assert.Contains(t, keys, "key2")
}

// TestSyncedMapSetIfNotExists tests the SetIfNotExists method of SyncedMap.
func TestSyncedMapSetIfNotExists(t *testing.T) {
	m := NewSyncedMap[string, int]()

	val, isSet := m.SetIfNotExists("key1", 1)
	assert.Equal(t, 1, val)
	assert.True(t, isSet) // should be set since it didn't exist before

	val, isSet = m.SetIfNotExists("key1", 2)
	assert.Equal(t, 1, val)
	assert.False(t, isSet) // should not be set since it already exists
}

// TestSyncedMapIterate tests the Iterate method of SyncedMap.
func TestSyncedMapIterate(t *testing.T) {
	t.Run("continue iteration", func(t *testing.T) {
		m := NewSyncedMap[string, int]()
		m.Set("key1", 1)
		m.Set("key2", 2)

		count := 0

		m.Iterate(func(_ string, _ int) bool {
			count++
			return true
		})
		assert.Equal(t, 2, count)
	})

	t.Run("stop iteration", func(t *testing.T) {
		m := NewSyncedMap[string, int]()
		m.Set("key1", 1)
		m.Set("key2", 2)

		count := 0

		m.Iterate(func(_ string, _ int) bool {
			count++
			return false
		})
		assert.Equal(t, 1, count)
	})
}

// TestSyncedMapSetMulti tests the SetMulti method of SyncedMap.
func TestSyncedMapSetMulti(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.SetMulti([]string{"key1", "key2"}, 1)
	assert.Equal(t, 1, m.m["key1"])
	assert.Equal(t, 1, m.m["key2"])
}

// TestSyncedMapDelete tests the Delete and Exists methods of SyncedMap.
func TestSyncedMapDelete(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	assert.True(t, m.Delete("key1"))
	assert.False(t, m.Exists("key1"))
}

// TestSyncedMapClear tests the Clear and Length methods of SyncedMap.
func TestSyncedMapClear(t *testing.T) {
	m := NewSyncedMap[string, int]()
	m.Set("key1", 1)
	m.Set("key2", 2)
	assert.True(t, m.Clear())
	assert.Equal(t, 0, m.Length())
}

// TestSyncedSliceLength tests the Length and Size methods of SyncedSlice.
func TestSyncedSliceLength(t *testing.T) {
	t.Run("length not set", func(t *testing.T) {
		s := NewSyncedSlice[int]()
		assert.Equal(t, 0, s.Length())
		s.Append(new(int))
		assert.Equal(t, 1, s.Length())
		assert.Equal(t, 1, s.Size())
	})

	t.Run("length set", func(t *testing.T) {
		s := NewSyncedSlice[int](5)
		assert.Equal(t, 0, s.Length())
		s.Append(new(int))
		assert.Equal(t, 1, s.Length())
		assert.Equal(t, 5, s.Size())
	})
}

// TestSyncedSliceGet tests the Get method of SyncedSlice.
func TestSyncedSliceGet(t *testing.T) {
	s := NewSyncedSlice[int]()
	val := 42
	s.Append(&val)
	item, ok := s.Get(0)
	assert.True(t, ok)
	assert.Equal(t, 42, *item)

	_, ok = s.Get(1)
	assert.False(t, ok)
}

// TestSyncedSliceAppend tests the Append method of SyncedSlice.
func TestSyncedSliceAppend(t *testing.T) {
	s := NewSyncedSlice[int]()
	val := 42
	s.Append(&val)
	assert.Equal(t, 1, s.Length())
	item, ok := s.Get(0)
	assert.True(t, ok)
	assert.Equal(t, 42, *item)
}

// TestSyncedSlicePop tests the Pop method of SyncedSlice.
func TestSyncedSlicePop(t *testing.T) {
	s := NewSyncedSlice[int]()
	val := 42
	s.Append(&val)
	item, ok := s.Pop()
	assert.True(t, ok)
	assert.Equal(t, 42, *item)
	assert.Equal(t, 0, s.Length())
	_, ok = s.Pop()
	assert.False(t, ok)
}

// TestSyncedSliceShift tests the Shift method of SyncedSlice.
func TestSyncedSliceShift(t *testing.T) {
	s := NewSyncedSlice[int]()
	val := 42
	s.Append(&val)

	val2 := 43
	s.Append(&val2)

	item, ok := s.Shift()
	assert.True(t, ok)
	assert.Equal(t, 42, *item)
	assert.Equal(t, 1, s.Length())

	item, ok = s.Shift()
	assert.True(t, ok)
	assert.Equal(t, 43, *item)
	assert.Equal(t, 0, s.Length())

	_, ok = s.Shift()
	assert.False(t, ok)
}

// TestSyncedSwissMapLength tests the Length method of SyncedSwissMap.
func TestSyncedSwissMapLength(t *testing.T) {
	m := NewSyncedSwissMap[string, int](10)
	m.Set("key1", 1)
	m.Set("key2", 2)
	assert.Equal(t, 2, m.Length())
}

// TestSyncedSwissMapGet tests the Get method of SyncedSwissMap.
func TestSyncedSwissMapGet(t *testing.T) {
	m := NewSyncedSwissMap[string, int](10)
	m.Set("key1", 1)
	val, ok := m.Get("key1")
	assert.True(t, ok)
	assert.Equal(t, 1, val)
}

// TestSyncedSwissMapRange tests the Range method of SyncedSwissMap.
func TestSyncedSwissMapRange(t *testing.T) {
	m := NewSyncedSwissMap[string, int](10)
	m.Set("key1", 1)
	m.Set("key2", 2)
	items := m.Range()
	assert.Equal(t, 2, len(items)) //nolint:testifylint // assert.Len doesn't work with the map
	assert.Equal(t, 1, items["key1"])
	assert.Equal(t, 2, items["key2"])
}

// TestSyncedSwissMapDelete tests the Delete and Get methods of SyncedSwissMap.
func TestSyncedSwissMapDelete(t *testing.T) {
	m := NewSyncedSwissMap[string, int](10)
	m.Set("key1", 1)
	assert.True(t, m.Delete("key1"))
	_, ok := m.Get("key1")
	assert.False(t, ok)
}

// TestSyncedSwissMapDeleteBatch tests the DeleteBatch method of SyncedSwissMap.
func TestSyncedSwissMapDeleteBatch(t *testing.T) {
	m := NewSyncedSwissMap[string, int](10)
	m.Set("key1", 1)
	m.Set("key2", 2)
	assert.True(t, m.DeleteBatch([]string{"key1", "key2"}))
	assert.Equal(t, 0, m.Length())
}
