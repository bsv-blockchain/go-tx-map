package txmap

import (
	"sync"

	"github.com/dolthub/swiss"
)

// SyncedMap is a thread-safe generic map with read-write mutex synchronization.
// It supports concurrent access and provides an optional item limit for constrained storage.
type SyncedMap[K comparable, V any] struct {
	mu    sync.RWMutex
	m     map[K]V
	limit int
}

// NewSyncedMap creates and returns a new SyncedMap with an optional item limit.
//
// Parameters:
//   - l (optional): An integer specifying the maximum number of items allowed in the map. If omitted or zero, the map has no limit.
//
// Returns:
//   - *SyncedMap[K, V]: A pointer to a new, empty SyncedMap instance.
//
// If a limit is set and the map reaches its capacity, a random item will be deleted to make room for new entries.
func NewSyncedMap[K comparable, V any](l ...int) *SyncedMap[K, V] {
	limit := 0
	if len(l) > 0 {
		limit = l[0]
	}

	return &SyncedMap[K, V]{
		m:     make(map[K]V),
		limit: limit,
	}
}

// Length returns the number of key-value pairs currently stored in the SyncedMap.
//
// Returns:
//   - int: The number of items in the map.
func (m *SyncedMap[K, V]) Length() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return len(m.m)
}

// Exists checks if a key exists in the SyncedMap.
//
// Parameters:
//   - key: The key to check for existence.
//
// Returns:
//   - bool: True if the key exists, false otherwise.
func (m *SyncedMap[K, V]) Exists(key K) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	_, ok := m.m[key]

	return ok
}

// Get returns the value associated with the given key in the SyncedMap.
//
// Parameters:
//   - key: The key to retrieve the value for.
//
// Returns:
//   - V: The value associated with the key (zero value if not found).
//   - bool: True if the key exists, false otherwise.
func (m *SyncedMap[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	val, ok := m.m[key]

	return val, ok
}

// Range returns a copy of the SyncedMap as a standard Go map.
//
// Returns:
//   - map[K]V: A copy of the map's key-value pairs.
func (m *SyncedMap[K, V]) Range() map[K]V {
	m.mu.RLock()
	defer m.mu.RUnlock()

	items := make(map[K]V, len(m.m))

	for k, v := range m.m {
		items[k] = v
	}

	return items
}

// Keys returns a slice of all keys in the SyncedMap.
//
// Returns:
//   - []K: A slice containing all keys in the map.
func (m *SyncedMap[K, V]) Keys() []K {
	m.mu.RLock()
	defer m.mu.RUnlock()

	keys := make([]K, 0, len(m.m))

	for k := range m.m {
		keys = append(keys, k)
	}

	return keys
}

// Iterate calls the provided function for each key-value pair in the SyncedMap.
// The iteration stops if the function returns false.
//
// Parameters:
//   - f: A function that takes a key and value, and returns a bool indicating whether to continue iteration.
func (m *SyncedMap[K, V]) Iterate(f func(key K, value V) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	for k, v := range m.m {
		if !f(k, v) {
			return
		}
	}
}

// Set sets the value for the given key in the SyncedMap.
//
// Parameters:
//   - key: The key to set.
//   - value: The value to associate with the key.
func (m *SyncedMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.setUnlocked(key, value)
}

// SetIfNotExists sets the value for the key if it does not already exist in the SyncedMap.
//
// Parameters:
//   - key: The key to set the value for.
//   - value: The value to set for the key.
//
// Returns:
//   - V: The value that was set or already existed.
//   - bool: True if the value was set, false if the key already existed.
func (m *SyncedMap[K, V]) SetIfNotExists(key K, value V) (V, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if existingValue, ok := m.m[key]; ok {
		return existingValue, false
	}

	m.setUnlocked(key, value)

	return value, true
}

func (m *SyncedMap[K, V]) setUnlocked(key K, value V) {
	if m.limit > 0 && len(m.m) >= m.limit {
		for k := range m.m {
			// delete a random item
			delete(m.m, k)
			break
		}
	}

	m.m[key] = value
}

// SetMulti sets the given value for multiple keys in the SyncedMap.
//
// Parameters:
//   - keys: A slice of keys to set.
//   - value: The value to associate with each key.
func (m *SyncedMap[K, V]) SetMulti(keys []K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// add the keys
	for _, key := range keys {
		m.setUnlocked(key, value)
	}
}

// Delete removes the key and its value from the SyncedMap.
//
// Parameters:
//   - key: The key to delete.
//
// Returns:
//   - bool: True if the key was deleted, false otherwise.
func (m *SyncedMap[K, V]) Delete(key K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.m, key)

	return true
}

// Clear removes all key-value pairs from the SyncedMap.
//
// Returns:
//   - bool: True if the map was cleared successfully.
func (m *SyncedMap[K, V]) Clear() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.m = make(map[K]V)

	return true
}

// SyncedSlice is a thread-safe wrapper around a slice of pointers, providing synchronized access and modification operations.
type SyncedSlice[V any] struct {
	mu    sync.RWMutex
	items []*V
}

// NewSyncedSlice creates and returns a new SyncedSlice with an optional initial capacity.
//
// Parameters:
//   - length (optional): The initial capacity of the slice. If omitted or zero, the slice has no preallocated capacity.
//
// Returns:
//   - *SyncedSlice[V]: A pointer to a new, empty SyncedSlice instance.
func NewSyncedSlice[V any](length ...int) *SyncedSlice[V] {
	initialLength := 0
	if len(length) > 0 {
		initialLength = length[0]
	}

	return &SyncedSlice[V]{
		items: make([]*V, 0, initialLength),
	}
}

// Length returns the number of items currently stored in the SyncedSlice.
//
// Returns:
//   - int: The number of items in the slice.
func (s *SyncedSlice[V]) Length() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.items)
}

// Size returns the capacity of the SyncedSlice.
//
// Returns:
//   - int: The capacity of the slice.
func (s *SyncedSlice[V]) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return cap(s.items)
}

// Get returns the item at the specified index in the SyncedSlice.
//
// Parameters:
//   - index: The index of the item to retrieve.
//
// Returns:
//   - *V: A pointer to the item at the specified index, or nil if out of range.
//   - bool: True if the item exists at the index, false otherwise.
func (s *SyncedSlice[V]) Get(index int) (*V, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index < 0 || index >= len(s.items) {
		return nil, false
	}

	return s.items[index], true
}

// Append adds an item to the end of the SyncedSlice.
//
// Parameters:
//   - item: A pointer to the item to append to the slice.
func (s *SyncedSlice[V]) Append(item *V) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.items = append(s.items, item)
}

// Pop removes and returns the last item in the SyncedSlice.
//
// Returns:
//   - *V: A pointer to the last item, or nil if the slice is empty.
//   - bool: True if an item was returned, false if the slice was empty.
func (s *SyncedSlice[V]) Pop() (*V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.items) == 0 {
		return nil, false
	}

	item := s.items[len(s.items)-1]
	s.items = s.items[:len(s.items)-1]

	return item, true
}

// Shift removes and returns the first item in the SyncedSlice.
//
// Returns:
//   - *V: A pointer to the first item, or nil if the slice is empty.
//   - bool: True if an item was returned, false if the slice was empty.
func (s *SyncedSlice[V]) Shift() (*V, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.items) == 0 {
		return nil, false
	}

	item := s.items[0]
	s.items = s.items[1:]

	return item, true
}

// SyncedSwissMap is a concurrent-safe wrapper around swiss.Map, providing locking mechanisms for thread-safety.
type SyncedSwissMap[K comparable, V any] struct {
	mu       sync.RWMutex
	swissMap *swiss.Map[K, V]
}

// NewSyncedSwissMap creates and returns a new SyncedSwissMap with the specified initial capacity.
//
// Parameters:
//   - length: The initial capacity of the underlying swiss.Map.
//
// Returns:
//   - *SyncedSwissMap[K, V]: A pointer to a new, empty SyncedSwissMap instance.
func NewSyncedSwissMap[K comparable, V any](length uint32) *SyncedSwissMap[K, V] {
	return &SyncedSwissMap[K, V]{
		swissMap: swiss.NewMap[K, V](length),
	}
}

// Get returns the value associated with the given key in the SyncedSwissMap.
//
// Parameters:
//   - key: The key to retrieve the value for.
//
// Returns:
//   - V: The value associated with the key (zero value if not found).
//   - bool: True if the key exists, false otherwise.
func (m *SyncedSwissMap[K, V]) Get(key K) (V, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.swissMap.Get(key)
}

// Range returns a copy of the SyncedSwissMap as a standard Go map.
//
// Returns:
//   - map[K]V: A copy of the map's key-value pairs.
func (m *SyncedSwissMap[K, V]) Range() map[K]V {
	m.mu.RLock()
	defer m.mu.RUnlock()

	items := map[K]V{}

	m.swissMap.Iter(func(key K, value V) bool {
		items[key] = value
		return false
	})

	return items
}

// Set sets the value for the given key in the SyncedSwissMap.
//
// Parameters:
//   - key: The key to set.
//   - value: The value to associate with the key.
func (m *SyncedSwissMap[K, V]) Set(key K, value V) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.swissMap.Put(key, value)
}

// Length returns the number of key-value pairs currently stored in the SyncedSwissMap.
//
// Returns:
//   - int: The number of items in the map.
func (m *SyncedSwissMap[K, V]) Length() int {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.swissMap.Count()
}

// Delete removes the key and its value from the SyncedSwissMap.
//
// Parameters:
//   - key: The key to delete.
//
// Returns:
//   - bool: True if the key was deleted, false otherwise.
func (m *SyncedSwissMap[K, V]) Delete(key K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.swissMap.Delete(key)
}

// DeleteBatch removes multiple keys and their values from the SyncedSwissMap.
//
// Parameters:
//   - keys: A slice of keys to delete.
//
// Returns:
//   - bool: True if at least one key was deleted, false otherwise.
func (m *SyncedSwissMap[K, V]) DeleteBatch(keys []K) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	ok := false

	for _, key := range keys {
		ok = m.swissMap.Delete(key)
	}

	return ok
}
