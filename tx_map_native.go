// Package txmap provides alternative implementations using Go's native map
// (which uses Swiss Tables in Go 1.24+) for benchmarking purposes.
package txmap

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// NativeMap is a simple concurrent-safe map that uses Go's native map
type NativeMap struct {
	mu     sync.RWMutex
	m      map[chainhash.Hash]struct{}
	length int
}

// NewNativeMap creates a new NativeMap with the specified initial length.
// The length is used to preallocate the map size for better performance.
// It is not a hard limit, but a hint to the underlying map.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *NativeMap: A pointer to the newly created NativeMap instance.
//
// Considerations: The length is not enforced, and the map can grow beyond this size.
func NewNativeMap(length uint32) *NativeMap {
	return &NativeMap{
		m: make(map[chainhash.Hash]struct{}, length),
	}
}

// Exists checks if the given hash exists in the map.
// It returns true if the hash is found, false otherwise.
//
// Params:
//   - hash: The hash to check for existence in the map.
//
// Returns:
//   - bool: True if the hash exists in the map, false otherwise.
func (s *NativeMap) Exists(hash chainhash.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m[hash]

	return ok
}

// Get retrieves the value associated with the given hash from the map.
// It always returns 0 and a boolean indicating whether the hash was found.
//
// Params:
//   - hash: The hash to retrieve from the map.
//
// Returns:
//   - uint64: Always returns 0, as this map does not store values.
//   - bool: True if the hash was found in the map, false otherwise.
func (s *NativeMap) Get(hash chainhash.Hash) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m[hash]

	return 0, ok
}

// Put adds a new hash to the map. It increments the length of the map.
//
// Params:
//   - hash: The hash to add to the map.
//
// Returns:
//   - error: always returns nil, as this map does not have any constraints on adding hashes.
func (s *NativeMap) Put(hash chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length++

	s.m[hash] = struct{}{}

	return nil
}

// PutMulti adds multiple hashes to the map. It increments the length of the map for each hash added.
//
// Params:
//   - hashes: A slice of hashes to add to the map.
//
// Returns:
//   - error: always returns nil, as this map does not have any constraints on adding hashes.
func (s *NativeMap) PutMulti(hashes []chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hash := range hashes {
		s.m[hash] = struct{}{}

		s.length++
	}

	return nil
}

// Delete removes a hash from the map. It decrements the length of the map.
//
// Params:
//   - hash: The hash to remove from the map.
//
// Returns:
//   - error: always returns nil, as this map does not have any constraints on deleting hashes.
func (s *NativeMap) Delete(hash chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length--

	delete(s.m, hash)

	return nil
}

// Length returns the current number of hashes in the map.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
func (s *NativeMap) Length() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.length
}

// Keys returns a slice of all hashes currently stored in the map.
// It iterates over the map and collects the keys.
// The order of keys is not guaranteed.
//
// Returns:
//   - []chainhash.Hash: A slice containing all the hashes in the map.
func (s *NativeMap) Keys() []chainhash.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]chainhash.Hash, 0, s.length)

	for k := range s.m {
		keys = append(keys, k)
	}

	return keys
}

// Map returns the TxHashMap
func (s *NativeMap) Map() TxHashMap {
	return s
}

// Iter iterates over all key-value pairs in the map and applies the provided function to each pair.
// Stops iterating if the function returns true.
//
// Params:
//   - f: A function that takes a hash and its associated value (always 0 in this map).
func (s *NativeMap) Iter(f func(hash chainhash.Hash, value uint64) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k := range s.m {
		if f(k, 0) {
			return
		}
	}
}

// check that NativeMapUint64 implements TxMap
var _ TxMap = (*NativeMapUint64)(nil)

// NativeMapUint64 is a concurrent-safe map that uses Go's native map to store
// transaction hashes as keys and uint64 values.
type NativeMapUint64 struct {
	mu     sync.RWMutex
	m      map[chainhash.Hash]uint64
	length int
}

// NewNativeMapUint64 creates a new NativeMapUint64 with the specified initial length.
// The length is used to preallocate the map size for better performance.
// It is not a hard limit, but a hint to the underlying map.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *NativeMapUint64: A pointer to the newly created NativeMapUint64 instance.
func NewNativeMapUint64(length uint32) *NativeMapUint64 {
	return &NativeMapUint64{
		m: make(map[chainhash.Hash]uint64, length),
	}
}

// Map returns the underlying native map used by NativeMapUint64.
//
// Returns:
//   - map[chainhash.Hash]uint64: The underlying native map.
func (s *NativeMapUint64) Map() map[chainhash.Hash]uint64 {
	return s.m
}

// Exists checks if the given hash exists in the map.
// It returns true if the hash is found, false otherwise.
//
// Params:
//   - hash: The hash to check for existence in the map.
//
// Returns:
//   - bool: True if the hash exists in the map, false otherwise.
func (s *NativeMapUint64) Exists(hash chainhash.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m[hash]

	return ok
}

// Put adds a new hash with an associated uint64 value to the map.
// It checks if the hash already exists in the map and returns an error if it does.
// If the hash does not exist, it adds the hash and increments the length of the map.
//
// Params:
//   - hash: The hash to add to the map.
//   - n: The uint64 value to associate with the hash.
//
// Returns:
//   - error: An error if the hash already exists in the map, nil otherwise.
func (s *NativeMapUint64) Put(hash chainhash.Hash, n uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m[hash]
	if exists {
		return fmt.Errorf(errWrapFormat, ErrHashAlreadyExists, hash)
	}

	s.m[hash] = n

	s.length++

	return nil
}

// PutMulti adds multiple hashes with an associated uint64 value to the map.
// It checks if any of the hashes already exist in the map and returns an error if any do.
// If none of the hashes exist, it adds each hash with the value and increments the length of the map.
//
// Params:
//   - hashes: A slice of hashes to add to the map.
//   - n: The uint64 value to associate with each hash.
//
// Returns:
//   - error: An error if any of the hashes already exist in the map, nil otherwise.
func (s *NativeMapUint64) PutMulti(hashes []chainhash.Hash, n uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hash := range hashes {
		_, exists := s.m[hash]
		if exists {
			return fmt.Errorf(errWrapFormat, ErrHashAlreadyExists, hash)
		}

		s.m[hash] = n

		s.length++
	}

	return nil
}

// Set updates the value associated with the given hash in the map.
// It will error out if the hash does not exist.
//
// Params:
//   - hash: The hash to update in the map.
//   - value: The value to associate with the hash (not used in this map).
//
// Returns:
//   - error: An error if the hash does not exist in the map, nil otherwise.
func (s *NativeMapUint64) Set(hash chainhash.Hash, value uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m[hash]
	if !exists {
		return fmt.Errorf(errWrapFormat, ErrHashDoesNotExist, hash)
	}

	s.m[hash] = value

	return nil
}

// SetIfExists updates the value associated with the given hash in the map if it exists.
// It returns a boolean indicating whether the hash was found and updated.
// If the hash does not exist, it returns false and no error.
//
// Params:
//   - hash: The hash to update in the map.
//   - value: The value to associate with the hash.
//
// Returns:
//   - bool: True if the hash was found and updated, false otherwise.
//   - error: An error if there was an issue updating the hash, nil otherwise.
func (s *NativeMapUint64) SetIfExists(hash chainhash.Hash, value uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m[hash]
	if !exists {
		return false, nil
	}

	s.m[hash] = value

	return true, nil
}

// SetIfNotExists adds the hash with the given value to the map only if the hash does not already exist.
// It returns a boolean indicating whether the hash was added.
// If the hash already exists, it returns false and no error.
//
// Params:
//   - hash: The hash to add to the map.
//   - value: The value to associate with the hash.
//
// Returns:
//   - bool: True if the hash was added, false if it already existed.
//   - error: An error if there was an issue adding the hash, nil otherwise.
func (s *NativeMapUint64) SetIfNotExists(hash chainhash.Hash, value uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m[hash]
	if exists {
		return false, nil
	}

	s.m[hash] = value

	s.length++

	return true, nil
}

// Get retrieves the uint64 value associated with the given hash from the map.
// It locks the map for reading, checks if the hash exists, and returns the value and a boolean indicating success.
// If the hash does not exist, it returns 0 and false.
//
// Params:
//   - hash: The hash to retrieve from the map.
//
// Returns:
//   - uint64: The value associated with the hash, or 0 if the hash does not exist.
//   - bool: True if the hash was found in the map, false otherwise.
func (s *NativeMapUint64) Get(hash chainhash.Hash) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n, ok := s.m[hash]
	if !ok {
		return 0, false
	}

	return n, true
}

// Length returns the current number of hashes in the map.
// It locks the map for reading and returns the length.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
func (s *NativeMapUint64) Length() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.length
}

// Keys returns a slice of all hashes currently stored in the map.
// It locks the map for reading, iterates over the map, and collects the keys.
// The order of keys is not guaranteed.
//
// Returns:
//   - []chainhash.Hash: A slice containing all the hashes in the map.
func (s *NativeMapUint64) Keys() []chainhash.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]chainhash.Hash, 0, s.length)

	for k := range s.m {
		keys = append(keys, k)
	}

	return keys
}

// Iter iterates over all key-value pairs in the map and applies the provided function to each pair.
// Stops iterating if the function returns true.
//
// Params:
//   - f: A function that takes a hash and its associated uint64 value.
func (s *NativeMapUint64) Iter(f func(hash chainhash.Hash, value uint64) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for k, v := range s.m {
		if f(k, v) {
			return
		}
	}
}

// Delete removes a hash from the map. It decrements the length of the map.
// It locks the map for writing, checks if the hash exists, and removes it if found.
// If the hash does not exist, it returns an error.
//
// Params:
//   - hash: The hash to remove from the map.
//
// Returns:
//   - error: An error if the hash does not exist in the map, nil otherwise.
func (s *NativeMapUint64) Delete(hash chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m[hash]
	if !exists {
		return fmt.Errorf("%w: %s", ErrHashDoesNotExist, hash)
	}

	delete(s.m, hash)

	s.length--

	return nil
}

// NativeLockFreeMapUint64 is a lock-free map for uint64 keys and values
type NativeLockFreeMapUint64 struct {
	m      map[uint64]uint64
	length atomic.Uint32
}

// NewNativeLockFreeMapUint64 creates a new NativeLockFreeMapUint64 with the specified initial length.
// The length is used to preallocate the map size for better performance.
// It is not a hard limit, but a hint to the underlying map.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *NativeLockFreeMapUint64: A pointer to the newly created NativeLockFreeMapUint64 instance.
func NewNativeLockFreeMapUint64(length int) *NativeLockFreeMapUint64 {
	return &NativeLockFreeMapUint64{
		m:      make(map[uint64]uint64, length),
		length: atomic.Uint32{},
	}
}

// Map returns the underlying native map used by NativeLockFreeMapUint64.
// It provides access to the map for operations that do not require locking.
//
// Returns:
//   - map[uint64]uint64: The underlying native map.
//
// Considerations: This method does not lock the map, so it is not suitable for concurrent access.
func (s *NativeLockFreeMapUint64) Map() map[uint64]uint64 {
	return s.m
}

// Exists checks if the given hash exists in the map.
//
// Params:
//   - hash: The hash to check for existence in the map.
//
// Returns:
//   - bool: True if the hash exists in the map, false otherwise.
//
// Considerations: This method does not lock the map, so it is not suitable for concurrent access.
func (s *NativeLockFreeMapUint64) Exists(hash uint64) bool {
	_, ok := s.m[hash]
	return ok
}

// Put adds a new hash with an associated uint64 value to the map.
// It checks if the hash already exists in the map and returns an error if it does.
// If the hash does not exist, it adds the hash and increments the length of the map.
//
// Params:
//   - hash: The hash to add to the map.
//   - n: The uint64 value to associate with the hash.
//
// Returns:
//   - error: An error if the hash already exists in the map, nil otherwise.
//
// Considerations: This method does not lock the map, so it is not suitable for concurrent access.
func (s *NativeLockFreeMapUint64) Put(hash, n uint64) error {
	_, exists := s.m[hash]
	if exists {
		return ErrHashAlreadyExists
	}

	s.m[hash] = n
	s.length.Add(1)

	return nil
}

// Get retrieves the uint64 value associated with the given hash from the map.
//
// Params:
//   - hash: The hash to retrieve from the map.
//
// Returns:
//   - uint64: The value associated with the hash, or 0 if the hash does not exist.
//   - bool: True if the hash was found in the map, false otherwise.
//
// Considerations: This method does not lock the map, so it is not suitable for concurrent access.
func (s *NativeLockFreeMapUint64) Get(hash uint64) (uint64, bool) {
	n, ok := s.m[hash]
	if !ok {
		return 0, false
	}

	return n, true
}

// Length returns the current number of hashes in the map.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
//
// Considerations: This method uses atomic operations to retrieve the length, making it safe for concurrent access.
func (s *NativeLockFreeMapUint64) Length() int {
	return int(s.length.Load())
}

// check that NativeSplitMap implements TxMap
var _ TxMap = (*NativeSplitMap)(nil)

// NativeSplitMap is a map that splits the data into multiple buckets to reduce contention.
// It uses NativeMapUint64 for each bucket to store the hashes and their associated uint64 values.
// Since NativeMapUint64 is concurrent-safe, NativeSplitMap can handle concurrent access without additional locks.
type NativeSplitMap struct {
	m           map[uint16]*NativeMapUint64
	nrOfBuckets uint16
}

// NewNativeSplitMap creates a new NativeSplitMap with the specified initial length.
// The length is used to preallocate the size of each bucket.
// It divides the length by the number of buckets to determine the size of each bucket.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *NativeSplitMap: A pointer to the newly created NativeSplitMap instance.
//
// Considerations: The number of buckets is fixed at 1024, and the length is divided by this number to determine the size of each bucket.
func NewNativeSplitMap(length int, buckets ...uint16) *NativeSplitMap {
	useBuckets := uint16(1024)
	if len(buckets) > 0 {
		useBuckets = buckets[0]
	}

	m := &NativeSplitMap{
		m:           make(map[uint16]*NativeMapUint64, useBuckets),
		nrOfBuckets: useBuckets,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewNativeMapUint64(uint32(math.Ceil(float64(length) / float64(m.nrOfBuckets))))
	}

	return m
}

// Buckets returns the number of buckets in the NativeSplitMap.
func (g *NativeSplitMap) Buckets() uint16 {
	return g.nrOfBuckets
}

// Exists checks if the given hash exists in the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and checks the corresponding bucket.
//
// Params:
//   - hash: The hash to check for existence in the map.
//
// Returns:
//   - bool: True if the hash exists in the map, false otherwise.
func (g *NativeSplitMap) Exists(hash chainhash.Hash) bool {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Exists(hash)
}

// Get retrieves the uint64 value associated with the given hash from the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and retrieves the value from the corresponding bucket.
//
// Params:
//   - hash: The hash to retrieve from the map.
//
// Returns:
//   - uint64: The value associated with the hash, or 0 if the hash does not exist.
//   - bool: True if the hash was found in the map, false otherwise.
func (g *NativeSplitMap) Get(hash chainhash.Hash) (uint64, bool) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Get(hash)
}

// Put adds a new hash with an associated uint64 value to the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and adds the hash to the corresponding bucket.
// It checks if the hash already exists in the bucket and returns an error if it does.
//
// Params:
//   - hash: The hash to add to the map.
//   - n: The uint64 value to associate with the hash.
//
// Returns:
//   - error: An error if the hash already exists in the map, nil otherwise.
func (g *NativeSplitMap) Put(hash chainhash.Hash, n uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n)
}

// PutMulti adds multiple hashes with an associated uint64 value to the map.
// It iterates over the hashes, calculates the bucket index for each hash using the Bytes2Uint16Buckets function,
// and adds each hash to the corresponding bucket.
// It checks if any of the hashes already exist in the bucket and returns an error if any do.
//
// Params:
//   - hashes: A slice of hashes to add to the map.
//   - n: The uint64 value to associate with each hash.
//
// Returns:
//   - error: An error if any of the hashes already exist in the map, nil otherwise.
func (g *NativeSplitMap) PutMulti(hashes []chainhash.Hash, n uint64) (err error) {
	for _, hash := range hashes {
		if err = g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n); err != nil {
			return fmt.Errorf("failed to put multi in bucket %d: %w", Bytes2Uint16Buckets(hash, g.nrOfBuckets), err)
		}
	}

	return nil
}

// PutMultiBucket adds multiple hashes with an associated uint64 value to a specific bucket.
// It checks if the bucket exists and then adds the hashes directly to that bucket.
//
// Params:
//   - bucket: The bucket index to add the hashes to.
//   - hashes: A slice of hashes to add to the specified bucket.
//   - n: The uint64 value to associate with each hash.
//
// Returns:
//   - error: An error if the bucket does not exist or if there is an issue adding the hashes, nil otherwise.
func (g *NativeSplitMap) PutMultiBucket(bucket uint16, hashes []chainhash.Hash, n uint64) error {
	if bucket > g.nrOfBuckets {
		return fmt.Errorf("%w: %d, max bucket is %d", ErrBucketDoesNotExist, bucket, g.nrOfBuckets)
	}

	return g.m[bucket].PutMulti(hashes, n)
}

// Set updates the value associated with the given hash in the map.
//
// Params:
//   - hash: The hash to update in the map.
//   - value: The value to associate with the hash.
//
// Returns:
//   - error: An error if the hash does not exist in the map, nil otherwise.
func (g *NativeSplitMap) Set(hash chainhash.Hash, value uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Set(hash, value)
}

// SetIfExists updates the value associated with the given hash in the map if it exists.
// It returns a boolean indicating whether the hash was found and updated.
// If the hash does not exist, it returns false and no error.
//
// Params:
//   - hash: The hash to update in the map.
//   - value: The value to associate with the hash.
//
// Returns:
//   - bool: True if the hash was found and updated, false otherwise.
//   - error: An error if there was an issue updating the hash, nil otherwise.
func (g *NativeSplitMap) SetIfExists(hash chainhash.Hash, value uint64) (bool, error) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].SetIfExists(hash, value)
}

// SetIfNotExists adds the hash with the given value to the map only if the hash does not already exist.
// It returns a boolean indicating whether the hash was added.
// If the hash already exists, it returns false and no error.
//
// Params:
//   - hash: The hash to add to the map.
//   - value: The value to associate with the hash.
//
// Returns:
//   - bool: True if the hash was added, false if it already existed.
//   - error: An error if there was an issue adding the hash, nil otherwise.
func (g *NativeSplitMap) SetIfNotExists(hash chainhash.Hash, value uint64) (bool, error) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].SetIfNotExists(hash, value)
}

// Keys returns a slice of all hashes currently stored in the map.
// It iterates over all buckets and collects the keys from each bucket.
// The order of keys is not guaranteed.
//
// Returns:
//   - []chainhash.Hash: A slice containing all the hashes in the map.
func (g *NativeSplitMap) Keys() []chainhash.Hash {
	keys := make([]chainhash.Hash, 0, g.Length())

	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		keys = append(keys, g.m[i].Keys()...)
	}

	return keys
}

// Length returns the current number of hashes in the map.
// It iterates over all buckets and sums their lengths to get the total count.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
func (g *NativeSplitMap) Length() int {
	length := 0

	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		length += g.m[i].Length()
	}

	return length
}

// Delete removes a hash from the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and checks the corresponding bucket for the hash.
//
// Params:
//   - hash: The hash to remove from the map.
//
// Returns:
//   - error: An error if the hash does not exist in the map or if the bucket does not exist, nil otherwise.
func (g *NativeSplitMap) Delete(hash chainhash.Hash) error {
	bucket := Bytes2Uint16Buckets(hash, g.nrOfBuckets)

	if _, ok := g.m[bucket]; !ok {
		return fmt.Errorf("%w: %d", ErrBucketDoesNotExist, bucket)
	}

	if !g.m[bucket].Exists(hash) {
		return fmt.Errorf("%w in bucket %d: %s", ErrHashDoesNotExist, bucket, hash)
	}

	return g.m[bucket].Delete(hash)
}

// Map returns the underlying map of all buckets used by NativeSplitMap.
//
// Returns:
//   - TxMap: A map where the keys are bucket indices and the values are pointers to NativeMapUint64 instances.
func (g *NativeSplitMap) Map() *NativeMapUint64 {
	m := NewNativeMapUint64(uint32(g.Length())) //nolint:gosec // integer overflow conversion int -> uint32
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		keys := g.m[i].Keys()
		for _, key := range keys {
			val, _ := g.m[i].Get(key)
			_ = m.Put(key, val)
		}
	}

	return m
}

// Iter iterates over all key-value pairs in the map and applies the provided function to each pair.
// Stops iterating if the function returns true.
//
// Params:
//   - f: A function that takes a hash and its associated uint64 value.
func (g *NativeSplitMap) Iter(f func(hash chainhash.Hash, value uint64) bool) {
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Iter(f)
	}
}

// check that NativeSplitMapUint64 implements TxMap
var _ TxMap = (*NativeSplitMapUint64)(nil)

// NativeSplitMapUint64 is a map that splits the data into multiple buckets to reduce contention.
// It uses NativeMapUint64 for each bucket to store the hashes and their associated uint64 values.
// The number of buckets is fixed at 1024, and the length is divided by this number to determine the size of each bucket.
type NativeSplitMapUint64 struct {
	m           map[uint16]*NativeMapUint64
	nrOfBuckets uint16
}

// NewNativeSplitMapUint64 creates a new NativeSplitMapUint64 with the specified initial length.
// The length is used to preallocate the size of each bucket.
// It divides the length by the number of buckets to determine the size of each bucket.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *NativeSplitMapUint64: A pointer to the newly created NativeSplitMapUint64 instance.
func NewNativeSplitMapUint64(length uint32, buckets ...uint16) *NativeSplitMapUint64 {
	useBuckets := uint16(1024)
	if len(buckets) > 0 {
		useBuckets = buckets[0]
	}

	m := &NativeSplitMapUint64{
		m:           make(map[uint16]*NativeMapUint64, useBuckets),
		nrOfBuckets: useBuckets,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewNativeMapUint64(length / uint32(m.nrOfBuckets))
	}

	return m
}

// Exists checks if the given hash exists in the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and checks the corresponding bucket.
//
// Params:
//   - hash: The hash to check for existence in the map.
//
// Returns:
//   - bool: True if the hash exists in the map, false otherwise.
func (g *NativeSplitMapUint64) Exists(hash chainhash.Hash) bool {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Exists(hash)
}

// Map returns the underlying map of buckets used by NativeSplitMapUint64.
//
// Returns:
//   - map[uint16]*NativeMapUint64: A map where the keys are bucket indices and the values are pointers to NativeMapUint64 instances.
func (g *NativeSplitMapUint64) Map() map[uint16]*NativeMapUint64 {
	return g.m
}

// Put adds a new hash with an associated uint64 value to the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and adds the hash to the corresponding bucket.
// It checks if the hash already exists in the bucket and returns an error if it does.
//
// Params:
//   - hash: The hash to add to the map.
//   - n: The uint64 value to associate with the hash.
//
// Returns:
//   - error: An error if the hash already exists in the map, nil otherwise.
func (g *NativeSplitMapUint64) Put(hash chainhash.Hash, n uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n)
}

// PutMulti adds multiple hashes with an associated uint64 value to the map.
// It iterates over the hashes, calculates the bucket index for each hash using the Bytes2Uint16Buckets function,
// and adds each hash to the corresponding bucket.
// It checks if any of the hashes already exist in the bucket and returns an error if any do.
//
// Params:
//   - hashes: A slice of hashes to add to the map.
//   - n: The uint64 value to associate with each hash.
//
// Returns:
//   - error: An error if any of the hashes already exist in the map, nil otherwise.
func (g *NativeSplitMapUint64) PutMulti(hashes []chainhash.Hash, n uint64) error {
	for _, hash := range hashes {
		if err := g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n); err != nil {
			return fmt.Errorf("failed to put multi in bucket %d: %w", Bytes2Uint16Buckets(hash, g.nrOfBuckets), err)
		}
	}

	return nil
}

// Set updates the value associated with the given hash in the map.
// It will error out if the hash does not exist.
//
// Params:
//   - hash: The hash to update in the map.
//   - value: The value to associate with the hash.
//
// Returns:
//   - error: An error if the hash does not exist in the map, nil otherwise.
func (g *NativeSplitMapUint64) Set(hash chainhash.Hash, value uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Set(hash, value)
}

// SetIfExists updates the value associated with the given hash in the map if it exists.
// It returns a boolean indicating whether the hash was found and updated.
// If the hash does not exist, it returns false and no error.
//
// Params:
//   - hash: The hash to update in the map.
//   - value: The value to associate with the hash.
//
// Returns:
//   - bool: True if the hash was found and updated, false otherwise.
//   - error: An error if there was an issue updating the hash, nil otherwise.
func (g *NativeSplitMapUint64) SetIfExists(hash chainhash.Hash, value uint64) (bool, error) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].SetIfExists(hash, value)
}

// SetIfNotExists adds the hash with the given value to the map only if the hash does not already exist.
// It returns a boolean indicating whether the hash was added.
// If the hash already exists, it returns false and no error.
//
// Params:
//   - hash: The hash to add to the map.
//   - value: The value to associate with the hash.
//
// Returns:
//   - bool: True if the hash was added, false if it already existed.
//   - error: An error if there was an issue adding the hash, nil otherwise.
func (g *NativeSplitMapUint64) SetIfNotExists(hash chainhash.Hash, value uint64) (bool, error) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].SetIfNotExists(hash, value)
}

// Get retrieves the uint64 value associated with the given hash from the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and retrieves the value from the corresponding bucket.
//
// Params:
//   - hash: The hash to retrieve from the map.
//
// Returns:
//   - uint64: The value associated with the hash, or 0 if the hash does not exist.
//   - bool: True if the hash was found in the map, false otherwise.
func (g *NativeSplitMapUint64) Get(hash chainhash.Hash) (uint64, bool) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Get(hash)
}

// Iter iterates over all key-value pairs in the map and applies the provided function to each pair.
// Stops iterating if the function returns true.
//
// Params:
//   - f: A function that takes a hash and its associated uint64 value.
func (g *NativeSplitMapUint64) Iter(f func(hash chainhash.Hash, value uint64) bool) {
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Iter(f)
	}
}

// Length returns the current number of hashes in the map.
// It iterates over all buckets and sums their lengths to get the total count.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
func (g *NativeSplitMapUint64) Length() int {
	length := 0
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		length += g.m[i].length
	}

	return length
}

// Delete removes a hash from the map.
// It calculates the bucket index using the Bytes2Uint16Buckets function and checks the corresponding bucket for the hash.
// If the hash does not exist, it returns an error.
//
// Params:
//   - hash: The hash to remove from the map.
//
// Returns:
//   - error: An error if the hash does not exist in the map or if the bucket does not exist, nil otherwise.
func (g *NativeSplitMapUint64) Delete(hash chainhash.Hash) error {
	bucket := Bytes2Uint16Buckets(hash, g.nrOfBuckets)

	if _, ok := g.m[bucket]; !ok {
		return fmt.Errorf("%w: %d", ErrBucketDoesNotExist, bucket)
	}

	if !g.m[bucket].Exists(hash) {
		return fmt.Errorf("%w in bucket %d: %s", ErrHashDoesNotExist, bucket, hash)
	}

	return g.m[bucket].Delete(hash)
}

// Keys returns a slice of all hashes currently stored in the map.
// It iterates over all buckets and collects the keys from each bucket.
// The order of keys is not guaranteed.
//
// Returns:
//   - []chainhash.Hash: A slice containing all the hashes in the map.
func (g *NativeSplitMapUint64) Keys() []chainhash.Hash {
	keys := make([]chainhash.Hash, 0, g.Length())

	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		keys = append(keys, g.m[i].Keys()...)
	}

	return keys
}

// NativeSplitLockFreeMapUint64 is a map that splits the data into multiple buckets to reduce contention.
// It uses NativeLockFreeMapUint64 for each bucket to store the hashes and their associated uint64 values.
type NativeSplitLockFreeMapUint64 struct {
	m           map[uint64]*NativeLockFreeMapUint64
	nrOfBuckets uint64
}

// NewNativeSplitLockFreeMapUint64 creates a new NativeSplitLockFreeMapUint64 with the specified initial length.
// The length is used to preallocate the size of each bucket.
// It divides the length by the number of buckets to determine the size of each bucket.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *NativeSplitLockFreeMapUint64: A pointer to the newly created NativeSplitLockFreeMapUint64 instance.
func NewNativeSplitLockFreeMapUint64(length int, buckets ...uint64) *NativeSplitLockFreeMapUint64 {
	useBuckets := uint64(1024)
	if len(buckets) > 0 {
		useBuckets = buckets[0]
	}

	m := &NativeSplitLockFreeMapUint64{
		m:           make(map[uint64]*NativeLockFreeMapUint64, useBuckets),
		nrOfBuckets: useBuckets,
	}

	for i := uint64(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewNativeLockFreeMapUint64(length / int(m.nrOfBuckets)) //nolint:gosec // integer overflow conversion uint64 -> int
	}

	return m
}

// Exists checks if the given hash exists in the map.
// It calculates the bucket index using the modulo operation and checks the corresponding bucket.
//
// Params:
//   - hash: The hash to check for existence in the map.
//
// Returns:
//   - bool: True if the hash exists in the map, false otherwise.
//
// Considerations: This method does not lock the map, so it is not suitable for concurrent access.
func (g *NativeSplitLockFreeMapUint64) Exists(hash uint64) bool {
	return g.m[hash%g.nrOfBuckets].Exists(hash)
}

// Map returns the underlying map of buckets used by NativeSplitLockFreeMapUint64.
// It provides access to the map for operations that do not require locking.
//
// Returns:
//   - map[uint64]*NativeLockFreeMapUint64: A map where the keys are bucket indices and the values are pointers to NativeLockFreeMapUint64 instances.
//
// Considerations: This method does not lock the map, so it is not suitable for concurrent access.
func (g *NativeSplitLockFreeMapUint64) Map() map[uint64]*NativeLockFreeMapUint64 {
	return g.m
}

// Put adds a new hash with an associated uint64 value to the map.
// It calculates the bucket index using the modulo operation and adds the hash to the corresponding bucket.
// It checks if the hash already exists in the bucket and returns an error if it does.
//
// Params:
//   - hash: The hash to add to the map.
//   - n: The uint64 value to associate with the hash.
//
// Returns:
//   - error: An error if the hash already exists in the map, nil otherwise.
//
// Considerations: This method does not lock the map, so it is not suitable for concurrent access.
func (g *NativeSplitLockFreeMapUint64) Put(hash, n uint64) error {
	return g.m[hash%g.nrOfBuckets].Put(hash, n)
}

// Get retrieves the uint64 value associated with the given hash from the map.
// It calculates the bucket index using the modulo operation and retrieves the value from the corresponding bucket.
//
// Params:
//   - hash: The hash to retrieve from the map.
//
// Returns:
//   - uint64: The value associated with the hash, or 0 if the hash does not exist.
//   - bool: True if the hash was found in the map, false otherwise.
//
// Considerations: This method does not lock the map, so it is not suitable for concurrent access.
func (g *NativeSplitLockFreeMapUint64) Get(hash uint64) (uint64, bool) {
	return g.m[hash%g.nrOfBuckets].Get(hash)
}

// Length returns the current number of hashes in the map.
// It iterates over all buckets and sums their lengths to get the total count.
// It uses atomic operations to ensure thread safety.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
func (g *NativeSplitLockFreeMapUint64) Length() int {
	length := 0
	for i := uint64(0); i <= g.nrOfBuckets; i++ {
		length += int(g.m[i].length.Load())
	}

	return length
}
