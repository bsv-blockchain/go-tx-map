// Package txmap provides alternative implementations using cockroachdb/swiss
// instead of dolthub/swiss for benchmarking purposes.
package txmap

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	crswiss "github.com/cockroachdb/swiss"
)

// CRSwissMap is a simple concurrent-safe map that uses the cockroachdb/swiss package
type CRSwissMap struct {
	mu     sync.RWMutex
	m      *crswiss.Map[chainhash.Hash, struct{}]
	length int
}

// NewCRSwissMap creates a new CRSwissMap with the specified initial length.
// The length is used to preallocate the map size for better performance.
// It is not a hard limit, but a hint to the underlying swiss map.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *CRSwissMap: A pointer to the newly created CRSwissMap instance.
//
// Considerations: The length is not enforced, and the map can grow beyond this size.
func NewCRSwissMap(length uint32) *CRSwissMap {
	return &CRSwissMap{
		m: crswiss.New[chainhash.Hash, struct{}](int(length)),
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
func (s *CRSwissMap) Exists(hash chainhash.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)

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
func (s *CRSwissMap) Get(hash chainhash.Hash) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)

	return 0, ok
}

// Put adds a new hash to the map. It increments the length of the map.
//
// Params:
//   - hash: The hash to add to the map.
//
// Returns:
//   - error: always returns nil, as this map does not have any constraints on adding hashes.
func (s *CRSwissMap) Put(hash chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length++

	s.m.Put(hash, struct{}{})

	return nil
}

// PutMulti adds multiple hashes to the map. It increments the length of the map for each hash added.
//
// Params:
//   - hashes: A slice of hashes to add to the map.
//
// Returns:
//   - error: always returns nil, as this map does not have any constraints on adding hashes.
func (s *CRSwissMap) PutMulti(hashes []chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hash := range hashes {
		s.m.Put(hash, struct{}{})

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
func (s *CRSwissMap) Delete(hash chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length--

	s.m.Delete(hash)

	return nil
}

// Length returns the current number of hashes in the map.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
func (s *CRSwissMap) Length() int {
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
func (s *CRSwissMap) Keys() []chainhash.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]chainhash.Hash, 0, s.length)

	s.m.All(func(k chainhash.Hash, _ struct{}) bool {
		keys = append(keys, k)
		return true // continue iteration
	})

	return keys
}

// Map returns the TxHashMap
func (s *CRSwissMap) Map() TxHashMap {
	return s
}

// Iter iterates over all key-value pairs in the map and applies the provided function to each pair.
// Stops iterating if the function returns true.
//
// Params:
//   - f: A function that takes a hash and its associated value (always 0 in this map).
func (s *CRSwissMap) Iter(f func(hash chainhash.Hash, value uint64) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.m.All(func(k chainhash.Hash, _ struct{}) bool {
		return !f(k, 0) // cockroachdb returns true to continue, dolthub returns true to stop
	})
}

// check that CRSwissMapUint64 implements TxMap
var _ TxMap = (*CRSwissMapUint64)(nil)

// CRSwissMapUint64 is a concurrent-safe map that uses the cockroachdb/swiss package to store
// transaction hashes as keys and uint64 values.
type CRSwissMapUint64 struct {
	mu     sync.RWMutex
	m      *crswiss.Map[chainhash.Hash, uint64]
	length int
}

// NewCRSwissMapUint64 creates a new CRSwissMapUint64 with the specified initial length.
// The length is used to preallocate the map size for better performance.
// It is not a hard limit, but a hint to the underlying swiss map.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *CRSwissMapUint64: A pointer to the newly created CRSwissMapUint64 instance.
func NewCRSwissMapUint64(length uint32) *CRSwissMapUint64 {
	return &CRSwissMapUint64{
		m: crswiss.New[chainhash.Hash, uint64](int(length)),
	}
}

// Map returns the underlying swiss map used by CRSwissMapUint64.
//
// Returns:
//   - *crswiss.Map[chainhash.Hash, uint64]: A pointer to the underlying swiss map.
func (s *CRSwissMapUint64) Map() *crswiss.Map[chainhash.Hash, uint64] {
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
func (s *CRSwissMapUint64) Exists(hash chainhash.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)

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
func (s *CRSwissMapUint64) Put(hash chainhash.Hash, n uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m.Get(hash)
	if exists {
		return fmt.Errorf(errWrapFormat, ErrHashAlreadyExists, hash)
	}

	s.m.Put(hash, n)

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
func (s *CRSwissMapUint64) PutMulti(hashes []chainhash.Hash, n uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hash := range hashes {
		_, exists := s.m.Get(hash)
		if exists {
			return fmt.Errorf(errWrapFormat, ErrHashAlreadyExists, hash)
		}

		s.m.Put(hash, n)

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
func (s *CRSwissMapUint64) Set(hash chainhash.Hash, value uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m.Get(hash)
	if !exists {
		return fmt.Errorf(errWrapFormat, ErrHashDoesNotExist, hash)
	}

	s.m.Put(hash, value)

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
func (s *CRSwissMapUint64) SetIfExists(hash chainhash.Hash, value uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m.Get(hash)
	if !exists {
		return false, nil
	}

	s.m.Put(hash, value)

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
func (s *CRSwissMapUint64) SetIfNotExists(hash chainhash.Hash, value uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m.Get(hash)
	if exists {
		return false, nil
	}

	s.m.Put(hash, value)

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
func (s *CRSwissMapUint64) Get(hash chainhash.Hash) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n, ok := s.m.Get(hash)
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
func (s *CRSwissMapUint64) Length() int {
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
func (s *CRSwissMapUint64) Keys() []chainhash.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]chainhash.Hash, 0, s.length)

	s.m.All(func(k chainhash.Hash, _ uint64) bool {
		keys = append(keys, k)
		return true // continue iteration
	})

	return keys
}

// Iter iterates over all key-value pairs in the map and applies the provided function to each pair.
// Stops iterating if the function returns true.
//
// Params:
//   - f: A function that takes a hash and its associated uint64 value.
func (s *CRSwissMapUint64) Iter(f func(hash chainhash.Hash, value uint64) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.m.All(func(k chainhash.Hash, v uint64) bool {
		return !f(k, v) // cockroachdb returns true to continue, dolthub returns true to stop
	})
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
func (s *CRSwissMapUint64) Delete(hash chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m.Get(hash)
	if !exists {
		return fmt.Errorf("%w: %s", ErrHashDoesNotExist, hash)
	}

	s.m.Delete(hash)

	s.length--

	return nil
}

// CRSwissLockFreeMapUint64 is a lock-free map for uint64 keys and values
type CRSwissLockFreeMapUint64 struct {
	m      *crswiss.Map[uint64, uint64]
	length atomic.Uint32
}

// NewCRSwissLockFreeMapUint64 creates a new CRSwissLockFreeMapUint64 with the specified initial length.
// The length is used to preallocate the map size for better performance.
// It is not a hard limit, but a hint to the underlying swiss map.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *CRSwissLockFreeMapUint64: A pointer to the newly created CRSwissLockFreeMapUint64 instance.
func NewCRSwissLockFreeMapUint64(length int) *CRSwissLockFreeMapUint64 {
	return &CRSwissLockFreeMapUint64{
		m:      crswiss.New[uint64, uint64](length),
		length: atomic.Uint32{},
	}
}

// Map returns the underlying swiss map used by CRSwissLockFreeMapUint64.
// It provides access to the map for operations that do not require locking.
//
// Returns:
//   - *crswiss.Map[uint64, uint64]: A pointer to the underlying swiss map.
//
// Considerations: This method does not lock the map, so it is not suitable for concurrent access.
func (s *CRSwissLockFreeMapUint64) Map() *crswiss.Map[uint64, uint64] {
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
func (s *CRSwissLockFreeMapUint64) Exists(hash uint64) bool {
	_, ok := s.m.Get(hash)
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
func (s *CRSwissLockFreeMapUint64) Put(hash, n uint64) error {
	_, exists := s.m.Get(hash)
	if exists {
		return ErrHashAlreadyExists
	}

	s.m.Put(hash, n)
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
func (s *CRSwissLockFreeMapUint64) Get(hash uint64) (uint64, bool) {
	n, ok := s.m.Get(hash)
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
func (s *CRSwissLockFreeMapUint64) Length() int {
	return int(s.length.Load())
}

// check that CRSplitSwissMap implements TxMap
var _ TxMap = (*CRSplitSwissMap)(nil)

// CRSplitSwissMap is a map that splits the data into multiple buckets to reduce contention.
// It uses CRSwissMapUint64 for each bucket to store the hashes and their associated uint64 values.
// Since CRSwissMapUint64 is concurrent-safe, CRSplitSwissMap can handle concurrent access without additional locks.
type CRSplitSwissMap struct {
	m           map[uint16]*CRSwissMapUint64
	nrOfBuckets uint16
}

// NewCRSplitSwissMap creates a new CRSplitSwissMap with the specified initial length.
// The length is used to preallocate the size of each bucket.
// It divides the length by the number of buckets to determine the size of each bucket.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *CRSplitSwissMap: A pointer to the newly created CRSplitSwissMap instance.
//
// Considerations: The number of buckets is fixed at 1024, and the length is divided by this number to determine the size of each bucket.
func NewCRSplitSwissMap(length int, buckets ...uint16) *CRSplitSwissMap {
	useBuckets := uint16(1024)
	if len(buckets) > 0 {
		useBuckets = buckets[0]
	}

	m := &CRSplitSwissMap{
		m:           make(map[uint16]*CRSwissMapUint64, useBuckets),
		nrOfBuckets: useBuckets,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewCRSwissMapUint64(uint32(math.Ceil(float64(length) / float64(m.nrOfBuckets))))
	}

	return m
}

// Buckets returns the number of buckets in the CRSplitSwissMap.
func (g *CRSplitSwissMap) Buckets() uint16 {
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
func (g *CRSplitSwissMap) Exists(hash chainhash.Hash) bool {
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
func (g *CRSplitSwissMap) Get(hash chainhash.Hash) (uint64, bool) {
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
func (g *CRSplitSwissMap) Put(hash chainhash.Hash, n uint64) error {
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
func (g *CRSplitSwissMap) PutMulti(hashes []chainhash.Hash, n uint64) (err error) {
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
func (g *CRSplitSwissMap) PutMultiBucket(bucket uint16, hashes []chainhash.Hash, n uint64) error {
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
func (g *CRSplitSwissMap) Set(hash chainhash.Hash, value uint64) error {
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
func (g *CRSplitSwissMap) SetIfExists(hash chainhash.Hash, value uint64) (bool, error) {
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
func (g *CRSplitSwissMap) SetIfNotExists(hash chainhash.Hash, value uint64) (bool, error) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].SetIfNotExists(hash, value)
}

// Keys returns a slice of all hashes currently stored in the map.
// It iterates over all buckets and collects the keys from each bucket.
// The order of keys is not guaranteed.
//
// Returns:
//   - []chainhash.Hash: A slice containing all the hashes in the map.
func (g *CRSplitSwissMap) Keys() []chainhash.Hash {
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
func (g *CRSplitSwissMap) Length() int {
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
func (g *CRSplitSwissMap) Delete(hash chainhash.Hash) error {
	bucket := Bytes2Uint16Buckets(hash, g.nrOfBuckets)

	if _, ok := g.m[bucket]; !ok {
		return fmt.Errorf("%w: %d", ErrBucketDoesNotExist, bucket)
	}

	if !g.m[bucket].Exists(hash) {
		return fmt.Errorf("%w in bucket %d: %s", ErrHashDoesNotExist, bucket, hash)
	}

	return g.m[bucket].Delete(hash)
}

// Map returns the underlying map of all buckets used by CRSplitSwissMap.
//
// Returns:
//   - TxMap: A map where the keys are bucket indices and the values are pointers to CRSwissMapUint64 instances.
func (g *CRSplitSwissMap) Map() *CRSwissMapUint64 {
	m := NewCRSwissMapUint64(uint32(g.Length())) //nolint:gosec // integer overflow conversion int -> uint32
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
func (g *CRSplitSwissMap) Iter(f func(hash chainhash.Hash, value uint64) bool) {
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Iter(f)
	}
}

// check that CRSplitSwissMapUint64 implements TxMap
var _ TxMap = (*CRSplitSwissMapUint64)(nil)

// CRSplitSwissMapUint64 is a map that splits the data into multiple buckets to reduce contention.
// It uses CRSwissMapUint64 for each bucket to store the hashes and their associated uint64 values.
// The number of buckets is fixed at 1024, and the length is divided by this number to determine the size of each bucket.
type CRSplitSwissMapUint64 struct {
	m           map[uint16]*CRSwissMapUint64
	nrOfBuckets uint16
}

// NewCRSplitSwissMapUint64 creates a new CRSplitSwissMapUint64 with the specified initial length.
// The length is used to preallocate the size of each bucket.
// It divides the length by the number of buckets to determine the size of each bucket.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *CRSplitSwissMapUint64: A pointer to the newly created CRSplitSwissMapUint64 instance.
func NewCRSplitSwissMapUint64(length uint32, buckets ...uint16) *CRSplitSwissMapUint64 {
	useBuckets := uint16(1024)
	if len(buckets) > 0 {
		useBuckets = buckets[0]
	}

	m := &CRSplitSwissMapUint64{
		m:           make(map[uint16]*CRSwissMapUint64, useBuckets),
		nrOfBuckets: useBuckets,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewCRSwissMapUint64(length / uint32(m.nrOfBuckets))
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
func (g *CRSplitSwissMapUint64) Exists(hash chainhash.Hash) bool {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Exists(hash)
}

// Map returns the underlying map of buckets used by CRSplitSwissMapUint64.
//
// Returns:
//   - map[uint16]*CRSwissMapUint64: A map where the keys are bucket indices and the values are pointers to CRSwissMapUint64 instances.
func (g *CRSplitSwissMapUint64) Map() map[uint16]*CRSwissMapUint64 {
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
func (g *CRSplitSwissMapUint64) Put(hash chainhash.Hash, n uint64) error {
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
func (g *CRSplitSwissMapUint64) PutMulti(hashes []chainhash.Hash, n uint64) error {
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
func (g *CRSplitSwissMapUint64) Set(hash chainhash.Hash, value uint64) error {
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
func (g *CRSplitSwissMapUint64) SetIfExists(hash chainhash.Hash, value uint64) (bool, error) {
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
func (g *CRSplitSwissMapUint64) SetIfNotExists(hash chainhash.Hash, value uint64) (bool, error) {
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
func (g *CRSplitSwissMapUint64) Get(hash chainhash.Hash) (uint64, bool) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Get(hash)
}

// Iter iterates over all key-value pairs in the map and applies the provided function to each pair.
// Stops iterating if the function returns true.
//
// Params:
//   - f: A function that takes a hash and its associated uint64 value.
func (g *CRSplitSwissMapUint64) Iter(f func(hash chainhash.Hash, value uint64) bool) {
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Iter(f)
	}
}

// Length returns the current number of hashes in the map.
// It iterates over all buckets and sums their lengths to get the total count.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
func (g *CRSplitSwissMapUint64) Length() int {
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
func (g *CRSplitSwissMapUint64) Delete(hash chainhash.Hash) error {
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
func (g *CRSplitSwissMapUint64) Keys() []chainhash.Hash {
	keys := make([]chainhash.Hash, 0, g.Length())

	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		keys = append(keys, g.m[i].Keys()...)
	}

	return keys
}

// CRSplitSwissLockFreeMapUint64 is a map that splits the data into multiple buckets to reduce contention.
// It uses CRSwissLockFreeMapUint64 for each bucket to store the hashes and their associated uint64 values.
type CRSplitSwissLockFreeMapUint64 struct {
	m           map[uint64]*CRSwissLockFreeMapUint64
	nrOfBuckets uint64
}

// NewCRSplitSwissLockFreeMapUint64 creates a new CRSplitSwissLockFreeMapUint64 with the specified initial length.
// The length is used to preallocate the size of each bucket.
// It divides the length by the number of buckets to determine the size of each bucket.
//
// Params:
//   - length: The initial length of the map, used for preallocation.
//
// Returns:
//   - *CRSplitSwissLockFreeMapUint64: A pointer to the newly created CRSplitSwissLockFreeMapUint64 instance.
func NewCRSplitSwissLockFreeMapUint64(length int, buckets ...uint64) *CRSplitSwissLockFreeMapUint64 {
	useBuckets := uint64(1024)
	if len(buckets) > 0 {
		useBuckets = buckets[0]
	}

	m := &CRSplitSwissLockFreeMapUint64{
		m:           make(map[uint64]*CRSwissLockFreeMapUint64, useBuckets),
		nrOfBuckets: useBuckets,
	}

	for i := uint64(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewCRSwissLockFreeMapUint64(length / int(m.nrOfBuckets)) //nolint:gosec // integer overflow conversion uint64 -> int
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
func (g *CRSplitSwissLockFreeMapUint64) Exists(hash uint64) bool {
	return g.m[hash%g.nrOfBuckets].Exists(hash)
}

// Map returns the underlying map of buckets used by CRSplitSwissLockFreeMapUint64.
// It provides access to the map for operations that do not require locking.
//
// Returns:
//   - map[uint64]*CRSwissLockFreeMapUint64: A map where the keys are bucket indices and the values are pointers to CRSwissLockFreeMapUint64 instances.
//
// Considerations: This method does not lock the map, so it is not suitable for concurrent access.
func (g *CRSplitSwissLockFreeMapUint64) Map() map[uint64]*CRSwissLockFreeMapUint64 {
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
func (g *CRSplitSwissLockFreeMapUint64) Put(hash, n uint64) error {
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
func (g *CRSplitSwissLockFreeMapUint64) Get(hash uint64) (uint64, bool) {
	return g.m[hash%g.nrOfBuckets].Get(hash)
}

// Length returns the current number of hashes in the map.
// It iterates over all buckets and sums their lengths to get the total count.
// It uses atomic operations to ensure thread safety.
//
// Returns:
//   - int: The number of hashes currently stored in the map.
func (g *CRSplitSwissLockFreeMapUint64) Length() int {
	length := 0
	for i := uint64(0); i <= g.nrOfBuckets; i++ {
		length += int(g.m[i].length.Load())
	}

	return length
}
