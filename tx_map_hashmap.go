// Package txmap provides alternative implementations using tidwall/hashmap
// (robin hood hashing with xxh3) for benchmarking purposes.
package txmap

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/tidwall/hashmap"
)

// TidwallMap is a simple concurrent-safe map that uses tidwall/hashmap
type TidwallMap struct {
	mu     sync.RWMutex
	m      *hashmap.Map[chainhash.Hash, struct{}]
	length int
}

// NewTidwallMap creates a new TidwallMap with the specified initial length.
func NewTidwallMap(length uint32) *TidwallMap {
	return &TidwallMap{
		m: &hashmap.Map[chainhash.Hash, struct{}]{},
	}
}

// Exists checks if the given hash exists in the map.
func (s *TidwallMap) Exists(hash chainhash.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)
	return ok
}

// Get retrieves the value associated with the given hash from the map.
func (s *TidwallMap) Get(hash chainhash.Hash) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)
	return 0, ok
}

// Put adds a new hash to the map.
func (s *TidwallMap) Put(hash chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length++
	s.m.Set(hash, struct{}{})
	return nil
}

// PutMulti adds multiple hashes to the map.
func (s *TidwallMap) PutMulti(hashes []chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hash := range hashes {
		s.m.Set(hash, struct{}{})
		s.length++
	}
	return nil
}

// Delete removes a hash from the map.
func (s *TidwallMap) Delete(hash chainhash.Hash) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.length--
	s.m.Delete(hash)
	return nil
}

// Length returns the current number of hashes in the map.
func (s *TidwallMap) Length() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.length
}

// Keys returns a slice of all hashes currently stored in the map.
func (s *TidwallMap) Keys() []chainhash.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]chainhash.Hash, 0, s.length)
	s.m.Scan(func(k chainhash.Hash, v struct{}) bool {
		keys = append(keys, k)
		return true
	})
	return keys
}

// Map returns the TxHashMap
func (s *TidwallMap) Map() TxHashMap {
	return s
}

// Iter iterates over all key-value pairs in the map.
func (s *TidwallMap) Iter(f func(hash chainhash.Hash, value uint64) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.m.Scan(func(k chainhash.Hash, v struct{}) bool {
		return !f(k, 0)
	})
}

// check that TidwallMapUint64 implements TxMap
var _ TxMap = (*TidwallMapUint64)(nil)

// TidwallMapUint64 is a concurrent-safe map that uses tidwall/hashmap to store
// transaction hashes as keys and uint64 values.
type TidwallMapUint64 struct {
	mu     sync.RWMutex
	m      *hashmap.Map[chainhash.Hash, uint64]
	length int
}

// NewTidwallMapUint64 creates a new TidwallMapUint64 with the specified initial length.
func NewTidwallMapUint64(length uint32) *TidwallMapUint64 {
	return &TidwallMapUint64{
		m: &hashmap.Map[chainhash.Hash, uint64]{},
	}
}

// Map returns the underlying hashmap used by TidwallMapUint64.
func (s *TidwallMapUint64) Map() *hashmap.Map[chainhash.Hash, uint64] {
	return s.m
}

// Exists checks if the given hash exists in the map.
func (s *TidwallMapUint64) Exists(hash chainhash.Hash) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	_, ok := s.m.Get(hash)
	return ok
}

// Put adds a new hash with an associated uint64 value to the map.
func (s *TidwallMapUint64) Put(hash chainhash.Hash, n uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m.Get(hash)
	if exists {
		return fmt.Errorf(errWrapFormat, ErrHashAlreadyExists, hash)
	}

	s.m.Set(hash, n)
	s.length++
	return nil
}

// PutMulti adds multiple hashes with an associated uint64 value to the map.
func (s *TidwallMapUint64) PutMulti(hashes []chainhash.Hash, n uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, hash := range hashes {
		_, exists := s.m.Get(hash)
		if exists {
			return fmt.Errorf(errWrapFormat, ErrHashAlreadyExists, hash)
		}

		s.m.Set(hash, n)
		s.length++
	}
	return nil
}

// Set updates the value associated with the given hash in the map.
func (s *TidwallMapUint64) Set(hash chainhash.Hash, value uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m.Get(hash)
	if !exists {
		return fmt.Errorf(errWrapFormat, ErrHashDoesNotExist, hash)
	}

	s.m.Set(hash, value)
	return nil
}

// SetIfExists updates the value associated with the given hash in the map if it exists.
func (s *TidwallMapUint64) SetIfExists(hash chainhash.Hash, value uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m.Get(hash)
	if !exists {
		return false, nil
	}

	s.m.Set(hash, value)
	return true, nil
}

// SetIfNotExists adds the hash with the given value to the map only if the hash does not already exist.
func (s *TidwallMapUint64) SetIfNotExists(hash chainhash.Hash, value uint64) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, exists := s.m.Get(hash)
	if exists {
		return false, nil
	}

	s.m.Set(hash, value)
	s.length++
	return true, nil
}

// Get retrieves the uint64 value associated with the given hash from the map.
func (s *TidwallMapUint64) Get(hash chainhash.Hash) (uint64, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	n, ok := s.m.Get(hash)
	if !ok {
		return 0, false
	}
	return n, true
}

// Length returns the current number of hashes in the map.
func (s *TidwallMapUint64) Length() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.length
}

// Keys returns a slice of all hashes currently stored in the map.
func (s *TidwallMapUint64) Keys() []chainhash.Hash {
	s.mu.RLock()
	defer s.mu.RUnlock()

	keys := make([]chainhash.Hash, 0, s.length)
	s.m.Scan(func(k chainhash.Hash, v uint64) bool {
		keys = append(keys, k)
		return true
	})
	return keys
}

// Iter iterates over all key-value pairs in the map.
func (s *TidwallMapUint64) Iter(f func(hash chainhash.Hash, value uint64) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	s.m.Scan(func(k chainhash.Hash, v uint64) bool {
		return !f(k, v)
	})
}

// Delete removes a hash from the map.
func (s *TidwallMapUint64) Delete(hash chainhash.Hash) error {
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

// TidwallLockFreeMapUint64 is a lock-free map for uint64 keys and values
type TidwallLockFreeMapUint64 struct {
	m      *hashmap.Map[uint64, uint64]
	length atomic.Uint32
}

// NewTidwallLockFreeMapUint64 creates a new TidwallLockFreeMapUint64.
func NewTidwallLockFreeMapUint64(length int) *TidwallLockFreeMapUint64 {
	return &TidwallLockFreeMapUint64{
		m:      &hashmap.Map[uint64, uint64]{},
		length: atomic.Uint32{},
	}
}

// Map returns the underlying hashmap.
func (s *TidwallLockFreeMapUint64) Map() *hashmap.Map[uint64, uint64] {
	return s.m
}

// Exists checks if the given hash exists in the map.
func (s *TidwallLockFreeMapUint64) Exists(hash uint64) bool {
	_, ok := s.m.Get(hash)
	return ok
}

// Put adds a new hash with an associated uint64 value to the map.
func (s *TidwallLockFreeMapUint64) Put(hash, n uint64) error {
	_, exists := s.m.Get(hash)
	if exists {
		return ErrHashAlreadyExists
	}

	s.m.Set(hash, n)
	s.length.Add(1)
	return nil
}

// Get retrieves the uint64 value associated with the given hash from the map.
func (s *TidwallLockFreeMapUint64) Get(hash uint64) (uint64, bool) {
	n, ok := s.m.Get(hash)
	if !ok {
		return 0, false
	}
	return n, true
}

// Length returns the current number of hashes in the map.
func (s *TidwallLockFreeMapUint64) Length() int {
	return int(s.length.Load())
}

// check that TidwallSplitMap implements TxMap
var _ TxMap = (*TidwallSplitMap)(nil)

// TidwallSplitMap is a map that splits the data into multiple buckets to reduce contention.
// It uses TidwallMapUint64 for each bucket.
type TidwallSplitMap struct {
	m           map[uint16]*TidwallMapUint64
	nrOfBuckets uint16
}

// NewTidwallSplitMap creates a new TidwallSplitMap with the specified initial length.
func NewTidwallSplitMap(length int, buckets ...uint16) *TidwallSplitMap {
	useBuckets := uint16(1024)
	if len(buckets) > 0 {
		useBuckets = buckets[0]
	}

	m := &TidwallSplitMap{
		m:           make(map[uint16]*TidwallMapUint64, useBuckets),
		nrOfBuckets: useBuckets,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewTidwallMapUint64(uint32(math.Ceil(float64(length) / float64(m.nrOfBuckets))))
	}

	return m
}

// Buckets returns the number of buckets in the TidwallSplitMap.
func (g *TidwallSplitMap) Buckets() uint16 {
	return g.nrOfBuckets
}

// Exists checks if the given hash exists in the map.
func (g *TidwallSplitMap) Exists(hash chainhash.Hash) bool {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Exists(hash)
}

// Get retrieves the uint64 value associated with the given hash from the map.
func (g *TidwallSplitMap) Get(hash chainhash.Hash) (uint64, bool) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Get(hash)
}

// Put adds a new hash with an associated uint64 value to the map.
func (g *TidwallSplitMap) Put(hash chainhash.Hash, n uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n)
}

// PutMulti adds multiple hashes with an associated uint64 value to the map.
func (g *TidwallSplitMap) PutMulti(hashes []chainhash.Hash, n uint64) (err error) {
	for _, hash := range hashes {
		if err = g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n); err != nil {
			return fmt.Errorf("failed to put multi in bucket %d: %w", Bytes2Uint16Buckets(hash, g.nrOfBuckets), err)
		}
	}
	return nil
}

// PutMultiBucket adds multiple hashes with an associated uint64 value to a specific bucket.
func (g *TidwallSplitMap) PutMultiBucket(bucket uint16, hashes []chainhash.Hash, n uint64) error {
	if bucket > g.nrOfBuckets {
		return fmt.Errorf("%w: %d, max bucket is %d", ErrBucketDoesNotExist, bucket, g.nrOfBuckets)
	}
	return g.m[bucket].PutMulti(hashes, n)
}

// Set updates the value associated with the given hash in the map.
func (g *TidwallSplitMap) Set(hash chainhash.Hash, value uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Set(hash, value)
}

// SetIfExists updates the value associated with the given hash in the map if it exists.
func (g *TidwallSplitMap) SetIfExists(hash chainhash.Hash, value uint64) (bool, error) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].SetIfExists(hash, value)
}

// SetIfNotExists adds the hash with the given value to the map only if the hash does not already exist.
func (g *TidwallSplitMap) SetIfNotExists(hash chainhash.Hash, value uint64) (bool, error) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].SetIfNotExists(hash, value)
}

// Keys returns a slice of all hashes currently stored in the map.
func (g *TidwallSplitMap) Keys() []chainhash.Hash {
	keys := make([]chainhash.Hash, 0, g.Length())
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		keys = append(keys, g.m[i].Keys()...)
	}
	return keys
}

// Length returns the current number of hashes in the map.
func (g *TidwallSplitMap) Length() int {
	length := 0
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		length += g.m[i].Length()
	}
	return length
}

// Delete removes a hash from the map.
func (g *TidwallSplitMap) Delete(hash chainhash.Hash) error {
	bucket := Bytes2Uint16Buckets(hash, g.nrOfBuckets)

	if _, ok := g.m[bucket]; !ok {
		return fmt.Errorf("%w: %d", ErrBucketDoesNotExist, bucket)
	}

	if !g.m[bucket].Exists(hash) {
		return fmt.Errorf("%w in bucket %d: %s", ErrHashDoesNotExist, bucket, hash)
	}

	return g.m[bucket].Delete(hash)
}

// Map returns the underlying map of all buckets.
func (g *TidwallSplitMap) Map() *TidwallMapUint64 {
	m := NewTidwallMapUint64(uint32(g.Length())) //nolint:gosec // integer overflow conversion int -> uint32
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		keys := g.m[i].Keys()
		for _, key := range keys {
			val, _ := g.m[i].Get(key)
			_ = m.Put(key, val)
		}
	}
	return m
}

// Iter iterates over all key-value pairs in the map.
func (g *TidwallSplitMap) Iter(f func(hash chainhash.Hash, value uint64) bool) {
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Iter(f)
	}
}

// check that TidwallSplitMapUint64 implements TxMap
var _ TxMap = (*TidwallSplitMapUint64)(nil)

// TidwallSplitMapUint64 is a map that splits the data into multiple buckets to reduce contention.
// It uses TidwallMapUint64 for each bucket.
type TidwallSplitMapUint64 struct {
	m           map[uint16]*TidwallMapUint64
	nrOfBuckets uint16
}

// NewTidwallSplitMapUint64 creates a new TidwallSplitMapUint64 with the specified initial length.
func NewTidwallSplitMapUint64(length uint32, buckets ...uint16) *TidwallSplitMapUint64 {
	useBuckets := uint16(1024)
	if len(buckets) > 0 {
		useBuckets = buckets[0]
	}

	m := &TidwallSplitMapUint64{
		m:           make(map[uint16]*TidwallMapUint64, useBuckets),
		nrOfBuckets: useBuckets,
	}

	for i := uint16(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewTidwallMapUint64(length / uint32(m.nrOfBuckets))
	}

	return m
}

// Exists checks if the given hash exists in the map.
func (g *TidwallSplitMapUint64) Exists(hash chainhash.Hash) bool {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Exists(hash)
}

// Map returns the underlying map of buckets.
func (g *TidwallSplitMapUint64) Map() map[uint16]*TidwallMapUint64 {
	return g.m
}

// Put adds a new hash with an associated uint64 value to the map.
func (g *TidwallSplitMapUint64) Put(hash chainhash.Hash, n uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n)
}

// PutMulti adds multiple hashes with an associated uint64 value to the map.
func (g *TidwallSplitMapUint64) PutMulti(hashes []chainhash.Hash, n uint64) error {
	for _, hash := range hashes {
		if err := g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Put(hash, n); err != nil {
			return fmt.Errorf("failed to put multi in bucket %d: %w", Bytes2Uint16Buckets(hash, g.nrOfBuckets), err)
		}
	}
	return nil
}

// Set updates the value associated with the given hash in the map.
func (g *TidwallSplitMapUint64) Set(hash chainhash.Hash, value uint64) error {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Set(hash, value)
}

// SetIfExists updates the value associated with the given hash in the map if it exists.
func (g *TidwallSplitMapUint64) SetIfExists(hash chainhash.Hash, value uint64) (bool, error) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].SetIfExists(hash, value)
}

// SetIfNotExists adds the hash with the given value to the map only if the hash does not already exist.
func (g *TidwallSplitMapUint64) SetIfNotExists(hash chainhash.Hash, value uint64) (bool, error) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].SetIfNotExists(hash, value)
}

// Get retrieves the uint64 value associated with the given hash from the map.
func (g *TidwallSplitMapUint64) Get(hash chainhash.Hash) (uint64, bool) {
	return g.m[Bytes2Uint16Buckets(hash, g.nrOfBuckets)].Get(hash)
}

// Iter iterates over all key-value pairs in the map.
func (g *TidwallSplitMapUint64) Iter(f func(hash chainhash.Hash, value uint64) bool) {
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Iter(f)
	}
}

// Length returns the current number of hashes in the map.
func (g *TidwallSplitMapUint64) Length() int {
	length := 0
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		length += g.m[i].length
	}
	return length
}

// Delete removes a hash from the map.
func (g *TidwallSplitMapUint64) Delete(hash chainhash.Hash) error {
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
func (g *TidwallSplitMapUint64) Keys() []chainhash.Hash {
	keys := make([]chainhash.Hash, 0, g.Length())
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		keys = append(keys, g.m[i].Keys()...)
	}
	return keys
}

// TidwallSplitLockFreeMapUint64 is a map that splits the data into multiple buckets to reduce contention.
// It uses TidwallLockFreeMapUint64 for each bucket.
type TidwallSplitLockFreeMapUint64 struct {
	m           map[uint64]*TidwallLockFreeMapUint64
	nrOfBuckets uint64
}

// NewTidwallSplitLockFreeMapUint64 creates a new TidwallSplitLockFreeMapUint64.
func NewTidwallSplitLockFreeMapUint64(length int, buckets ...uint64) *TidwallSplitLockFreeMapUint64 {
	useBuckets := uint64(1024)
	if len(buckets) > 0 {
		useBuckets = buckets[0]
	}

	m := &TidwallSplitLockFreeMapUint64{
		m:           make(map[uint64]*TidwallLockFreeMapUint64, useBuckets),
		nrOfBuckets: useBuckets,
	}

	for i := uint64(0); i <= m.nrOfBuckets; i++ {
		m.m[i] = NewTidwallLockFreeMapUint64(length / int(m.nrOfBuckets)) //nolint:gosec // integer overflow conversion uint64 -> int
	}

	return m
}

// Exists checks if the given hash exists in the map.
func (g *TidwallSplitLockFreeMapUint64) Exists(hash uint64) bool {
	return g.m[hash%g.nrOfBuckets].Exists(hash)
}

// Map returns the underlying map of buckets.
func (g *TidwallSplitLockFreeMapUint64) Map() map[uint64]*TidwallLockFreeMapUint64 {
	return g.m
}

// Put adds a new hash with an associated uint64 value to the map.
func (g *TidwallSplitLockFreeMapUint64) Put(hash, n uint64) error {
	return g.m[hash%g.nrOfBuckets].Put(hash, n)
}

// Get retrieves the uint64 value associated with the given hash from the map.
func (g *TidwallSplitLockFreeMapUint64) Get(hash uint64) (uint64, bool) {
	return g.m[hash%g.nrOfBuckets].Get(hash)
}

// Length returns the current number of hashes in the map.
func (g *TidwallSplitLockFreeMapUint64) Length() int {
	length := 0
	for i := uint64(0); i <= g.nrOfBuckets; i++ {
		length += int(g.m[i].length.Load())
	}
	return length
}
