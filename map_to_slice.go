// Package txmap provides a set of concurrent-safe data structures and utilities
// for managing mappings and collections with high-performance requirements.
// The package leverages Go's sync package and other advanced concurrency
// primitives such as lock-free techniques where applicable.
//
// Core Features:
//
//  1. **Thread-safe Maps**: Flexible implementations of maps with read-write mutex synchronization
//     for thread-safety in multi-threaded environments.
//
//     - **SyncedMap**: A generic concurrent-safe map with optional size limits.
//     - **SwissMap**: A simple concurrent-safe map based on the swiss.Map library,
//     designed to store transaction hashes or other key-value mappings efficiently.
//     - **SwissMapUint64**: A variation of SwissMap for transaction hashes associated
//     with `uint64` values.
//     - **SwissLockFreeMapUint64**: A specialized lock-free map for `uint64` keys and values,
//     offering better performance for certain scenarios.
//
//  2. **Synced Slice**: A thread-safe wrapper around slices, allowing for synchronized
//     access and updates. Useful for managing shared lists in concurrent code.
//
//  3. **Split Bucket Maps**: Advanced sharding technique for reducing contention by splitting
//     data into multiple buckets (e.g., SplitSwissMap).
//     - Buckets minimize lock contention by distributing keys across multiple synchronized maps.
//
//  4. **Utilities for Map Conversion**: Helper functions to convert various map data
//     structures into slices, making data extraction and iteration easier.
//
// Design Considerations:
//   - **Locking Mechanisms**: Where necessary, the maps use read-write locks for consistent
//     reads and writes, while minimizing the lock duration for better performance.
//   - **Limit Controls**: Maps such as SyncedMap optionally provide control over the maximum
//     number of items stored, ensuring that memory usage is kept in check.
//   - **High Concurrency**: Certain implementations, like SwissLockFreeMapUint64, are lock-free
//     for a subset of their operations, promoting better scalability in concurrent workloads.
//   - **Preallocation**: Many data structures accept initialization parameters for preallocating
//     internal storage, reducing runtime overhead from frequent allocations.
//
// Usage Scenarios:
// The package is suitable for tasks requiring efficient handling of:
// - Large-scale transaction mappings (e.g., blockchain transaction hash maps).
// - Concurrent key-value access and modifications under high contention.
// - Specialized locking or predictive space management for performance-critical applications.
//
// Examples:
// - Managing transaction hash lookups and metadata in high-frequency trading systems.
// - Concurrent-safe configuration or cache management in distributed services.
// - Utility for parallel data aggregation or transformation of shared resources.
//
// Dependencies:
// The package depends on the [`swiss`](https://github.com/dolthub/swiss) library and
// additionally uses the `chainhash` library (`github.com/libsv/go-bt/v2/chainhash`) where applicable.
package txmap

import (
	"sync"
)

// ConvertSyncMapToUint32Slice transforms a *sync.Map of uint32 keys into a slice of uint32 values and returns if it was non-empty.
// It iterates over the sync.Map keys, appending them to a uint32 slice, and determines if the map contained elements.
func ConvertSyncMapToUint32Slice(syncMap *sync.Map) ([]uint32, bool) {
	var sliceWithMapElements []uint32

	mapHasAnyElements := false

	syncMap.Range(func(key, _ interface{}) bool {
		mapHasAnyElements = true
		val := key.(uint32)
		sliceWithMapElements = append(sliceWithMapElements, val)

		return true
	})

	return sliceWithMapElements, mapHasAnyElements
}

// ConvertSyncedMapToUint32Slice transforms the values of a SyncedMap into a single uint32 slice and checks if the map is non-empty.
func ConvertSyncedMapToUint32Slice[K comparable](syncMap *SyncedMap[K, []uint32]) ([]uint32, bool) {
	var sliceWithMapElements []uint32

	mapHasAnyElements := false

	syncMap.Iterate(func(_ K, val []uint32) bool {
		mapHasAnyElements = true

		sliceWithMapElements = append(sliceWithMapElements, val...)

		return true
	})

	return sliceWithMapElements, mapHasAnyElements
}
