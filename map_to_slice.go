package txmap

import (
	"sync"
)

// ConvertSyncMapToUint32Slice returns a slice of uint32 keys from the provided *sync.Map.
//
// Parameters:
//   - syncMap: A pointer to a sync.Map where keys are of type uint32.
//
// Returns:
//   - []uint32: A slice containing all uint32 keys from the map.
//   - bool: True if the map contained any elements, false otherwise.
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

// ConvertSyncedMapToUint32Slice returns a slice of all uint32 values from the provided SyncedMap.
//
// Parameters:
//   - syncMap: A pointer to a SyncedMap with any comparable key type and []uint32 values.
//
// Returns:
//   - []uint32: A slice containing all uint32 values from the map (flattened).
//   - bool: True if the map contained any elements, false otherwise.
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
