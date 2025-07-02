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
