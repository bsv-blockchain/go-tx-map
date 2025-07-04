package txmap

import (
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConvertSyncMapToUint32Slice tests the conversion of a sync.Map to a slice of uint32.
func TestConvertSyncMapToUint32Slice(t *testing.T) {
	t.Run("Empty map", func(t *testing.T) {
		var oldBlockIDs sync.Map
		result, hasTransactions := ConvertSyncMapToUint32Slice(&oldBlockIDs)
		assert.Empty(t, result)
		assert.False(t, hasTransactions)
	})

	t.Run("Non-empty map", func(t *testing.T) {
		var oldBlockIDs sync.Map

		oldBlockIDs.Store(uint32(1), struct{}{})
		oldBlockIDs.Store(uint32(2), struct{}{})
		oldBlockIDs.Store(uint32(3), struct{}{})

		result, hasTransactions := ConvertSyncMapToUint32Slice(&oldBlockIDs)
		assert.ElementsMatch(t, []uint32{1, 2, 3}, result)
		assert.True(t, hasTransactions)
	})
}

// TestGenericConvertSyncMapToUint32Slice tests the conversion of a generic synced map to a slice of uint32.
func TestGenericConvertSyncMapToUint32Slice(t *testing.T) {
	t.Run("Empty map", func(t *testing.T) {
		oldBlockIDs := NewSyncedMap[int, []uint32]()
		result, hasTransactions := ConvertSyncedMapToUint32Slice[int](oldBlockIDs)
		assert.Empty(t, result)
		assert.False(t, hasTransactions)
	})

	t.Run("Non-empty map", func(t *testing.T) {
		oldBlockIDs := NewSyncedMap[int, []uint32]()

		oldBlockIDs.Set(1, []uint32{1})
		oldBlockIDs.Set(2, []uint32{2})
		oldBlockIDs.Set(3, []uint32{3})

		result, hasTransactions := ConvertSyncedMapToUint32Slice[int](oldBlockIDs)
		assert.ElementsMatch(t, []uint32{1, 2, 3}, result)
		assert.True(t, hasTransactions)
	})
}

// TestSplitSwissMapUint64Delete tests the Delete method of SplitSwissMapUint64.
func TestSplitSwissMapUint64Delete(t *testing.T) {
	t.Run("bucket does not exist", func(t *testing.T) {
		m := NewSplitSwissMapUint64(10)
		h := chainhash.Hash{0x00, 0x01}
		bucket := Bytes2Uint16Buckets(h, m.nrOfBuckets)
		delete(m.m, bucket)

		err := m.Delete(h)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrBucketDoesNotExist)
	})

	t.Run("hash does not exist", func(t *testing.T) {
		m := NewSplitSwissMapUint64(10)
		h := chainhash.Hash{0x02, 0x01}

		err := m.Delete(h)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrHashDoesNotExist)
	})

	t.Run("success", func(t *testing.T) {
		m := NewSplitSwissMapUint64(10)
		h := chainhash.Hash{0x03, 0x01}

		require.NoError(t, m.Put(h, 1))
		assert.Equal(t, 1, m.Length())

		err := m.Delete(h)
		require.NoError(t, err)
		assert.False(t, m.Exists(h))
		assert.Equal(t, 0, m.Length())
	})
}
