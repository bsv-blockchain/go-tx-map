package txmap

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitSwissMapUint64_Delete(t *testing.T) {
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
