package txmap

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestBytes2Uint16 tests the Bytes2Uint16Buckets function with various byte arrays and modulus values.
func TestBytes2Uint16(t *testing.T) {
	type args struct {
		b   [32]byte
		mod uint16
	}

	tests := []struct {
		name string
		args args
		want uint16
	}{
		{
			name: "bytes2Uint16",
			args: args{
				b:   [32]byte{0x00, 0x01},
				mod: 256,
			},
			want: 1,
		},
		{
			name: "bytes2Uint16",
			args: args{
				b:   [32]byte{0x01, 0xff},
				mod: 256,
			},
			want: 255,
		},
		{
			name: "bytes2Uint16",
			args: args{
				b:   [32]byte{0xff, 0x01},
				mod: 256,
			},
			want: 1,
		},
		{
			name: "bytes2Uint16",
			args: args{
				b:   [32]byte{0xff, 0xff},
				mod: 256,
			},
			want: 255,
		},
		{
			name: "bytes2Uint16",
			args: args{
				b:   [32]byte{0xdd, 0xdd},
				mod: 256,
			},
			want: 221,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, Bytes2Uint16Buckets(tt.args.b, tt.args.mod), "bytes2Uint16(%v)", tt.args.b)
		})
	}
}

// TestNewSwissMap tests the creation and basic usage of a SwissMap.
func TestNewSwissMap(t *testing.T) {
	t.Run("NewSwissMap", func(t *testing.T) {
		m := NewSwissMap(100)
		assert.NotNil(t, m)

		testTxHashMap(t, m)

		mm := m.Map()
		assert.NotNil(t, mm)
	})
}

// TestNewSplitSwissMap tests the creation and basic usage of a SplitSwissMap.
func TestNewSplitSwissMap(t *testing.T) {
	t.Run("NewSplitSwissMap", func(t *testing.T) {
		m := NewSplitSwissMap(100)
		assert.NotNil(t, m)

		testTxMap(t, m)

		mm := m.Map()
		assert.NotNil(t, mm)
	})
}

// TestNewSwissLockFreeMapUint64 tests the creation and basic usage of a SwissLockFreeMapUint64.
func TestNewSwissLockFreeMapUint64(t *testing.T) {
	t.Run("NewSwissLockFreeMapUint64", func(t *testing.T) {
		m := NewSwissLockFreeMapUint64(100)
		assert.NotNil(t, m)

		testTxMapUint64(t, m)

		mm := m.Map()
		assert.NotNil(t, mm)
	})
}

// TestNewSplitSwissLockFreeMapUint64 tests the creation and basic usage of a SplitSwissLockFreeMapUint64.
func TestNewSplitSwissLockFreeMapUint64(t *testing.T) {
	t.Run("NewSplitSwissLockFreeMapUint64", func(t *testing.T) {
		m := NewSplitSwissLockFreeMapUint64(100)
		assert.NotNil(t, m)

		testTxMapUint64(t, m)

		mm := m.Map()
		assert.NotNil(t, mm)
	})
}

// TestNewSplitSwissMapUint64 tests the creation and basic usage of a SplitSwissMapUint64.
func TestNewSplitSwissMapUint64(t *testing.T) {
	t.Run("NewSplitSwissMapUint64", func(t *testing.T) {
		m := NewSplitSwissMapUint64(100)
		assert.NotNil(t, m)

		testTxMap(t, m)

		mm := m.Map()
		assert.NotNil(t, mm)
	})
}

// TestNewSwissMapUint64 tests the creation and basic usage of a SwissMapUint64.
func TestNewSwissMapUint64(t *testing.T) {
	t.Run("NewSwissMapUint64", func(t *testing.T) {
		m := NewSwissMapUint64(100)
		assert.NotNil(t, m)

		testTxMap(t, m)

		mm := m.Map()
		assert.NotNil(t, mm)
	})
}

// TestSplitSwissLockFreeMapUint64 tests the creation and basic usage of a SplitSwissLockFreeMapUint64.
func TestSplitSwissLockFreeMapUint64(t *testing.T) {
	t.Run("SplitSwissLockFreeMapUint64", func(t *testing.T) {
		m := NewSplitSwissLockFreeMapUint64(100)
		assert.NotNil(t, m)

		testTxMapUint64(t, m)

		mm := m.Map()
		assert.NotNil(t, mm)
	})
}

// TestSwissLockFreeMapUint64GetValueExists ensures getting an existing key
// returns the stored value and true.
func TestSwissLockFreeMapUint64GetValueExists(t *testing.T) {
	m := NewSwissLockFreeMapUint64(10)
	require.NoError(t, m.Put(1, 5))

	val, ok := m.Get(1)
	assert.True(t, ok)
	assert.Equal(t, uint64(5), val)
}

// TestSwissLockFreeMapUint64GetValueMissing ensures getting a missing key
// returns zero value and false.
func TestSwissLockFreeMapUint64GetValueMissing(t *testing.T) {
	m := NewSwissLockFreeMapUint64(10)

	val, ok := m.Get(2)
	assert.False(t, ok)
	assert.Equal(t, uint64(0), val)
}

// TestSplitSwissMapPutMultiBucket tests the PutMultiBucket method of SplitSwissMap.
func TestSplitSwissMapPutMultiBucket(t *testing.T) {
	t.Run("bucket does not exist", func(t *testing.T) {
		m := NewSplitSwissMap(10)
		err := m.PutMultiBucket(m.nrOfBuckets+1, []chainhash.Hash{{0x00, 0x01}}, 1)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrBucketDoesNotExist)
	})

	t.Run("duplicate hash", func(t *testing.T) {
		m := NewSplitSwissMap(10)
		h := chainhash.Hash{0x01, 0x02}
		bucket := Bytes2Uint16Buckets(h, m.nrOfBuckets)
		require.NoError(t, m.PutMultiBucket(bucket, []chainhash.Hash{h}, 1))

		err := m.PutMultiBucket(bucket, []chainhash.Hash{h}, 2)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrHashAlreadyExists)
	})

	t.Run("success", func(t *testing.T) {
		m := NewSplitSwissMap(10)
		h1 := chainhash.Hash{0x02, 0x01, 0x01}
		h2 := chainhash.Hash{0x02, 0x01, 0x02}
		bucket := Bytes2Uint16Buckets(h1, m.nrOfBuckets)
		hashes := []chainhash.Hash{h1, h2}

		require.NoError(t, m.PutMultiBucket(bucket, hashes, 3))

		for _, h := range hashes {
			v, ok := m.Get(h)
			assert.True(t, ok)
			assert.Equal(t, uint64(3), v)
		}
	})
}

// testTxMap tests the basic operations of a TxMap implementation.
func testTxMap(t *testing.T, m TxMap) {
	err := m.Put([32]byte{0x00, 0x01}, 1)
	require.NoError(t, err)

	ok := m.Exists([32]byte{0x00, 0x01})
	assert.True(t, ok)

	val, ok := m.Get([32]byte{0x00, 0x01})
	assert.True(t, ok)
	// it's a key only map, so the value is always zero
	assert.Equal(t, uint64(1), val)

	ok = m.Exists([32]byte{0x01, 0x01})
	assert.False(t, ok)

	assert.Equal(t, 1, m.Length())

	err = m.PutMulti([]chainhash.Hash{
		[32]byte{0x02, 0x01},
		[32]byte{0x03, 0x01},
		[32]byte{0x04, 0x01},
	}, 2)
	require.NoError(t, err)

	ok = m.Exists([32]byte{0x02, 0x01})
	assert.True(t, ok)
	ok = m.Exists([32]byte{0x03, 0x01})
	assert.True(t, ok)
	ok = m.Exists([32]byte{0x04, 0x01})
	assert.True(t, ok)

	keys := m.Keys()
	assert.Equal(t, 4, len(keys)) //nolint:testifylint // assert.Len doesn't work with the map
	assert.Contains(t, keys, chainhash.Hash{0x00, 0x01})
	assert.Contains(t, keys, chainhash.Hash{0x02, 0x01})
	assert.Contains(t, keys, chainhash.Hash{0x03, 0x01})
	assert.Contains(t, keys, chainhash.Hash{0x04, 0x01})

	val, ok = m.Get([32]byte{0x02, 0x01})
	assert.True(t, ok)
	assert.Equal(t, uint64(2), val)

	val, ok = m.Get([32]byte{0x03, 0x01})
	assert.True(t, ok)
	assert.Equal(t, uint64(2), val)

	val, ok = m.Get([32]byte{0x04, 0x01})
	assert.True(t, ok)
	assert.Equal(t, uint64(2), val)

	assert.Equal(t, 4, m.Length())

	err = m.Delete([32]byte{0x02, 0x01})
	require.NoError(t, err)

	ok = m.Exists([32]byte{0x02, 0x01})
	assert.False(t, ok)

	assert.Equal(t, 3, m.Length())
}

// testTxMapUint64 tests the basic operations of a Uint64 map implementation.
func testTxMapUint64(t *testing.T, m Uint64) {
	err := m.Put(1, 1)
	require.NoError(t, err)

	ok := m.Exists(1)
	assert.True(t, ok)

	val, ok := m.Get(1)
	assert.True(t, ok)
	// it's a key only map, so the value is always zero
	assert.Equal(t, uint64(1), val)

	ok = m.Exists(2)
	assert.False(t, ok)

	assert.Equal(t, 1, m.Length())
}

// testTxHashMap tests the basic operations of a TxHashMap implementation.
func testTxHashMap(t *testing.T, m TxHashMap) {
	err := m.Put([32]byte{0x00, 0x01})
	require.NoError(t, err)

	ok := m.Exists([32]byte{0x00, 0x01})
	assert.True(t, ok)

	val, ok := m.Get([32]byte{0x00, 0x01})
	assert.True(t, ok)
	// it's a key only map, so the value is always zero
	assert.Equal(t, uint64(0), val)

	ok = m.Exists([32]byte{0x01, 0x01})
	assert.False(t, ok)

	assert.Equal(t, 1, m.Length())

	err = m.PutMulti([]chainhash.Hash{
		[32]byte{0x02, 0x01},
		[32]byte{0x03, 0x01},
		[32]byte{0x04, 0x01},
	})
	require.NoError(t, err)

	ok = m.Exists([32]byte{0x02, 0x01})
	assert.True(t, ok)
	ok = m.Exists([32]byte{0x03, 0x01})
	assert.True(t, ok)
	ok = m.Exists([32]byte{0x04, 0x01})
	assert.True(t, ok)

	keys := m.Keys()
	assert.Equal(t, 4, len(keys)) //nolint:testifylint // assert.Len doesn't work with the map
	assert.Contains(t, keys, chainhash.Hash{0x00, 0x01}, "keys should contain 0x00, 0x01")
	assert.Contains(t, keys, chainhash.Hash{0x02, 0x01}, "keys should contain 0x02, 0x01")
	assert.Contains(t, keys, chainhash.Hash{0x03, 0x01}, "keys should contain 0x03, 0x01")
	assert.Contains(t, keys, chainhash.Hash{0x04, 0x01}, "keys should contain 0x04, 0x01")

	assert.Equal(t, 4, m.Length())

	err = m.Delete([32]byte{0x02, 0x01})
	require.NoError(t, err)

	ok = m.Exists([32]byte{0x02, 0x01})
	assert.False(t, ok)

	assert.Equal(t, 3, m.Length())
}

// TestSplitSwissMapBuckets verifies the number of buckets returned.
func TestSplitSwissMapBuckets(t *testing.T) {
	m := NewSplitSwissMap(10)
	require.NotNil(t, m)
	assert.Equal(t, uint16(1024), m.Buckets())
}

// TestSplitSwissMapPutMulti tests the PutMulti method of SplitSwissMap.
func TestSplitSwissMapPutMulti(t *testing.T) {
	t.Run("empty slice", func(t *testing.T) {
		m := NewSplitSwissMap(10)
		err := m.PutMulti(nil, 1)
		require.NoError(t, err)
		assert.Equal(t, 0, m.Length())
	})

	t.Run("successful insert", func(t *testing.T) {
		m := NewSplitSwissMap(10)
		hashes := []chainhash.Hash{{0x00, 0x01}, {0x02, 0x02}, {0x04, 0x03}}
		require.NoError(t, m.PutMulti(hashes, 2))
		for _, h := range hashes {
			ok := m.Exists(h)
			assert.True(t, ok)
		}
		assert.Equal(t, len(hashes), m.Length())
	})

	t.Run("error on duplicate", func(t *testing.T) {
		m := NewSplitSwissMap(10)
		hash := chainhash.Hash{0x09, 0x01}
		require.NoError(t, m.Put(hash, 3))
		err := m.PutMulti([]chainhash.Hash{hash, {0x0a, 0x01}}, 3)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "failed to put multi in bucket")
		assert.Equal(t, 1, m.Length())
	})

	t.Run("error on duplicate within slice", func(t *testing.T) {
		m := NewSplitSwissMap(10)
		hash := chainhash.Hash{0x01, 0x02}
		err := m.PutMulti([]chainhash.Hash{hash, hash}, 1)
		require.Error(t, err)
		assert.Equal(t, 1, m.Length())
	})
}

// TestSplitSwissMapDelete tests the Delete method of SplitSwissMap.
func TestSplitSwissMapDelete(t *testing.T) {
	tests := []struct {
		name    string
		prepare func(*SplitSwissMap) chainhash.Hash
		wantErr error
	}{
		{
			name: "bucket missing",
			prepare: func(m *SplitSwissMap) chainhash.Hash {
				hash := chainhash.Hash{0x00, 0x03}
				bucket := Bytes2Uint16Buckets(hash, m.nrOfBuckets)
				delete(m.m, bucket)
				return hash
			},
			wantErr: ErrBucketDoesNotExist,
		},
		{
			name: "hash missing",
			prepare: func(_ *SplitSwissMap) chainhash.Hash {
				return chainhash.Hash{0x00, 0x05}
			},
			wantErr: ErrHashDoesNotExist,
		},
		{
			name: "delete success",
			prepare: func(m *SplitSwissMap) chainhash.Hash {
				hash := chainhash.Hash{0x00, 0x07}
				require.NoError(t, m.Put(hash, 1))
				return hash
			},
			wantErr: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := NewSplitSwissMap(10)
			hash := tt.prepare(m)
			err := m.Delete(hash)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
				assert.False(t, m.Exists(hash))
			}
		})
	}
}
