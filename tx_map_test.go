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

// TestNewSplitSwissMapUint64Headroom verifies that NewSplitSwissMapUint64 allocates
// each bucket with 20% headroom over length/nrOfBuckets. With nrOfBuckets=1024 and
// length=14*1024 (the load-factor limit for a single dolthub group per bucket), the
// underlying dolthub/swiss.Map for bucket 0 must have free capacity >= ceil(perBucket*1.2)
// so that filling it to perBucket items does not trigger a rehash.
func TestNewSplitSwissMapUint64Headroom(t *testing.T) {
	const nrOfBuckets = uint32(1024)
	const length = uint32(14 * 1024)
	expectedMin := int(((length + length/5) / nrOfBuckets))

	m := NewSplitSwissMapUint64(length)
	require.NotNil(t, m)

	bucket0 := m.Map()[0]
	require.NotNil(t, bucket0)

	// Capacity() on dolthub/swiss.Map returns remaining capacity until rehash.
	// Since the map is freshly allocated and empty, this equals the load-factor limit.
	cap0 := bucket0.Map().Capacity()
	require.GreaterOrEqual(t, cap0, expectedMin,
		"bucket 0 should have at least %d capacity (length=%d, buckets=%d, 1.2x headroom); got %d",
		expectedMin, length, nrOfBuckets, cap0)
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

	err = m.Set([32]byte{0x02, 0x01}, uint64(2))
	require.Error(t, err, "cannot set a key that does not exist")

	err = m.Set([32]byte{0x03, 0x01}, uint64(3))
	require.NoError(t, err)

	wasSet, err := m.SetIfExists([32]byte{0x04, 0x01}, uint64(4))
	require.NoError(t, err)
	assert.True(t, wasSet)

	val, ok = m.Get([32]byte{0x04, 0x01})
	assert.True(t, ok)
	assert.Equal(t, uint64(4), val)

	wasSet, err = m.SetIfExists([32]byte{0x44, 0x01}, uint64(4))
	require.NoError(t, err)
	assert.False(t, wasSet)

	wasSet, err = m.SetIfNotExists([32]byte{0x44, 0x01}, uint64(4))
	require.NoError(t, err)
	assert.True(t, wasSet)

	val, ok = m.Get([32]byte{0x44, 0x01})
	assert.True(t, ok)
	assert.Equal(t, uint64(4), val)

	wasSet, err = m.SetIfNotExists([32]byte{0x03, 0x01}, uint64(5))
	require.NoError(t, err)
	assert.False(t, wasSet)

	ok = m.Exists([32]byte{0x03, 0x01})
	assert.True(t, ok)

	val, ok = m.Get([32]byte{0x03, 0x01})
	assert.True(t, ok)
	assert.Equal(t, uint64(3), val)

	nrOfKeys := 0

	m.Iter(func(_ chainhash.Hash, _ uint64) bool {
		nrOfKeys++

		return false
	})
	assert.Equal(t, 4, nrOfKeys)
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

// TestSwissMapUint64Clear verifies that Clear empties the map while
// preserving the underlying preallocation so the same instance can be
// reused via sync.Pool without re-allocating.
func TestSwissMapUint64Clear(t *testing.T) {
	m := NewSwissMapUint64(1024)
	for i := 0; i < 100; i++ {
		var h chainhash.Hash
		h[0] = byte(i)
		require.NoError(t, m.Put(h, uint64(i)))
	}
	require.Equal(t, 100, m.Length())

	m.Clear()
	require.Equal(t, 0, m.Length())

	// Underlying capacity should still be present — re-fill without any
	// rehash. We can't directly observe rehash from outside, but we can
	// assert that lookups behave correctly after Clear+refill.
	for i := 0; i < 100; i++ {
		var h chainhash.Hash
		h[0] = byte(i)
		require.NoError(t, m.Put(h, uint64(i*2)))
	}
	for i := 0; i < 100; i++ {
		var h chainhash.Hash
		h[0] = byte(i)
		v, ok := m.Get(h)
		require.True(t, ok)
		require.Equal(t, uint64(i*2), v)
	}
}

// TestSplitSwissMapUint64Clear verifies that Clear empties every bucket.
func TestSplitSwissMapUint64Clear(t *testing.T) {
	m := NewSplitSwissMapUint64(10_000)
	for i := 0; i < 1000; i++ {
		var h chainhash.Hash
		h[0] = byte(i)
		h[1] = byte(i >> 8)
		require.NoError(t, m.Put(h, uint64(i)))
	}
	require.Equal(t, 1000, m.Length())

	m.Clear()
	require.Equal(t, 0, m.Length())

	// Re-fill and lookup.
	for i := 0; i < 1000; i++ {
		var h chainhash.Hash
		h[0] = byte(i)
		h[1] = byte(i >> 8)
		require.NoError(t, m.Put(h, uint64(i*3)))
	}
	for i := 0; i < 1000; i++ {
		var h chainhash.Hash
		h[0] = byte(i)
		h[1] = byte(i >> 8)
		v, ok := m.Get(h)
		require.True(t, ok)
		require.Equal(t, uint64(i*3), v)
	}
}

// TestSwissMapClear verifies Clear on the value-less SwissMap.
func TestSwissMapClear(t *testing.T) {
	m := NewSwissMap(1024)
	for i := 0; i < 100; i++ {
		var h chainhash.Hash
		h[0] = byte(i)
		require.NoError(t, m.Put(h))
	}
	require.Equal(t, 100, m.Length())

	m.Clear()
	require.Equal(t, 0, m.Length())

	for i := 0; i < 100; i++ {
		var h chainhash.Hash
		h[0] = byte(i)
		assert.False(t, m.Exists(h))
	}
}
