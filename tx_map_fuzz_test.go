package txmap

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FuzzBytes2Uint16Buckets verifies that Bytes2Uint16Buckets produces
// deterministic results for arbitrary hashes and non-zero modulus values.
//
// The fuzz test seeds the corpus with several representative hash and
// modulus pairs and then checks that:
//  1. The returned value is always less than the provided modulus.
//  2. The value matches the expected manual calculation.
func FuzzBytes2Uint16Buckets(f *testing.F) {
	// Seed with edge cases from unit tests to guide the fuzzer.
	seeds := []struct {
		data []byte
		mod  uint16
	}{
		{data: []byte{0x00, 0x01}, mod: 256},
		{data: []byte{0xff, 0xff}, mod: 1024},
		{data: []byte{0x12, 0x34}, mod: 10},
	}

	for _, seed := range seeds {
		f.Add(seed.data, seed.mod)
	}

	f.Fuzz(func(t *testing.T, b []byte, mod uint16) {
		if mod == 0 {
			t.Skip("mod cannot be zero")
		}
		if len(b) < 2 {
			t.Skip("need at least two bytes")
		}

		var hash chainhash.Hash
		copy(hash[:], b)

		got := Bytes2Uint16Buckets(hash, mod)

		expected := (uint16(hash[0])<<8 | uint16(hash[1])) % mod

		require.Less(t, got, mod)
		assert.Equal(t, expected, got)
	})
}
