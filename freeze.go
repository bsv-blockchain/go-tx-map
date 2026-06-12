package txmap

// Freeze / Clear lifecycle
//
// Freeze marks a map read-only. It exists to eliminate read-side lock
// contention on the lock-based maps: once a map has been fully populated and
// will only be read, Freeze lets every subsequent read (Exists, Get, Length,
// Keys, Iter) skip the per-bucket RWMutex.RLock. Acquiring an RLock performs an
// atomic add/sub on the reader counter, and under many cores reading the same
// shard that cache line ping-pongs between CPUs and dominates the profile.
// After Freeze, reads do a single relaxed atomic load of a bool flag instead.
//
// Once frozen, every write method (Put, PutMulti, Set, SetIfExists,
// SetIfNotExists, Delete) returns ErrMapFrozen rather than silently mutating a
// map that concurrent readers assume is immutable.
//
// On the lock-free maps (SwissLockFreeMapUint64, *LockFreeMapUint64 and their
// split variants) reads already take no lock, so Freeze only installs the
// write guard, for API uniformity.
//
// Clear empties the map in place — retaining the (potentially multi-GB)
// preallocated backing storage — and un-freezes it, so a pooled map can be
// recycled across uses: Clear -> Put... -> Freeze -> Get... -> Clear.
//
// HAPPENS-BEFORE CONTRACT: the caller must ensure all writes have completed
// before calling Freeze, and that Freeze happens-before any reader observes the
// frozen state (e.g. an errgroup.Wait between the write phase and the read
// phase). Freeze and Clear are not safe to call concurrently with other
// operations on the same map.

// Compile-time checks that every concrete map type satisfies its interface,
// including the Freeze/Clear methods now required by TxMap, TxHashMap and
// Uint64. (The TxMap assertions are duplicated next to each type definition;
// gathering all twelve here gives a single checklist that every implementation
// gained the new methods.)
var (
	_ TxMap = (*SwissMapUint64)(nil)
	_ TxMap = (*SplitSwissMap)(nil)
	_ TxMap = (*SplitSwissMapUint64)(nil)
	_ TxMap = (*NativeMapUint64)(nil)
	_ TxMap = (*NativeSplitMap)(nil)
	_ TxMap = (*NativeSplitMapUint64)(nil)

	_ TxHashMap = (*SwissMap)(nil)
	_ TxHashMap = (*NativeMap)(nil)

	_ Uint64 = (*SwissLockFreeMapUint64)(nil)
	_ Uint64 = (*NativeLockFreeMapUint64)(nil)
	_ Uint64 = (*SplitSwissLockFreeMapUint64)(nil)
	_ Uint64 = (*NativeSplitLockFreeMapUint64)(nil)
)

// --- dolthub/swiss-backed leaf maps -----------------------------------------

// Freeze marks the map read-only. See the lifecycle notes at the top of this file.
func (s *SwissMap) Freeze() { s.frozen.Store(true) }

// Freeze marks the map read-only. See the lifecycle notes at the top of this file.
func (s *SwissMapUint64) Freeze() { s.frozen.Store(true) }

// Freeze marks the map read-only; subsequent Put calls return ErrMapFrozen.
func (s *SwissLockFreeMapUint64) Freeze() { s.frozen.Store(true) }

// Clear empties the map without releasing the underlying backing storage and
// un-freezes it for reuse. Not safe for concurrent use.
func (s *SwissLockFreeMapUint64) Clear() {
	s.m.Clear()
	s.length.Store(0)
	s.frozen.Store(false)
}

// --- native-map-backed leaf maps --------------------------------------------

// Freeze marks the map read-only. See the lifecycle notes at the top of this file.
func (s *NativeMap) Freeze() { s.frozen.Store(true) }

// Clear empties the map (retaining its allocated capacity) and un-freezes it
// for reuse. Per the lifecycle contract above, Clear must not run concurrently
// with other operations on the map (a frozen reader skips the lock that Clear
// takes); the write lock only orders it against other locked, non-frozen ops.
func (s *NativeMap) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.m)
	s.length = 0
	s.frozen.Store(false)
}

// Freeze marks the map read-only. See the lifecycle notes at the top of this file.
func (s *NativeMapUint64) Freeze() { s.frozen.Store(true) }

// Clear empties the map (retaining its allocated capacity) and un-freezes it
// for reuse. Per the lifecycle contract above, Clear must not run concurrently
// with other operations on the map (a frozen reader skips the lock that Clear
// takes); the write lock only orders it against other locked, non-frozen ops.
func (s *NativeMapUint64) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	clear(s.m)
	s.length = 0
	s.frozen.Store(false)
}

// Freeze marks the map read-only; subsequent Put calls return ErrMapFrozen.
func (s *NativeLockFreeMapUint64) Freeze() { s.frozen.Store(true) }

// Clear empties the map and un-freezes it for reuse. Not safe for concurrent use.
func (s *NativeLockFreeMapUint64) Clear() {
	clear(s.m)
	s.length.Store(0)
	s.frozen.Store(false)
}

// --- split maps: Freeze/Clear fan out to every bucket -----------------------

// Freeze freezes every bucket, so reads across the whole split map become
// lock-free. See the lifecycle notes at the top of this file.
func (g *SplitSwissMap) Freeze() {
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Freeze()
	}
}

// Clear empties and un-freezes every bucket, recycling the split map for reuse.
func (g *SplitSwissMap) Clear() {
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Clear()
	}
}

// Freeze freezes every bucket, so reads across the whole split map become
// lock-free. See the lifecycle notes at the top of this file.
func (g *SplitSwissMapUint64) Freeze() {
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Freeze()
	}
}

// Freeze freezes every bucket; subsequent Put calls return ErrMapFrozen.
func (g *SplitSwissLockFreeMapUint64) Freeze() {
	for i := uint64(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Freeze()
	}
}

// Clear empties and un-freezes every bucket, recycling the split map for reuse.
func (g *SplitSwissLockFreeMapUint64) Clear() {
	for i := uint64(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Clear()
	}
}

// Freeze freezes every bucket, so reads across the whole split map become
// lock-free. See the lifecycle notes at the top of this file.
func (g *NativeSplitMap) Freeze() {
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Freeze()
	}
}

// Clear empties and un-freezes every bucket, recycling the split map for reuse.
func (g *NativeSplitMap) Clear() {
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Clear()
	}
}

// Freeze freezes every bucket, so reads across the whole split map become
// lock-free. See the lifecycle notes at the top of this file.
func (g *NativeSplitMapUint64) Freeze() {
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Freeze()
	}
}

// Clear empties and un-freezes every bucket, recycling the split map for reuse.
func (g *NativeSplitMapUint64) Clear() {
	for i := uint16(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Clear()
	}
}

// Freeze freezes every bucket; subsequent Put calls return ErrMapFrozen.
func (g *NativeSplitLockFreeMapUint64) Freeze() {
	for i := uint64(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Freeze()
	}
}

// Clear empties and un-freezes every bucket, recycling the split map for reuse.
func (g *NativeSplitLockFreeMapUint64) Clear() {
	for i := uint64(0); i <= g.nrOfBuckets; i++ {
		g.m[i].Clear()
	}
}
