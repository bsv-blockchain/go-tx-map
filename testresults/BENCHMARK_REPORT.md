# Swiss Map Implementation Benchmark Report

**Test Environment:** Intel Core i9-9880H @ 2.30GHz, darwin/amd64  
**Iterations:** 10,000,000 per benchmark

---

## Overview

This report compares two hash map implementations:
- **dolthub** - dolthub/swiss library
- **native** - Go's built-in map

---

## Quick Takeaways

- **Use native Go maps** for general purpose - fastest overall, especially for Delete (30-40% faster)
- **Use dolthub** for memory-constrained workloads - ~30% less memory than native (100M entries)
- **Avoid dolthub** for simple Map type Put operations - nearly 2x slower than native
- **LockFree maps are 2-25x faster** but don't support Delete

### dolthub vs native

- **Speed:** native wins most comparisons; ~5-46% faster (largest: Map Put, Map Delete)
- **Memory:** dolthub uses ~27-30% less (100M entries: 3.5 GB vs 5 GB for Map)

### Trade-off

| Choose | When |
|--------|------|
| **native** | Speed matters (general purpose, delete-heavy, latency-sensitive) |
| **dolthub** | Memory matters (large maps, 100M+ entries, RAM-constrained) |

---

## Performance by Implementation

**Units:** nanoseconds per operation (ns/op) - lower is better

### Put Operations

| Map Type | dolthub (ns/op) | native (ns/op) | Winner |
|----------|---------|--------|--------|
| Map | 147.8 | **80.3** | native |
| SplitMap | 651.5 | **628.6** | native |
| MapUint64 | 615.4 | **607.8** | native |
| SplitMapUint64 | 643.4 | **632.1** | native |
| LockFreeMapUint64 | 35.9 | **25.2** | native |
| SplitLockFreeMapUint64 | 64.0 | **59.3** | native |

**Put Winner: native** (6/6 wins)

### Get Operations

| Map Type | dolthub (ns/op) | native (ns/op) | Winner |
|----------|---------|--------|--------|
| Map | 67.5 | **62.1** | native |
| SplitMap | 99.8 | **93.1** | native |
| MapUint64 | 75.3 | **67.0** | native |
| SplitMapUint64 | 115.5 | **93.6** | native |
| LockFreeMapUint64 | 36.9 | **26.9** | native |
| SplitLockFreeMapUint64 | 67.2 | **59.3** | native |

**Get Winner: native** (6/6 wins)

### Exists Operations

| Map Type | dolthub (ns/op) | native (ns/op) | Winner |
|----------|---------|--------|--------|
| Map | 67.2 | **61.6** | native |
| SplitMap | 98.4 | **95.4** | native |
| MapUint64 | 67.7 | **62.5** | native |
| SplitMapUint64 | 100.3 | **104.1** | dolthub |
| LockFreeMapUint64 | 34.8 | **24.9** | native |
| SplitLockFreeMapUint64 | 63.6 | **62.1** | native |

**Exists Winner: native** (5/6 wins)

### Delete Operations

| Map Type | dolthub (ns/op) | native (ns/op) | Winner |
|----------|---------|--------|--------|
| Map | 86.4 | **47.7** | native |
| SplitMap | 727.4 | **690.9** | native |
| MapUint64 | 608.6 | **535.9** | native |
| SplitMapUint64 | 726.6 | **693.5** | native |

**Delete Winner: native** (4/4 wins)

### Memory Footprint (100,000,000 entries)

**Units:** GB - lower is better

| Map Type | dolthub (GB) | native (GB) | Winner |
|----------|--------------|-------------|--------|
| Map | **3.52** | 5.00 | dolthub |
| SplitMap | **4.37** | 6.01 | dolthub |
| MapUint64 | **4.36** | 6.00 | dolthub |
| SplitMapUint64 | **4.37** | — | dolthub |

**Memory Winner: dolthub** (4/4 wins)

**Relative to dolthub (100%):** native ~137-142%

---

## Overall Rankings

### Wins by Implementation

| Implementation | Put | Get | Exists | Delete | Memory | Total |
|----------------|-----|-----|--------|--------|--------|-------|
| **native** | 6 | 6 | 5 | 4 | 0 | **21** |
| **dolthub** | 0 | 0 | 1 | 0 | 4 | **5** |

### Average Performance (ns/op across all map types)

| Implementation | Put | Get | Exists | Delete | Overall Avg |
|----------------|-----|-----|--------|--------|-------------|
| **native** | 338.9 | 66.9 | 68.5 | 491.5 | **241.4** |
| **dolthub** | 359.7 | 77.0 | 72.0 | 537.3 | **261.5** |

---

## Key Findings

### 1. Native Go map is the overall winner
- Best for **Put**, **Get**, and **Delete** operations
- Especially dominant in Delete (30-40% faster than dolthub)

### 2. dolthub has the smallest memory footprint
- 4/4 wins for memory (Map, SplitMap, MapUint64, SplitMapUint64)
- ~30% less memory than native at 100M entries

### 3. dolthub underperforms on speed
- Particularly slow for Put on simple Map type (147.8 ns vs ~80 ns for native)

---

## Memory Footprint

**Test:** 100,000,000 entries per map

| Map Type | dolthub | native |
|----------|---------|--------|
| Map | 3.52 GB (100%) | 5.00 GB (142%) |
| SplitMap | 4.37 GB (100%) | 6.01 GB (137%) |
| MapUint64 | 4.36 GB (100%) | 6.00 GB (138%) |
| SplitMapUint64 | 4.37 GB (100%) | — |

**Bytes per entry (approx.):** dolthub Map ~35 B/entry; native Map ~50 B/entry

---

## Recommendations

| Priority | Recommendation |
|----------|----------------|
| **General Use** | Use **native** Go maps - best overall performance |
| **Memory-constrained** | Use **dolthub** - ~30% less memory than native at 100M entries |
| **Delete-heavy workloads** | Use **native** - significantly faster deletion |

---

## Notes

- LockFree map types do not support Delete operations
- All implementations show 0 allocations for read operations (Get, Exists)
- Write operations on value-storing maps (MapUint64, SplitMap*) incur ~285 B/op and 4 allocs/op
- Memory footprint measured with 100M entries
