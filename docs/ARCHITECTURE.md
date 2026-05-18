# Architecture

## Job lifecycle

1. **Map** — threads claim input records via `inputIndex.fetch_add(1)`. Each thread writes to its own `intermediateVectors[threadId]` (lock-free via `emit2`).
2. **Sort** — each thread sorts its local intermediate vector by `*K2` (value comparison).
3. **Barrier** — all threads must finish map+sort before shuffle.
4. **Shuffle** — only thread 0 merges all sorted runs into groups with the same K2 key, using `std::map<K2*, vector, K2PtrLess>`.
5. **Barrier** — all threads wait for shuffle to finish.
6. **Reduce** — threads claim groups via `reduceIndex.fetch_add(1)` and call client `reduce`.
7. **Barrier** — final synchronization before `waitForJob` returns.

## Shuffle key semantics

`K2PtrLess` compares `*a < *b`, not pointer addresses. Two distinct `K2*` objects with equal values land in the same shuffle group. This matches sort order (`*(a.first) < *(b.first)`).

## Packed job state

`STATE_PACK(stage, processed, total)` layout (64-bit atomic):

| Bits | Field |
|------|--------|
| 0–1 | `stage` (2 bits) |
| 2–32 | `processed` (31 bits, max ~2e9) |
| 33–63 | `total` (31 bits) |

`getJobState` reports `percentage = 100 * min(processed, total) / total` (or 100% when `total == 0`).

Shuffle progress runs on thread 0 only; at high thread counts this becomes the Amdahl bottleneck.

## Error handling

Failures set `JobFramework::errorCode` and `failed`; worker threads still participate in barriers to avoid deadlock. After `waitForJob`, call `getJobError(job)` — never `exit()` from the library.

## Memory ownership

- **Input** — owned by caller; must outlive the job.
- **emit2 pairs** — owned by the client until deleted in `reduce` (typical pattern).
- **emit3 pairs** — owned by caller after job completion.
