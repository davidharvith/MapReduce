# Architecture

## Job lifecycle

1. **Map** — threads claim input records via `inputIndex.fetch_add(1)`. Each thread writes to its own `intermediateVectors[threadId]` (lock-free via `emit2`).
2. **Sort** — each thread sorts its local intermediate vector by `*K2` (value comparison).
3. **Barrier** — all threads must finish map+sort before shuffle.
4. **Shuffle** — parallel **merge of sorted runs**: each thread holds a sorted run; runs are combined with `std::merge` in a tree (`log₂ N` barriers). Thread 0 scans the fully merged sorted stream and groups equivalent keys into `shuffledVectors`.
5. **Barrier** — all threads wait for shuffle to finish.
6. **Reduce** — threads claim groups via `reduceIndex.fetch_add(1)` and call client `reduce`.
7. **Barrier** — final synchronization before `waitForJob` returns.

## Shuffle key semantics

Sort and merge use `*(a.first) < *(b.first)`. Grouping starts a new bucket when `K2PtrLess` holds between the previous key and the current key (equivalent keys stay in one group).

## Packed job state

`STATE_PACK(stage, processed, total)` layout (64-bit atomic):

| Bits | Field |
|------|--------|
| 0–1 | `stage` (2 bits) |
| 2–32 | `processed` (31 bits, max ~2e9) |
| 33–63 | `total` (31 bits) |

## Error handling

Failures set `JobFramework::errorCode` and `failed`; worker threads still participate in barriers. After `waitForJob`, call `getJobError(job)`.

## Memory ownership

- **Input** — owned by caller; must outlive the job.
- **emit2 pairs** — owned by the client until deleted in `reduce`.
- **emit3 pairs** — owned by caller after job completion.
