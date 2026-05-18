#ifndef MAPREDUCE_INTERNAL_STATE_H
#define MAPREDUCE_INTERNAL_STATE_H

#include <cstdint>

#include "mapreduce/MapReduceFramework.h"

/// Packed atomic job progress: 2-bit stage | 31-bit processed | 31-bit total.
#define STATE_PACK(stage, processed, total) \
    ((uint64_t)(stage) | ((uint64_t)(processed) << 2) | ((uint64_t)(total) << 33))

#define STATE_UNPACK(stage_var, processed_var, total_var, state_value) \
    do {                                                               \
        stage_var = (state_value) & 0x3;                               \
        processed_var = ((state_value) >> 2) & 0x7FFFFFFF;             \
        total_var = ((state_value) >> 33) & 0x7FFFFFFF;                \
    } while (0)

inline bool tryIncrementProgress(std::atomic<uint64_t>& state) {
    uint64_t old_state = state.load();
    while (true) {
        unsigned stage;
        size_t processed;
        size_t total;
        STATE_UNPACK(stage, processed, total, old_state);
        const uint64_t new_state = STATE_PACK(stage, processed + 1, total);
        if (state.compare_exchange_weak(old_state, new_state)) {
            return true;
        }
    }
}

inline void ensureMapStage(std::atomic<uint64_t>& state) {
    uint64_t old = state.load(std::memory_order_relaxed);
    if ((old & 0x3ULL) == UNDEFINED_STAGE) {
        unsigned stage;
        size_t processed;
        size_t total;
        STATE_UNPACK(stage, processed, total, old);
        (void)stage;
        const uint64_t desired = STATE_PACK(MAP_STAGE, processed, total);
        state.compare_exchange_strong(old, desired);
    }
}

#endif
