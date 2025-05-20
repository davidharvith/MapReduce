#ifndef GENERAL_CONTEXT_H
#define GENERAL_CONTEXT_H

#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include "Barrier.h"
#include "MapReduceFramework.h"
#include <cstdint>

#define STATE_PACK(stage, processed, total) \
    ((uint64_t)(stage) | ((uint64_t)(processed) << 2) | ((uint64_t)(total) << 33))

#define STATE_UNPACK(stage_var, processed_var, total_var, state_value) \
    do { \
        stage_var = (state_value) & 0x3; \
        processed_var = ((state_value) >> 2) & 0x7FFFFFFF; \
        total_var = ((state_value) >> 33) & 0x7FFFFFFF; \
    } while (0)

// Forward declaration
struct ThreadContext;

// Holds all shared state for a MapReduce job
struct JobFrameWork {

    size_t totalIntermediatePairs = 0;    // Set after map+sort, used for shuffle progress


    // Framework API
    const MapReduceClient& client;
    const InputVec& inputVec;
    OutputVec& outputVec;

    // Threading
    int numThreads;
    std::vector<std::thread> threads;
    std::vector<ThreadContext*> threadContexts;

    // Intermediate data (per thread)
    std::vector<std::vector<IntermediatePair>> intermediateVectors;

    // Shuffle data (for reduce phase)
    std::vector<std::vector<IntermediatePair>> shuffledVectors;
    std::atomic<size_t> shuffledIndex{0}; // For shuffle phase
    // Synchronization
    std::atomic<size_t> inputIndex{0}; // For map phase
    std::atomic<size_t> reduceIndex{0};//for reduce phase
    std::mutex outputMutex;
    std::mutex shuffleMutex;
    // static std::mutex joinMutex; 
    std::mutex joinMutex; // added this line
    Barrier* barrier = nullptr;

    // Job state
    std::atomic<uint64_t> state{0};
    std::atomic<bool> finished{false};

    // For wait/close semantics
    std::atomic<bool> joined{false};

    //reduced groups

    JobFrameWork(const MapReduceClient& client,
                 const InputVec& inputVec,
                 OutputVec& outputVec,
                 int numThreads);

    ~JobFrameWork();
};

// Holds per-thread state
struct ThreadContext {
    int threadId;
    JobFrameWork* job;
    std::vector<IntermediatePair>* intermediateVec;

    ThreadContext(int id, JobFrameWork* job);
};



#endif // GENERAL_CONTEXT_H
