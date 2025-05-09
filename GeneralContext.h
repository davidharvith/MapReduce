#ifndef GENERAL_CONTEXT_H
#define GENERAL_CONTEXT_H

#include <vector>
#include <thread>
#include <atomic>
#include <mutex>
#include "Barrier.h"
#include "MapReduceFramework.h"

// Forward declaration
struct ThreadContext;

// Holds all shared state for a MapReduce job
struct JobFrameWork {
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

    // Synchronization
    std::atomic<size_t> inputIndex{0}; // For map phase
    std::atomic<size_t> reduceIndex{0};//for reduce phase
    std::mutex outputMutex;
    std::mutex shuffleMutex;
    Barrier* barrier = nullptr;

    // Job state
    std::atomic<stage_t> stage{UNDEFINED_STAGE};
    std::atomic<float> percentage{0.0f};
    std::atomic<bool> finished{false};

    // For wait/close semantics
    std::atomic<bool> joined{false};

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
