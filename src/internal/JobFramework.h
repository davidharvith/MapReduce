#ifndef MAPREDUCE_INTERNAL_JOBFRAMEWORK_H
#define MAPREDUCE_INTERNAL_JOBFRAMEWORK_H

#include <atomic>
#include <mutex>
#include <thread>
#include <vector>

#include "mapreduce/MapReduceClient.h"
#include "mapreduce/errors.h"

class Barrier;

struct ThreadContext;

/// Shared state for one MapReduce job.
struct JobFramework {
    const MapReduceClient& client;
    const InputVec& inputVec;
    OutputVec& outputVec;

    int numThreads;
    std::vector<std::thread> threads;
    std::vector<ThreadContext*> threadContexts;

    std::vector<std::vector<IntermediatePair>> intermediateVectors;
    std::vector<std::vector<IntermediatePair>> shuffledVectors;
    std::atomic<size_t> shuffleProgress{0};

    std::atomic<size_t> inputIndex{0};
    std::atomic<size_t> reduceIndex{0};
    std::mutex outputMutex;
    std::mutex joinMutex;
    Barrier* barrier = nullptr;

    std::atomic<uint64_t> state{0};
    std::atomic<bool> joined{false};
    std::atomic<bool> failed{false};
    std::atomic<int> errorCode{JOB_OK};

    JobFramework(const MapReduceClient& client,
                 const InputVec& inputVec,
                 OutputVec& outputVec,
                 int numThreads);

    ~JobFramework();

    void setError(int code);
};

struct ThreadContext {
    int threadId;
    JobFramework* job;
    std::vector<IntermediatePair>* intermediateVec;

    ThreadContext(int id, JobFramework* job);
};

#endif
