#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include <algorithm>
#include <map>
#include "MapReduceFramework.h"
#include "GeneralContext.h"
#include "Barrier.h"
#include "errors.h"

// Comparator for grouping K2* by value, not pointer
struct K2PtrLess {
    bool operator()(const K2* a, const K2* b) const {
        return *a < *b;
    }
};

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec,
                            OutputVec& outputVec,
                            int multiThreadLevel);

void threadMain(ThreadContext* context);

void emit2(K2* key, V2* value, void* context);
void emit3(K3* key, V3* value, void* context);
void waitForJob(JobHandle jobHandle);
void getJobState(JobHandle jobHandle, JobState* state);
void closeJobHandle(JobHandle jobHandle);

// --- Implementation ---

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec,
                            OutputVec& outputVec,
                            int multiThreadLevel) {
    JobFrameWork* job = nullptr;
    try {
        job = new JobFrameWork(client, inputVec, outputVec, multiThreadLevel);
        job->barrier = new Barrier(multiThreadLevel);
    } catch (const std::bad_alloc&) {
        std::cerr << BAD_ALLOCATION;
        exit(1);
    }
    try {
        job->threadContexts.resize(multiThreadLevel);
    } catch (const std::bad_alloc&) {
        std::cerr << BAD_ALLOCATION;
        exit(1);
    }
    try {
        for (int i = 0; i < multiThreadLevel; ++i) {
            job->threadContexts[i] = new ThreadContext(i, job);
            job->threads.emplace_back(std::thread(threadMain, job->threadContexts[i]));
        }
    } catch (...) {
        std::cerr << THREAD_CREATION_ERROR;
        delete job;
        exit(1);
    }

    return static_cast<JobHandle>(job);
}

void threadMain(ThreadContext* context) {
    JobFrameWork* job = context->job;

    /* ---- added this code ---- */
    {
        uint64_t old = job->state.load(std::memory_order_relaxed);
        if ((old & 0x3ULL) == UNDEFINED_STAGE) {          // stage bits == 0
            unsigned stage; 
            size_t processed, total;
            STATE_UNPACK(stage, processed, total, old);
            (void)stage;
            uint64_t desired = STATE_PACK(MAP_STAGE, processed, total);
            job->state.compare_exchange_strong(old, desired);
        }
    }

    // Map phase
    while (true) {
        size_t inputIndex = job->inputIndex.fetch_add(1);
        if (inputIndex >= job->inputVec.size()) {
            break;
        }

        // Process the map FIRST
        const K1* key = job->inputVec[inputIndex].first;
        const V1* value = job->inputVec[inputIndex].second;
        job->client.map(key, value, context);

        // THEN update the state
        uint64_t old_state = job->state.load();
        while (true) {
            unsigned stage;
            size_t processed, total;
            STATE_UNPACK(stage, processed, total, old_state);
            uint64_t new_state = STATE_PACK(stage, processed + 1, total);
            if (job->state.compare_exchange_weak(old_state, new_state)) {
                break;
            }
        }
    }

    // Sort phase
    std::sort(
        context->intermediateVec->begin(),
        context->intermediateVec->end(),
        [](const IntermediatePair& a, const IntermediatePair& b) {
            return *(a.first) < *(b.first);
        }
    );

    job->barrier->barrier(); // Wait for all threads to finish map+sort

    // Shuffle phase (only thread 0)
    if (context->threadId == 0) {
        size_t totalPairs = 0;
        for (auto& vec : job->intermediateVectors) {
            totalPairs += vec.size();
        }
        job->state.store(STATE_PACK(SHUFFLE_STAGE, 0, totalPairs));

        std::map<K2*, std::vector<IntermediatePair>, K2PtrLess> grouped;
        for (auto& vec : job->intermediateVectors) {
            for (auto& pair : vec) {
                grouped[pair.first].push_back(pair);
                job->state.fetch_add(1ULL << 2); // processed++
            }
        }
        job->shuffledVectors.clear();
        for (auto& entry : grouped) {
            job->shuffledVectors.push_back(std::move(entry.second));
        }
    }

    job->barrier->barrier(); // Wait for shuffle

    // Reduce phase
    if (context->threadId == 0) {
        job->state.store(STATE_PACK(REDUCE_STAGE, 0, job->shuffledVectors.size()));
    }
    job->barrier->barrier();

    while (true) {
        size_t idx = job->reduceIndex.fetch_add(1);
        if (idx >= job->shuffledVectors.size()) break;
        
        // Process the reduce FIRST
        job->client.reduce(&job->shuffledVectors[idx], context);

        // THEN update the state
        uint64_t old_state = job->state.load();
        while (true) {
            unsigned stage;
            size_t processed, total;
            STATE_UNPACK(stage, processed, total, old_state);
            uint64_t new_state = STATE_PACK(stage, processed + 1, total);
            if (job->state.compare_exchange_weak(old_state, new_state)) {
                break;
            }
        }
    }

    job->barrier->barrier(); // Wait for all threads to finish reduce
}


void emit2(K2* key, V2* value, void* context) {
    ThreadContext* threadContext = static_cast<ThreadContext*>(context);
    if (!threadContext) return;
    threadContext->intermediateVec->emplace_back(key, value);
}

void emit3(K3* key, V3* value, void* context) {
    ThreadContext* threadContext = static_cast<ThreadContext*>(context);
    if (!threadContext) return;
    JobFrameWork* job = threadContext->job;
    std::lock_guard<std::mutex> lock(job->outputMutex);
    job->outputVec.emplace_back(key, value);
}

void waitForJob(JobHandle jobHandle) {
    auto* job = static_cast<JobFrameWork*>(jobHandle);
    if (!job->joined.load()) {
        std::lock_guard<std::mutex> lock(job->joinMutex);
        if (!job->joined.load()) {
            for (auto& t : job->threads) {
                if (t.joinable()) {
                    t.join();
                }
            }
            job->joined.store(true);
        }
    }
}

void closeJobHandle(JobHandle jobHandle) {
    waitForJob(jobHandle);
    auto* job = static_cast<JobFrameWork*>(jobHandle);
    delete job;
}


void getJobState(JobHandle jobHandle, JobState* state) {
    auto* job = static_cast<JobFrameWork*>(jobHandle);
    uint64_t packed = job->state.load();
    unsigned stage;
    size_t processed, total;
    STATE_UNPACK(stage, processed, total, packed);
    state->stage = static_cast<stage_t>(stage);
    state->percentage = (total == 0) ? 100.0f : 
        (100.0f * static_cast<float>(std::min(processed, total)) / total);
}