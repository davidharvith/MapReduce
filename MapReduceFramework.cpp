#include <iostream>
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include <thread>
#include "MapReduceFramework.h"
#include "GeneralContext.h"  // Contains JobFrameWork and ThreadContext
#include "Barrier.h"        // Contains Barrier class
#include "errors.h"
#include <algorithm>
#include <map>

void threadMain(ThreadContext* context);

struct K2PtrLess {
    bool operator()(const K2* a, const K2* b) const {
        return *a < *b;
    }
};

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

    job->stage.store(MAP_STAGE);
    job->percentage.store(0.0f);

    return static_cast<JobHandle>(job);
}


void threadMain(ThreadContext* context){
    JobFrameWork* job = context->job;
    int threadId = context->threadId;

    // Map phase
    while (true) {
        size_t inputIndex = job->inputIndex.fetch_add(1);
        if (inputIndex >= job->inputVec.size()) {
            break;
        }
        const K1* key = job->inputVec[inputIndex].first;
        const V1* value = job->inputVec[inputIndex].second;

        job->client.map(key, value, context);
    }


    //sort phase
    std::sort(
        context->intermediateVec->begin(),
        context->intermediateVec->end(),
        [](const IntermediatePair& a, const IntermediatePair& b) {
            return *(a.first) < *(b.first);
        }
    );
    

    // Wait for all threads to finish map phase
    job->barrier->barrier();

    
    // Shuffle phase
    if (context->threadId == 0) {
        job->stage.store(SHUFFLE_STAGE);
    
        // Group all pairs by key
        std::map<K2*, std::vector<IntermediatePair>,K2PtrLess> grouped;
        for (auto& vec : job->intermediateVectors) {
            for (auto& pair : vec) {
                grouped[pair.first].push_back(pair);
            }
        }
    
        // Move each group into shuffledVectors
        job->shuffledVectors.clear();
        for (auto& entry : grouped) {
            job->shuffledVectors.push_back(std::move(entry.second));
        }
    }
    

    // Wait for all threads to finish shuffle phase
    job->barrier->barrier();

    if (job->stage.load() == SHUFFLE_STAGE) {
        job->stage.store(REDUCE_STAGE);
    
        while (true) {
            size_t idx = job->reduceIndex.fetch_add(1);
            if (idx >= job->shuffledVectors.size()) {
                break;
            }
            job->client.reduce(&job->shuffledVectors[idx], context);
        }
    }
    job->barrier->barrier(); // Wait for all threads to finish reduce
    

    // Mark the thread as finished
    if (--job->numThreads == 0) {
        job->finished.store(true);
    }

    job->barrier->barrier(); // Wait for all threads to finish reduce
}