#include "Phases.h"

#include <algorithm>
#include <map>

#include "JobFramework.h"
#include "State.h"

#include "Barrier.h"
#include "mapreduce/errors.h"

namespace {

struct K2PtrLess {
    bool operator()(const K2* a, const K2* b) const { return *a < *b; }
};

}  // namespace

bool syncBarrier(JobFramework* job) {
    if (job->failed.load(std::memory_order_acquire)) {
        try {
            job->barrier->barrier();
            return true;
        } catch (const std::system_error&) {
            job->setError(JOB_ERR_BARRIER);
            return false;
        }
    }
    try {
        job->barrier->barrier();
        return true;
    } catch (const std::system_error&) {
        job->setError(JOB_ERR_BARRIER);
        return false;
    }
}

void runMapPhase(ThreadContext* context) {
    JobFramework* job = context->job;
    if (job->failed.load(std::memory_order_acquire)) {
        return;
    }

    ensureMapStage(job->state);

    while (!job->failed.load(std::memory_order_acquire)) {
        const size_t inputIndex = job->inputIndex.fetch_add(1);
        if (inputIndex >= job->inputVec.size()) {
            break;
        }

        const K1* key = job->inputVec[inputIndex].first;
        const V1* value = job->inputVec[inputIndex].second;
        job->client.map(key, value, context);
        tryIncrementProgress(job->state);
    }
}

bool runSortPhase(ThreadContext* context) {
    JobFramework* job = context->job;
    if (job->failed.load(std::memory_order_acquire)) {
        return false;
    }

    try {
        std::sort(context->intermediateVec->begin(),
                  context->intermediateVec->end(),
                  [](const IntermediatePair& a, const IntermediatePair& b) {
                      return *(a.first) < *(b.first);
                  });
        return true;
    } catch (...) {
        job->setError(JOB_ERR_SORT);
        return false;
    }
}

bool runShufflePhase(ThreadContext* context) {
    JobFramework* job = context->job;
    if (context->threadId != 0 || job->failed.load(std::memory_order_acquire)) {
        return true;
    }

    size_t totalPairs = 0;
    for (const auto& vec : job->intermediateVectors) {
        totalPairs += vec.size();
    }

    if (totalPairs == 0) {
        job->state.store(STATE_PACK(REDUCE_STAGE, 0, 0));
        job->shuffledVectors.clear();
        return true;
    }

    job->state.store(STATE_PACK(SHUFFLE_STAGE, 0, totalPairs));

    std::map<K2*, std::vector<IntermediatePair>, K2PtrLess> grouped;
    try {
        for (auto& vec : job->intermediateVectors) {
            for (auto& pair : vec) {
                grouped[pair.first].push_back(pair);
                tryIncrementProgress(job->state);
            }
        }
    } catch (const std::bad_alloc&) {
        job->setError(JOB_ERR_BAD_ALLOCATION);
        return false;
    } catch (...) {
        job->setError(JOB_ERR_SHUFFLE);
        return false;
    }

    try {
        job->shuffledVectors.clear();
        for (auto& entry : grouped) {
            job->shuffledVectors.push_back(std::move(entry.second));
        }
        return true;
    } catch (const std::bad_alloc&) {
        job->setError(JOB_ERR_BAD_ALLOCATION);
        return false;
    } catch (...) {
        job->setError(JOB_ERR_VECTOR);
        return false;
    }
}

void beginReduceStage(JobFramework* job) {
    if (job->shuffledVectors.empty()) {
        return;
    }
    job->state.store(STATE_PACK(REDUCE_STAGE, 0, job->shuffledVectors.size()));
}

void runReduceWork(ThreadContext* context) {
    JobFramework* job = context->job;
    if (job->failed.load(std::memory_order_acquire) || job->shuffledVectors.empty()) {
        return;
    }

    while (!job->failed.load(std::memory_order_acquire)) {
        const size_t idx = job->reduceIndex.fetch_add(1);
        if (idx >= job->shuffledVectors.size()) {
            break;
        }

        try {
            job->client.reduce(&job->shuffledVectors[idx], context);
            tryIncrementProgress(job->state);
        } catch (...) {
            job->setError(JOB_ERR_REDUCE);
            return;
        }
    }
}
