#include "Phases.h"

#include <algorithm>
#include <utility>
#include <vector>

#include "JobFramework.h"
#include "ShuffleTypes.h"
#include "State.h"

#include "Barrier.h"
#include "mapreduce/errors.h"

namespace {

void mergeSortedRuns(std::vector<IntermediatePair>& dest, std::vector<IntermediatePair>& src) {
    if (src.empty()) {
        return;
    }
    if (dest.empty()) {
        dest = std::move(src);
        return;
    }

    std::vector<IntermediatePair> merged;
    merged.reserve(dest.size() + src.size());
    std::merge(dest.begin(),
               dest.end(),
               src.begin(),
               src.end(),
               std::back_inserter(merged),
               [](const IntermediatePair& a, const IntermediatePair& b) {
                   return *(a.first) < *(b.first);
               });
    dest = std::move(merged);
    src.clear();
}

void groupSortedIntoShuffled(const std::vector<IntermediatePair>& sorted,
                             std::vector<std::vector<IntermediatePair>>& groups,
                             std::atomic<size_t>* progress) {
    groups.clear();
    if (sorted.empty()) {
        return;
    }

    groups.emplace_back();
    groups.back().push_back(sorted.front());
    if (progress) {
        progress->fetch_add(1, std::memory_order_relaxed);
    }

    for (size_t i = 1; i < sorted.size(); ++i) {
        const IntermediatePair& pair = sorted[i];
        if (K2PtrLess{}(groups.back().back().first, pair.first)) {
            groups.emplace_back();
        }
        groups.back().push_back(pair);
        if (progress) {
            progress->fetch_add(1, std::memory_order_relaxed);
        }
    }
}

}  // namespace

bool syncBarrier(JobFramework* job) {
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
    const int tid = context->threadId;
    if (job->failed.load(std::memory_order_acquire)) {
        return true;
    }

    size_t totalPairs = 0;
    for (const auto& vec : job->intermediateVectors) {
        totalPairs += vec.size();
    }

    if (totalPairs == 0) {
        if (tid == 0) {
            job->state.store(STATE_PACK(REDUCE_STAGE, 0, 0));
            job->shuffledVectors.clear();
        }
        return true;
    }

    if (tid == 0) {
        job->shuffleProgress.store(0, std::memory_order_relaxed);
        job->state.store(STATE_PACK(SHUFFLE_STAGE, 0, totalPairs));
    }

    std::vector<IntermediatePair>& local = job->intermediateVectors[tid];

    for (int stride = 1; stride < job->numThreads; stride <<= 1) {
        if (tid % (2 * stride) == 0) {
            const int partner = tid + stride;
            if (partner < job->numThreads) {
                try {
                    mergeSortedRuns(local, job->intermediateVectors[partner]);
                } catch (const std::bad_alloc&) {
                    job->setError(JOB_ERR_BAD_ALLOCATION);
                    return false;
                } catch (...) {
                    job->setError(JOB_ERR_SHUFFLE);
                    return false;
                }
            }
        }
        if (!syncBarrier(job)) {
            return false;
        }
        if (tid == 0) {
            const size_t done =
                std::min(job->shuffleProgress.load(std::memory_order_relaxed), totalPairs);
            job->state.store(STATE_PACK(SHUFFLE_STAGE, done, totalPairs));
        }
    }

    if (tid == 0) {
        try {
            groupSortedIntoShuffled(local, job->shuffledVectors, &job->shuffleProgress);
            job->state.store(STATE_PACK(SHUFFLE_STAGE, totalPairs, totalPairs));
        } catch (const std::bad_alloc&) {
            job->setError(JOB_ERR_BAD_ALLOCATION);
            return false;
        } catch (...) {
            job->setError(JOB_ERR_VECTOR);
            return false;
        }
    }

    if (!syncBarrier(job)) {
        return false;
    }
    return true;
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
