#include "mapreduce/MapReduceFramework.h"

#include <algorithm>
#include <thread>

#include "internal/JobFramework.h"
#include "internal/Phases.h"
#include "internal/State.h"

#include "Barrier.h"
#include "mapreduce/errors.h"

namespace {

void threadMain(ThreadContext* context) {
    JobFramework* job = context->job;

    runMapPhase(context);
    runSortPhase(context);

    if (!syncBarrier(job)) {
        return;
    }

    runShufflePhase(context);

    if (!syncBarrier(job)) {
        return;
    }

    if (context->threadId == 0) {
        beginReduceStage(job);
    }

    if (!syncBarrier(job)) {
        return;
    }

    runReduceWork(context);

    syncBarrier(job);
}

}  // namespace

JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec,
                            OutputVec& outputVec,
                            int multiThreadLevel) {
    if (multiThreadLevel < 1) {
        return nullptr;
    }

    JobFramework* job = nullptr;
    try {
        job = new JobFramework(client, inputVec, outputVec, multiThreadLevel);
        job->barrier = new Barrier(multiThreadLevel);
    } catch (const std::bad_alloc&) {
        delete job;
        return nullptr;
    }

    try {
        job->threadContexts.resize(multiThreadLevel);
        for (int i = 0; i < multiThreadLevel; ++i) {
            job->threadContexts[i] = new ThreadContext(i, job);
            job->threads.emplace_back(threadMain, job->threadContexts[i]);
        }
    } catch (const std::bad_alloc&) {
        job->setError(JOB_ERR_BAD_ALLOCATION);
        waitForJob(job);
        delete job;
        return nullptr;
    } catch (...) {
        job->setError(JOB_ERR_THREAD_CREATION);
        waitForJob(job);
        delete job;
        return nullptr;
    }

    return static_cast<JobHandle>(job);
}

void emit2(K2* key, V2* value, void* context) {
    auto* threadContext = static_cast<ThreadContext*>(context);
    if (!threadContext) {
        return;
    }
    threadContext->intermediateVec->emplace_back(key, value);
}

void emit3(K3* key, V3* value, void* context) {
    auto* threadContext = static_cast<ThreadContext*>(context);
    if (!threadContext) {
        return;
    }
    JobFramework* job = threadContext->job;
    std::lock_guard<std::mutex> lock(job->outputMutex);
    job->outputVec.emplace_back(key, value);
}

void waitForJob(JobHandle jobHandle) {
    if (!jobHandle) {
        return;
    }
    auto* job = static_cast<JobFramework*>(jobHandle);
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
    delete static_cast<JobFramework*>(jobHandle);
}

void getJobState(JobHandle jobHandle, JobState* state) {
    if (!jobHandle || !state) {
        return;
    }
    auto* job = static_cast<JobFramework*>(jobHandle);
    const uint64_t packed = job->state.load();
    unsigned stage = 0;
    size_t processed = 0;
    size_t total = 0;
    STATE_UNPACK(stage, processed, total, packed);
    state->stage = static_cast<stage_t>(stage);
    state->percentage = (total == 0)
                              ? 100.0f
                              : (100.0f * static_cast<float>(std::min(processed, total)) /
                                 static_cast<float>(total));
}

int getJobError(JobHandle jobHandle) {
    if (!jobHandle) {
        return JOB_ERR_BAD_ALLOCATION;
    }
    auto* job = static_cast<JobFramework*>(jobHandle);
    return job->errorCode.load(std::memory_order_acquire);
}
