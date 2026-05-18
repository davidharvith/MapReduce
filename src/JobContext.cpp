#include "internal/JobFramework.h"
#include "internal/State.h"

#include "Barrier.h"

void JobFramework::setError(int code) {
    errorCode.store(code, std::memory_order_release);
    failed.store(true, std::memory_order_release);
}

JobFramework::JobFramework(const MapReduceClient& client,
                           const InputVec& inputVec,
                           OutputVec& outputVec,
                           int numThreads)
    : client(client),
      inputVec(inputVec),
      outputVec(outputVec),
      numThreads(numThreads) {
    intermediateVectors.resize(numThreads);
    threadContexts.resize(numThreads, nullptr);
    state.store(STATE_PACK(UNDEFINED_STAGE, 0, inputVec.size()));
}

JobFramework::~JobFramework() {
    delete barrier;
    for (auto* ctx : threadContexts) {
        delete ctx;
    }
}

ThreadContext::ThreadContext(int id, JobFramework* job)
    : threadId(id), job(job), intermediateVec(&job->intermediateVectors[id]) {}
