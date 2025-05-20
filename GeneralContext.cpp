#include "GeneralContext.h"

// std::mutex JobFrameWork::joinMutex;

JobFrameWork::JobFrameWork(const MapReduceClient& client,
                           const InputVec& inputVec,
                           OutputVec& outputVec,
                           int numThreads)
    : client(client), inputVec(inputVec), outputVec(outputVec), numThreads(numThreads)
{
    intermediateVectors.resize(numThreads);
    threadContexts.resize(numThreads, nullptr);
    // INITIALIZE STATE FOR MAP PHASE
    const uint64_t init_state = STATE_PACK(UNDEFINED_STAGE, 0, inputVec.size()); //  added this line
    state.store(init_state);


}

JobFrameWork::~JobFrameWork() {
    delete barrier;
    for (auto ctx : threadContexts)
        delete ctx;
}

ThreadContext::ThreadContext(int id, JobFrameWork* job)
    : threadId(id), job(job), intermediateVec(&(job->intermediateVectors[id]))
{}
