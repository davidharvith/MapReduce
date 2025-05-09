#include "GeneralContext.h"

JobFrameWork::JobFrameWork(const MapReduceClient& client,
                           const InputVec& inputVec,
                           OutputVec& outputVec,
                           int numThreads)
    : client(client), inputVec(inputVec), outputVec(outputVec), numThreads(numThreads)
{
    intermediateVectors.resize(numThreads);
    threadContexts.resize(numThreads, nullptr);
}

JobFrameWork::~JobFrameWork() {
    delete barrier;
    for (auto ctx : threadContexts)
        delete ctx;
}

ThreadContext::ThreadContext(int id, JobFrameWork* job)
    : threadId(id), job(job), intermediateVec(&(job->intermediateVectors[id]))
{}
