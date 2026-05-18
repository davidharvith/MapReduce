#ifndef MAPREDUCE_MAPREDUCEFRAMEWORK_H
#define MAPREDUCE_MAPREDUCEFRAMEWORK_H

#include "MapReduceClient.h"

typedef void* JobHandle;

enum stage_t { UNDEFINED_STAGE = 0, MAP_STAGE = 1, SHUFFLE_STAGE = 2, REDUCE_STAGE = 3 };

typedef struct {
    stage_t stage;
    float percentage;
} JobState;

/// Emit an intermediate (K2, V2) pair from map. Lock-free per-thread buffer.
void emit2(K2* key, V2* value, void* context);

/// Emit a final (K3, V3) pair from reduce. Mutex-protected output vector.
void emit3(K3* key, V3* value, void* context);

/// Start a job with the given thread count. Returns nullptr on failure.
JobHandle startMapReduceJob(const MapReduceClient& client,
                            const InputVec& inputVec,
                            OutputVec& outputVec,
                            int multiThreadLevel);

void waitForJob(JobHandle job);
void getJobState(JobHandle job, JobState* state);

/// Returns JOB_OK or a JobErrorCode after the job has finished.
int getJobError(JobHandle job);

void closeJobHandle(JobHandle job);

#endif
