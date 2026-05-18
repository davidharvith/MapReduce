#ifndef MAPREDUCE_INTERNAL_PHASES_H
#define MAPREDUCE_INTERNAL_PHASES_H

struct JobFramework;
struct ThreadContext;

void runMapPhase(ThreadContext* context);
bool runSortPhase(ThreadContext* context);
bool runShufflePhase(ThreadContext* context);
void beginReduceStage(JobFramework* job);
void runReduceWork(ThreadContext* context);
bool syncBarrier(JobFramework* job);

#endif
