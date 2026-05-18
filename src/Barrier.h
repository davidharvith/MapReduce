#ifndef MAPREDUCE_BARRIER_H
#define MAPREDUCE_BARRIER_H

#include <condition_variable>
#include <mutex>

/// Generation-based barrier: the generation counter prevents spurious wakeups
/// from releasing threads from a previous round.
class Barrier {
public:
    explicit Barrier(int numThreads);
    void barrier();

private:
    std::mutex mutex_;
    std::condition_variable cv_;
    int count_{0};
    int generation_{0};
    const int numThreads_;
};

#endif
