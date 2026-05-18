#include "Barrier.h"

Barrier::Barrier(int numThreads) : numThreads_(numThreads) {}

void Barrier::barrier() {
    std::unique_lock<std::mutex> lock(mutex_);
    const int gen = generation_;

    if (++count_ < numThreads_) {
        cv_.wait(lock, [this, gen] { return gen != generation_; });
    } else {
        count_ = 0;
        ++generation_;
        cv_.notify_all();
    }
}
