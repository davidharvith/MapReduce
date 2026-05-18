#include <gtest/gtest.h>

#include "Barrier.h"

#include <atomic>
#include <thread>
#include <vector>

TEST(BarrierTest, AllThreadsSynchronizeMultipleRounds) {
    constexpr int kThreads = 8;
    constexpr int kRounds = 50;
    Barrier barrier(kThreads);
    std::atomic<int> counter{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&]() {
            for (int r = 0; r < kRounds; ++r) {
                barrier.barrier();
                counter.fetch_add(1, std::memory_order_relaxed);
                barrier.barrier();
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }
    EXPECT_EQ(counter.load(), kThreads * kRounds);
}

TEST(BarrierTest, SingleThreadPassesImmediately) {
    Barrier barrier(1);
    for (int i = 0; i < 10; ++i) {
        barrier.barrier();
    }
}
