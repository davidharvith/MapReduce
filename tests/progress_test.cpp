#include <gtest/gtest.h>

#include "test_helpers.h"

#include <mapreduce/MapReduceFramework.h>

#include <chrono>
#include <thread>

TEST(ProgressTest, StageTransitionsMonotonicPercentage) {
    InputVec input;
    OutputVec output;
    std::vector<int> expected;
    buildCountWorkload(0, 50000, input, expected);

    CountClient client;
    JobHandle job = startMapReduceJob(client, input, output, 4);
    ASSERT_NE(job, nullptr);

    JobState state{};
    getJobState(job, &state);

    float lastPct = -1.0f;
    int lastStage = UNDEFINED_STAGE;
    bool sawShuffle = false;
    bool sawReduce = false;

    while (state.stage != REDUCE_STAGE || state.percentage < 100.0f) {
        if (state.stage == SHUFFLE_STAGE) {
            sawShuffle = true;
        }
        if (state.stage == REDUCE_STAGE) {
            sawReduce = true;
        }
        if (state.stage == lastStage) {
            EXPECT_GE(state.percentage, lastPct - 0.01f);
        }
        lastPct = state.percentage;
        lastStage = state.stage;
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
        getJobState(job, &state);
    }

    closeJobHandle(job);
    EXPECT_TRUE(sawShuffle || input.empty());
    EXPECT_TRUE(sawReduce);

    freeCountInput(input);
    freeCountOutput(output);
}
