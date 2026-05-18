#include <gtest/gtest.h>

#include "test_helpers.h"

#include <mapreduce/MapReduceFramework.h>

TEST(JobLifecycleTest, StartWaitCloseSucceeds) {
    InputVec input;
    OutputVec output;
    std::vector<int> expected;
    buildCountWorkload(0, 1000, input, expected);

    CountClient client;
    JobHandle job = startMapReduceJob(client, input, output, 2);
    ASSERT_NE(job, nullptr);

    JobState state{};
    getJobState(job, &state);
    EXPECT_GE(state.stage, UNDEFINED_STAGE);

    waitForJob(job);
    const int err = getJobError(job);
    EXPECT_EQ(err, JOB_OK);
    closeJobHandle(job);
    freeCountInput(input);
    freeCountOutput(output);
}

TEST(JobLifecycleTest, InvalidThreadLevelReturnsNull) {
    InputVec input;
    OutputVec output;
    CountClient client;
    EXPECT_EQ(startMapReduceJob(client, input, output, 0), nullptr);
}
