#include <gtest/gtest.h>

#include "test_helpers.h"

#include <mapreduce/MapReduceFramework.h>

class MultiThreadTest : public ::testing::TestWithParam<int> {};

TEST_P(MultiThreadTest, SameResultAcrossThreadCounts) {
    InputVec input;
    OutputVec output;
    std::vector<int> expected;
    buildCountWorkload(42, 50000, input, expected);

    CountClient client;
    JobHandle job = startMapReduceJob(client, input, output, GetParam());
    ASSERT_NE(job, nullptr);
    waitForJob(job);
    ASSERT_EQ(getJobError(job), JOB_OK);
    closeJobHandle(job);

    const auto got = collectCountOutput(output);
    for (size_t k = 0; k < expected.size(); ++k) {
        if (expected[k] == 0) {
            continue;
        }
        ASSERT_EQ(got.at(static_cast<int>(k)), expected[k]);
    }

    freeCountInput(input);
    freeCountOutput(output);
}

INSTANTIATE_TEST_SUITE_P(ThreadLevels,
                         MultiThreadTest,
                         ::testing::Values(1, 2, 4, 8));
