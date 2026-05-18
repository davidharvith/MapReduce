#include <gtest/gtest.h>

#include "test_helpers.h"

#include <mapreduce/MapReduceFramework.h>

TEST(CorrectnessCountTest, SingleThreadMatchesExpectedCounts) {
    InputVec input;
    OutputVec output;
    std::vector<int> expected;
    buildCountWorkload(0, kDefaultInputCount, input, expected);

    CountClient client;
    JobHandle job = startMapReduceJob(client, input, output, 1);
    ASSERT_NE(job, nullptr);
    waitForJob(job);
    ASSERT_EQ(getJobError(job), JOB_OK);
    closeJobHandle(job);

    const auto got = collectCountOutput(output);
    ASSERT_EQ(got.size(), expected.size());

    for (size_t k = 0; k < expected.size(); ++k) {
        if (expected[k] == 0) {
            EXPECT_EQ(got.count(static_cast<int>(k)), 0u);
        } else {
            ASSERT_NE(got.find(static_cast<int>(k)), got.end());
            EXPECT_EQ(got.at(static_cast<int>(k)), expected[k]);
        }
    }

    freeCountInput(input);
    freeCountOutput(output);
}
