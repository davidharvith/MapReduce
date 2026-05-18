#include <gtest/gtest.h>

#include "test_helpers.h"

#include <mapreduce/MapReduceFramework.h>

TEST(ConcurrentJobsTest, FourJobsInOneProcess) {
    constexpr int kJobs = 4;
    CountClient client;
    std::vector<InputVec> inputs(kJobs);
    std::vector<OutputVec> outputs(kJobs);
    std::vector<std::vector<int>> expected(kJobs);
    std::vector<JobHandle> jobs(kJobs);

    for (int i = 0; i < kJobs; ++i) {
        buildCountWorkload(static_cast<unsigned>(i), 10000, inputs[i], expected[i]);
        jobs[i] = startMapReduceJob(client, inputs[i], outputs[i], 1);
        ASSERT_NE(jobs[i], nullptr);
    }

    for (int i = 0; i < kJobs; ++i) {
        waitForJob(jobs[i]);
        EXPECT_EQ(getJobError(jobs[i]), JOB_OK);
        closeJobHandle(jobs[i]);

        const auto got = collectCountOutput(outputs[i]);
        for (size_t k = 0; k < expected[i].size(); ++k) {
            if (expected[i][k] == 0) {
                continue;
            }
            EXPECT_EQ(got.at(static_cast<int>(k)), expected[i][k]);
        }
        freeCountInput(inputs[i]);
        freeCountOutput(outputs[i]);
    }
}
