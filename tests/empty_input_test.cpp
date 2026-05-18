#include <gtest/gtest.h>

#include "test_helpers.h"

#include <mapreduce/MapReduceFramework.h>

TEST(EmptyInputTest, ProducesNoOutputAndCompletes) {
    InputVec input;
    OutputVec output;
    CountClient client;

    JobHandle job = startMapReduceJob(client, input, output, 4);
    ASSERT_NE(job, nullptr);
    waitForJob(job);
    EXPECT_EQ(getJobError(job), JOB_OK);
    closeJobHandle(job);
    EXPECT_TRUE(output.empty());
}
