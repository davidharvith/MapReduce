#include <gtest/gtest.h>

#include "test_helpers.h"

#include <algorithm>
#include <fstream>
#include <mapreduce/MapReduceFramework.h>
#include <sstream>
#include <string>

namespace {

std::vector<int> loadGoldenValues(const std::string& path) {
    std::ifstream in(path);
    if (!in) {
        return {};
    }
    std::vector<int> values;
    std::string line;
    while (std::getline(in, line)) {
        const auto tab = line.find('\t');
        if (tab == std::string::npos) {
            continue;
        }
        values.push_back(std::stoi(line.substr(tab + 1)));
    }
    std::sort(values.begin(), values.end());
    return values;
}

}  // namespace

TEST(GoldenRegressionTest, MatchesLegacySingleJobOutput) {
    const std::string golden =
        std::string(MAPREDUCE_SOURCE_DIR) + "/tests/golden/test1_single_job_1_thread.txt";

    InputVec input;
    OutputVec output;
    std::vector<int> expected;
    buildCountWorkload(0, kDefaultInputCount, input, expected);

    CountClient client;
    JobHandle job = startMapReduceJob(client, input, output, 1);
    ASSERT_NE(job, nullptr);
    closeJobHandle(job);

    std::vector<int> got;
    for (const auto& p : output) {
        got.push_back(static_cast<const CountElement*>(p.second)->num);
    }
    std::sort(got.begin(), got.end());

    const auto goldenVals = loadGoldenValues(golden);
    if (goldenVals.empty()) {
        GTEST_SKIP() << "Golden file not found: " << golden;
    }

    ASSERT_EQ(got.size(), goldenVals.size());
    for (size_t i = 0; i < got.size(); ++i) {
        EXPECT_EQ(got[i], goldenVals[i]) << "at index " << i;
    }

    freeCountInput(input);
    freeCountOutput(output);
}
