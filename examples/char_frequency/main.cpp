#include <mapreduce/MapReduceClient.h>
#include <mapreduce/MapReduceFramework.h>

#include <array>
#include <cstdio>
#include <iostream>
#include <string>

class VString : public V1 {
public:
    explicit VString(std::string content) : content(std::move(content)) {}
    std::string content;
};

class KChar : public K2, public K3 {
public:
    explicit KChar(char c) : c(c) {}
    bool operator<(const K2& other) const override {
        return c < static_cast<const KChar&>(other).c;
    }
    bool operator<(const K3& other) const override {
        return c < static_cast<const KChar&>(other).c;
    }
    char c;
};

class VCount : public V2, public V3 {
public:
    explicit VCount(int count) : count(count) {}
    int count;
};

class CounterClient : public MapReduceClient {
public:
    void map(const K1* /*key*/, const V1* value, void* context) const override {
        std::array<int, 256> counts{};
        for (const char ch : static_cast<const VString*>(value)->content) {
            counts[static_cast<unsigned char>(ch)]++;
        }
        for (int i = 0; i < 256; ++i) {
            if (counts[i] == 0) {
                continue;
            }
            emit2(new KChar(static_cast<char>(i)), new VCount(counts[i]), context);
        }
    }

    void reduce(const IntermediateVec* pairs, void* context) const override {
        const char c = static_cast<const KChar*>(pairs->at(0).first)->c;
        int count = 0;
        for (const IntermediatePair& pair : *pairs) {
            count += static_cast<const VCount*>(pair.second)->count;
            delete pair.first;
            delete pair.second;
        }
        emit3(new KChar(c), new VCount(count), context);
    }
};

int main(int argc, char** argv) {
    const bool pollProgress = (argc > 1 && std::string(argv[1]) == "--poll-progress");

    CounterClient client;
    InputVec inputVec;
    OutputVec outputVec;
    VString s1("This string is full of characters");
    VString s2("Multithreading is awesome");
    VString s3("race conditions are bad");
    inputVec.push_back({nullptr, &s1});
    inputVec.push_back({nullptr, &s2});
    inputVec.push_back({nullptr, &s3});

    JobState state{};
    JobState lastState{UNDEFINED_STAGE, 0};
    JobHandle job = startMapReduceJob(client, inputVec, outputVec, 4);
    if (!job) {
        std::cerr << "Failed to start job.\n";
        return 1;
    }

    if (pollProgress) {
        getJobState(job, &state);
        while (state.stage != REDUCE_STAGE || state.percentage < 100.0f) {
            if (lastState.stage != state.stage || lastState.percentage != state.percentage) {
                printf("stage %d, %.1f%%\n", state.stage, state.percentage);
            }
            lastState = state;
            getJobState(job, &state);
        }
        printf("stage %d, %.1f%%\n", state.stage, state.percentage);
    } else {
        waitForJob(job);
    }

    closeJobHandle(job);

    for (OutputPair& pair : outputVec) {
        const char c = static_cast<const KChar*>(pair.first)->c;
        const int count = static_cast<const VCount*>(pair.second)->count;
        if (count > 0 && c >= 32) {
            printf("The character '%c' appeared %d time%s\n", c, count, count > 1 ? "s" : "");
        }
        delete pair.first;
        delete pair.second;
    }

    return 0;
}
