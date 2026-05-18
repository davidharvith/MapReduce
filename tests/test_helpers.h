#ifndef MAPREDUCE_TEST_HELPERS_H
#define MAPREDUCE_TEST_HELPERS_H

#include <mapreduce/MapReduceClient.h>
#include <mapreduce/MapReduceFramework.h>
#include <mapreduce/errors.h>

#include <map>
#include <vector>

constexpr unsigned kUniqueKeys = 100;
constexpr int kDefaultInputCount = 100000;

class CountElement : public K1, public K2, public K3, public V1, public V2, public V3 {
public:
    explicit CountElement(int n) : num(n) {}

    bool operator<(const K1& other) const override {
        return num < dynamic_cast<const CountElement&>(other).num;
    }
    bool operator<(const K2& other) const override {
        return num < dynamic_cast<const CountElement&>(other).num;
    }
    bool operator<(const K3& other) const override {
        return num < dynamic_cast<const CountElement&>(other).num;
    }
    bool operator<(const V1& other) const {
        return num < dynamic_cast<const CountElement&>(other).num;
    }
    bool operator<(const V2& other) const {
        return num < dynamic_cast<const CountElement&>(other).num;
    }
    bool operator<(const V3& other) const {
        return num < dynamic_cast<const CountElement&>(other).num;
    }

    int num;
};

class CountClient : public MapReduceClient {
public:
    void map(const K1* key, const V1* val, void* context) const override;
    void reduce(const IntermediateVec* pairs, void* context) const override;
};

/// Build deterministic input (srand seed) and expected counts per key mod kUniqueKeys.
void buildCountWorkload(unsigned seed,
                        int inputCount,
                        InputVec& input,
                        std::vector<int>& expectedByKey);

void freeCountInput(InputVec& input);
void freeCountOutput(OutputVec& output);

std::map<int, int> collectCountOutput(const OutputVec& output);

#endif
