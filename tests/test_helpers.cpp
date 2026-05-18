#include "test_helpers.h"

#include <cstdlib>
#include <map>

void CountClient::map(const K1* key, const V1* /*val*/, void* context) const {
    const int input = (static_cast<const CountElement*>(key)->num) % kUniqueKeys;
    emit2(new CountElement(input), new CountElement(1), context);
}

void CountClient::reduce(const IntermediateVec* pairs, void* context) const {
    emit3(new CountElement(static_cast<CountElement*>(pairs->at(0).first)->num),
          new CountElement(static_cast<int>(pairs->size())),
          context);
    for (const IntermediatePair& pair : *pairs) {
        delete pair.first;
        delete pair.second;
    }
}

void buildCountWorkload(unsigned seed,
                        int inputCount,
                        InputVec& input,
                        std::vector<int>& expectedByKey) {
    expectedByKey.assign(kUniqueKeys, 0);
    std::srand(seed);
    input.clear();
    input.reserve(static_cast<size_t>(inputCount));
    for (int j = 0; j < inputCount; ++j) {
        const int r = std::rand();
        expectedByKey[static_cast<size_t>(r % kUniqueKeys)]++;
        input.push_back({new CountElement(r), nullptr});
    }
}

void freeCountInput(InputVec& input) {
    for (auto& p : input) {
        delete p.first;
        delete p.second;
    }
    input.clear();
}

void freeCountOutput(OutputVec& output) {
    for (auto& p : output) {
        delete p.first;
        delete p.second;
    }
    output.clear();
}

std::map<int, int> collectCountOutput(const OutputVec& output) {
    std::map<int, int> result;
    for (const auto& p : output) {
        result[static_cast<const CountElement*>(p.first)->num] =
            static_cast<const CountElement*>(p.second)->num;
    }
    return result;
}
