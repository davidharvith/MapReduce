#include <mapreduce/MapReduceClient.h>
#include <mapreduce/MapReduceFramework.h>

#include <chrono>
#include <cstdlib>
#include <iostream>
#include <thread>

class IntKey : public K1, public K2, public K3 {
public:
    explicit IntKey(int v) : v(v) {}
    bool operator<(const K1& o) const override { return v < static_cast<const IntKey&>(o).v; }
    bool operator<(const K2& o) const override { return v < static_cast<const IntKey&>(o).v; }
    bool operator<(const K3& o) const override { return v < static_cast<const IntKey&>(o).v; }
    int v;
};

class IntVal : public V1, public V2, public V3 {
public:
    explicit IntVal(int v) : v(v) {}
    int v;
};

class IdentityClient : public MapReduceClient {
public:
    void map(const K1* key, const V1* /*val*/, void* ctx) const override {
        const int k = static_cast<const IntKey*>(key)->v;
        emit2(new IntKey(k), new IntVal(1), ctx);
    }

    void reduce(const IntermediateVec* pairs, void* ctx) const override {
        int sum = 0;
        const int k = static_cast<const IntKey*>(pairs->at(0).first)->v;
        for (const auto& p : *pairs) {
            sum += static_cast<const IntVal*>(p.second)->v;
            delete p.first;
            delete p.second;
        }
        emit3(new IntKey(k), new IntVal(sum), ctx);
    }
};

static int parseThreads(int argc, char** argv) {
    for (int i = 1; i + 1 < argc; ++i) {
        if (std::string(argv[i]) == "--threads") {
            return std::atoi(argv[i + 1]);
        }
    }
    return static_cast<int>(std::thread::hardware_concurrency());
}

int main(int argc, char** argv) {
    const int threads = parseThreads(argc, argv);
    const int inputCount = 500000;

    InputVec input;
    input.reserve(inputCount);
    for (int i = 0; i < inputCount; ++i) {
        input.push_back({new IntKey(i % 10000), new IntVal(i)});
    }

    OutputVec output;
    IdentityClient client;

    const auto t0 = std::chrono::steady_clock::now();
    JobHandle job = startMapReduceJob(client, input, output, threads);
    if (!job) {
        std::cerr << "job start failed\n";
        return 1;
    }
    closeJobHandle(job);
    const auto t1 = std::chrono::steady_clock::now();

    const double sec =
        std::chrono::duration<double>(t1 - t0).count();
    const double ips = inputCount / sec;

    std::cout << "threads=" << threads << " inputs=" << inputCount << " time_s=" << sec
              << " inputs_per_sec=" << ips << '\n';

    for (auto& p : input) {
        delete p.first;
        delete p.second;
    }
    for (auto& p : output) {
        delete p.first;
        delete p.second;
    }
    return 0;
}
