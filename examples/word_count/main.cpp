#include <mapreduce/MapReduceClient.h>
#include <mapreduce/MapReduceFramework.h>

#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

class WordKey : public K2, public K3 {
public:
    explicit WordKey(std::string word) : word(std::move(word)) {}
    bool operator<(const K2& other) const override {
        return word < static_cast<const WordKey&>(other).word;
    }
    bool operator<(const K3& other) const override {
        return word < static_cast<const WordKey&>(other).word;
    }
    std::string word;
};

class CountValue : public V2, public V3 {
public:
    explicit CountValue(int n) : count(n) {}
    int count;
};

class LineValue : public V1 {
public:
    explicit LineValue(std::string line) : line(std::move(line)) {}
    std::string line;
};

class WordCountClient : public MapReduceClient {
public:
    void map(const K1* /*key*/, const V1* value, void* context) const override {
        const auto* line = static_cast<const LineValue*>(value);
        std::istringstream stream(line->line);
        std::string token;
        while (stream >> token) {
            emit2(new WordKey(token), new CountValue(1), context);
        }
    }

    void reduce(const IntermediateVec* pairs, void* context) const override {
        int total = 0;
        const std::string& word =
            static_cast<const WordKey*>(pairs->at(0).first)->word;
        for (const auto& pair : *pairs) {
            total += static_cast<const CountValue*>(pair.second)->count;
            delete pair.first;
            delete pair.second;
        }
        emit3(new WordKey(word), new CountValue(total), context);
    }
};

static std::string readFile(const char* path) {
    std::ifstream in(path);
    if (!in) {
        return {};
    }
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

int main(int argc, char** argv) {
    const char* path = (argc > 1) ? argv[1] : nullptr;
    if (!path) {
        std::cerr << "Usage: word_count <text-file>\n";
        return 1;
    }

    const std::string text = readFile(path);
    if (text.empty()) {
        std::cerr << "Could not read input file.\n";
        return 1;
    }

    InputVec input;
    OutputVec output;
    input.push_back({nullptr, new LineValue(text)});

    WordCountClient client;
    JobHandle job = startMapReduceJob(client, input, output, 4);
    if (!job) {
        std::cerr << "Failed to start job.\n";
        return 1;
    }
    closeJobHandle(job);

    for (const auto& pair : output) {
        std::cout << static_cast<const WordKey*>(pair.first)->word << '\t'
                  << static_cast<const CountValue*>(pair.second)->count << '\n';
        delete pair.first;
        delete pair.second;
    }

    delete static_cast<LineValue*>(input[0].second);
    return 0;
}
