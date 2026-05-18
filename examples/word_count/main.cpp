#include <mapreduce/MapReduceClient.h>
#include <mapreduce/MapReduceFramework.h>
#include <mapreduce/errors.h>

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

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
    static std::string normalizeToken(std::string token) {
        while (!token.empty() && !std::isalnum(static_cast<unsigned char>(token.front()))) {
            token.erase(token.begin());
        }
        while (!token.empty() && !std::isalnum(static_cast<unsigned char>(token.back()))) {
            token.pop_back();
        }
        for (char& c : token) {
            c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
        }
        return token;
    }

    void map(const K1* /*key*/, const V1* value, void* context) const override {
        const auto* line = static_cast<const LineValue*>(value);
        std::istringstream stream(line->line);
        std::string token;
        while (stream >> token) {
            token = normalizeToken(std::move(token));
            if (token.empty()) {
                continue;
            }
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

struct WordCount {
    std::string word;
    int count;
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

static void printUsage(const char* prog) {
    std::cerr << "Usage: " << prog
              << " <text-file> [--top N] [--threads N]\n";
}

static int parseArgs(int argc,
                     char** argv,
                     const char*& path,
                     int& topN,
                     int& threads) {
    if (argc < 2) {
        return 1;
    }
    path = argv[1];
    for (int i = 2; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--top" && i + 1 < argc) {
            topN = std::max(1, std::atoi(argv[++i]));
        } else if (arg == "--threads" && i + 1 < argc) {
            threads = std::max(1, std::atoi(argv[++i]));
        } else {
            return 1;
        }
    }
    return 0;
}

int main(int argc, char** argv) {
    const char* path = nullptr;
    int topN = 20;
    int threads = 4;
    if (parseArgs(argc, argv, path, topN, threads) != 0) {
        printUsage(argv[0]);
        return 1;
    }

    const std::string text = readFile(path);
    if (text.empty()) {
        std::cerr << "Could not read input file: " << path << '\n';
        return 1;
    }

    InputVec input;
    OutputVec output;
    input.push_back({nullptr, new LineValue(text)});

    WordCountClient client;
    JobHandle job = startMapReduceJob(client, input, output, threads);
    if (!job) {
        std::cerr << "Failed to start job.\n";
        delete static_cast<LineValue*>(input[0].second);
        return 1;
    }
    waitForJob(job);
    if (getJobError(job) != JOB_OK) {
        std::cerr << "Job failed with error code " << getJobError(job) << '\n';
        closeJobHandle(job);
        delete static_cast<LineValue*>(input[0].second);
        return 1;
    }
    closeJobHandle(job);

    std::vector<WordCount> results;
    results.reserve(output.size());
    for (const auto& pair : output) {
        results.push_back({static_cast<const WordKey*>(pair.first)->word,
                           static_cast<const CountValue*>(pair.second)->count});
        delete pair.first;
        delete pair.second;
    }

    std::sort(results.begin(), results.end(), [](const WordCount& a, const WordCount& b) {
        if (a.count != b.count) {
            return a.count > b.count;
        }
        return a.word < b.word;
    });

    const int limit = std::min(topN, static_cast<int>(results.size()));
    for (int i = 0; i < limit; ++i) {
        std::cout << results[i].word << '\t' << results[i].count << '\n';
    }

    delete static_cast<LineValue*>(input[0].second);
    return 0;
}
