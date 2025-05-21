#include "MapReduceFramework.h"
#include <cstdio>
#include <string>
#include <sstream>
#include <unistd.h>

class VString : public V1 {
public:
    VString(std::string content) : content(content) { }
    std::string content;
};

class KWord : public K2, public K3 {
public:
    KWord(std::string word) : word(word) { }
    virtual bool operator<(const K2 &other) const {
        return word < static_cast<const KWord&>(other).word;
    }
    virtual bool operator<(const K3 &other) const {
        return word < static_cast<const KWord&>(other).word;
    }
    std::string word;
};

class VCount : public V2, public V3 {
public:
    VCount(int count) : count(count) { }
    int count;
};

class WordCountClient : public MapReduceClient {
public:
    void map(const K1* key, const V1* value, void* context) const {
        std::string content = static_cast<const VString*>(value)->content;
        std::istringstream iss(content);
        std::string word;
        
        // Count words in the input string
        while (iss >> word) {
            // Convert to lowercase for case-insensitive counting
            for (char& c : word) {
                c = std::tolower(c);
            }
            
            KWord* k2 = new KWord(word);
            VCount* v2 = new VCount(1);
            usleep(50000); // Reduced sleep time for faster processing
            emit2(k2, v2, context);
        }
    }

    virtual void reduce(const IntermediateVec* pairs, void* context) const {
        const std::string word = static_cast<const KWord*>(pairs->at(0).first)->word;
        int count = 0;
        
        for (const IntermediatePair& pair : *pairs) {
            count += static_cast<const VCount*>(pair.second)->count;
            delete pair.first;
            delete pair.second;
        }
        
        KWord* k3 = new KWord(word);
        VCount* v3 = new VCount(count);
        usleep(50000); // Reduced sleep time for faster processing
        emit3(k3, v3, context);
    }
};

int main(int argc, char** argv) {
    WordCountClient client;
    InputVec inputVec;
    OutputVec outputVec;
    
    // Empty input vector
    printf("Testing with empty input vector...\n");
    
    JobState state;
    JobState last_state = {UNDEFINED_STAGE, 0};
    
    // Start the MapReduce job with 8 threads
    const int NUM_THREADS = 8;
    printf("Starting MapReduce job with %d threads...\n", NUM_THREADS);
    JobHandle job = startMapReduceJob(client, inputVec, outputVec, NUM_THREADS);
    getJobState(job, &state);
    
    // Monitor job progress
    while (state.stage != REDUCE_STAGE || state.percentage != 100.0) {
        if (last_state.stage != state.stage || last_state.percentage != state.percentage) {
            printf("Stage %d, %.1f%%\n", state.stage, state.percentage);
        }
        usleep(100000);
        last_state = state;
        getJobState(job, &state);
    }
    
    printf("Stage %d, %.1f%%\n", state.stage, state.percentage);
    printf("Done!\n");
    
    closeJobHandle(job);
    
    // Print results
    printf("\nWord Frequency Results:\n");
    printf("======================\n");
    if (outputVec.empty()) {
        printf("No words found in empty input.\n");
    } else {
        for (OutputPair& pair : outputVec) {
            std::string word = ((const KWord*)pair.first)->word;
            int count = ((const VCount*)pair.second)->count;
            printf("The word '%s' appeared %d time%s\n", 
                   word.c_str(), count, count > 1 ? "s" : "");
            delete pair.first;
            delete pair.second;
        }
    }
    
    return 0;
} 