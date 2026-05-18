#ifndef MAPREDUCE_MAPREDUCECLIENT_H
#define MAPREDUCE_MAPREDUCECLIENT_H

#include <utility>
#include <vector>

/// Input key for the map phase.
class K1 {
public:
    virtual ~K1() = default;
    virtual bool operator<(const K1& other) const = 0;
};

class V1 {
public:
    virtual ~V1() = default;
};

/// Intermediate key/value emitted from map (K2, V2).
class K2 {
public:
    virtual ~K2() = default;
    virtual bool operator<(const K2& other) const = 0;
};

class V2 {
public:
    virtual ~V2() = default;
};

/// Output key/value emitted from reduce (K3, V3).
class K3 {
public:
    virtual ~K3() = default;
    virtual bool operator<(const K3& other) const = 0;
};

class V3 {
public:
    virtual ~V3() = default;
};

typedef std::pair<K1*, V1*> InputPair;
typedef std::pair<K2*, V2*> IntermediatePair;
typedef std::pair<K3*, V3*> OutputPair;

typedef std::vector<InputPair> InputVec;
typedef std::vector<IntermediatePair> IntermediateVec;
typedef std::vector<OutputPair> OutputVec;

/// User-defined map/reduce logic for a job.
class MapReduceClient {
public:
    virtual void map(const K1* key, const V1* value, void* context) const = 0;
    virtual void reduce(const IntermediateVec* pairs, void* context) const = 0;

protected:
    ~MapReduceClient() = default;
};

#endif
