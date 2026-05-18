#ifndef MAPREDUCE_INTERNAL_SHUFFLETYPES_H
#define MAPREDUCE_INTERNAL_SHUFFLETYPES_H

#include "mapreduce/MapReduceClient.h"

struct K2PtrLess {
    bool operator()(const K2* a, const K2* b) const { return *a < *b; }
};

#endif
