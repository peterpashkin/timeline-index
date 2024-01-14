//
// Created by Peter Pashkin on 02.01.24.
//

#include <iostream>
#include "TemporalTable.h"

std::vector<Tuple> TemporalTable::time_travel(uint32_t query_version) {
    std::vector<Tuple> result;
    for(auto& tuple : tuples) {
        if(tuple.second.start <= query_version && (!tuple.second.end.has_value() || tuple.second.end.value() > query_version)) {
            result.push_back(tuple.first);
        }
    }
    return result;
}

std::vector<uint64_t> TemporalTable::temporal_sum(uint16_t index) {
    std::vector<uint64_t> result;
    // for each version check what tuples are currently in the version
    for(int i=0; i<next_version; i++) {
        uint64_t current_sum = 0;
        for(auto& [tuple, lifespan] : tuples) {
            if(lifespan.start <= i && (!lifespan.end.has_value() || lifespan.end.value() > i)) {
                current_sum += tuple[index];
            }
        }
        result.push_back(current_sum);
    }
    return result;
}

std::vector<uint64_t> TemporalTable::temporal_max(uint16_t index) {
    // same thing as temporal_sum, but with max
    std::vector<uint64_t> result;
    // for each version check what tuples are currently in the version
    for(int i=0; i<next_version; i++) {
        uint64_t current_max = 0;
        for(auto& tuple : tuples) {
            if(tuple.second.start <= i && (!tuple.second.end.has_value() || tuple.second.end.value() > i)) {
                current_max = std::max(current_max, tuple.first[index]);
            }
        }
        result.push_back(current_max);
    }
    return result;
}

TemporalTable TemporalTable::temporal_join(TemporalTable&other) {

}
