//
// Created by Peter Pashkin on 02.01.24.
//

#include <iostream>
#include "TemporalTable.h"
#include "TimelineIndex.h"

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
    for(uint32_t i=0; i<next_version; i++) {
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
    for(uint32_t i=0; i<next_version; i++) {
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


TemporalTable TemporalTable::temporal_join(TemporalTable&other, uint16_t index) {
    // literally slowest algo ever O(n*m)

    TemporalTable result(std::max(next_version, other.next_version), 0);

    for(auto [tuple_a, lifespan_a] : tuples) {
        for(auto [tuple_b, lifespan_b] : other.tuples) {
            if(tuple_a[index] == tuple_b[index]) {
                uint32_t new_start = std::max(lifespan_a.start, lifespan_b.start);
                std::optional<uint32_t> new_end;
                if(lifespan_a.end.has_value() && lifespan_b.end.has_value()) {
                    new_end = std::min(lifespan_a.end.value(), lifespan_b.end.value());
                } else if(lifespan_a.end.has_value()) {
                    new_end = lifespan_a.end;
                } else if(lifespan_b.end.has_value()) {
                    new_end = lifespan_b.end;
                } else {
                    new_end = std::nullopt;
                }

                if(new_end.has_value() && new_end.value() <= new_start) {
                    continue;
                }

                result.tuples.emplace_back(tuple_a, LifeSpan{new_start, new_end});
            }
        }
    }

    return result;
}


// this method computes the time travel of joined tables. It does not utilize checkpoints and therefore is not relevant and just here for testing purposes
std::vector<Tuple> TimelineIndex::time_travel_joined(version query_version) {
    std::vector<Tuple> result;

    // the main difference will be that we construct a vector of uint32_t instead a bitset to allow having some
    // values multiple times e.g. (x,y) (x,z) are activated
    std::vector<uint32_t> row_ids(table.get_table_size(), 0);

    auto events = version_map.get_events(0, query_version + 1);
    for(auto& event : events) {
        if(event.type == EventType::INSERT) {
            row_ids[event.row_id] += 1;
        } else {
            row_ids[event.row_id] -= 1;
        }
    }

    for(int i=0; i<row_ids.size(); i++) {
        for(int u=0; u<row_ids[i]; u++) {
            result.push_back(table.tuples[i].first);
        }
    }

    return result;
}

