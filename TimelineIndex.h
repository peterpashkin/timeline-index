//
// Created by Peter Pashkin on 04.12.23.
//

#include "EventList.h"
#include "VersionMap.h"
#include "TemporalTable.h"
#include "Tree.h"
#include <unordered_set>
#include <unordered_map>
#include <algorithm>

#ifndef TIMELINEINDEX_TIMELINEINDEX_H
#define TIMELINEINDEX_TIMELINEINDEX_H

typedef Tree<uint32_t, 16> checkpoint;
typedef uint32_t version;

struct Intersection {
    std::unordered_set<uint32_t> row_ids_A;
    std::unordered_set<uint32_t> row_ids_B;
};

struct ThreadSum {
    std::vector<uint64_t>& sum;
    uint32_t starting_version;
    uint32_t ending_version;
    uint16_t index;
};

/**
 * @brief TimelineIndex class
 * @details This class represents the TimelineIndex working on top of a const TemporalTable.
 * that represent the state of the table at a given version
 */
class TimelineIndex {
    TemporalTable& table;
    TemporalTable& joined_table;
    VersionMap version_map;
    std::vector<std::pair<version, checkpoint>> checkpoints;
    const uint64_t temporal_table_size;

    std::pair<version, checkpoint> find_nearest_checkpoint(version query_version);
    std::pair<version, checkpoint> find_earlier_checkpoint(version query_version);

public:
    explicit TimelineIndex(TemporalTable& table);
    explicit TimelineIndex(TemporalTable& table, TemporalTable& joined_table);
    void append_version(std::vector<Event>& events);
    std::vector<Tuple> time_travel(version query_version);


    void threading_sum(uint32_t starting_version, uint32_t ending_version, uint16_t index, std::vector<uint64_t>& sum);
    std::vector<uint64_t> temporal_sum(uint16_t index);
    void threading_max(uint32_t starting_version, uint32_t ending_version, uint16_t index, std::vector<uint64_t>& max);
    std::vector<uint64_t> temporal_max(uint16_t index);
    TimelineIndex temporal_join(TimelineIndex other);

    std::vector<Tuple> time_travel_joined(version query_version);



    std::vector<uint64_t> temporal_sum_original(uint16_t index);
    std::vector<uint64_t> temporal_max_original(uint16_t index);
    std::vector<uint64_t> temporal_max_hashmap(uint16_t index);
    std::vector<uint64_t> temporal_max_multiset(uint16_t index);
    std::vector<Tuple> time_travel_original(version query_version);
};


#endif //TIMELINEINDEX_TIMELINEINDEX_H



