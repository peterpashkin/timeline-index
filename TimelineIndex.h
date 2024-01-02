//
// Created by Peter Pashkin on 04.12.23.
//

#include "EventList.h"
#include "VersionMap.h"
#include "TemporalTable.h"
#include <boost/dynamic_bitset.hpp>

#ifndef TIMELINEINDEX_TIMELINEINDEX_H
#define TIMELINEINDEX_TIMELINEINDEX_H

typedef boost::dynamic_bitset<> checkpoint;
typedef uint32_t version;

/**
 * @brief TimelineIndex class
 * @details This class represents the TimelineIndex working on top of a const TemporalTable.
 * that represent the state of the table at a given version
 */
class TimelineIndex {
    TemporalTable& table;
    VersionMap version_map;
    std::vector<std::pair<version, checkpoint>> checkpoints;
    const uint64_t temporal_table_size;

    std::pair<version, checkpoint> find_nearest_checkpoint(version query_version);

public:
    explicit TimelineIndex(TemporalTable& table);
    std::vector<Tuple> time_travel(version query_version);

    //TODO might consider lambda functions for general purpose aggregation

    // index is a very basic indicator of what to aggregate
    std::vector<uint64_t> temporal_sum(uint16_t index);
    std::vector<uint64_t> temporal_avg();
    std::vector<uint64_t> temporal_max(uint16_t index);
    TimelineIndex temporal_join(TimelineIndex other);

};


#endif //TIMELINEINDEX_TIMELINEINDEX_H



