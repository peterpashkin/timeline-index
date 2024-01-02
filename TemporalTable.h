//
// Created by Peter Pashkin on 04.12.23.
//

#pragma once
#include <vector>
#include <span>
#include <boost/dynamic_bitset.hpp>

#ifndef TIMELINEINDEX_TEMPORALTABLE_H
#define TIMELINEINDEX_TEMPORALTABLE_H



/**
 * @brief Tuple class
 * @details This class represents a tuple from the Temporal Table, which is a vector of spans
 */

// TODO: consider using a vector of spans instead of a vector of uint64_t
// currently like this to make temporal aggregations more logical
typedef std::vector<uint64_t> Tuple;
struct LifeSpan {
    uint32_t start;
    // tuple might not have been deleted, then this value is None
    std::optional<uint32_t> end;
};

/**
 * @brief TemporalTable class
 * @details This class represents a table of all the tuple changes
 */
class TemporalTable {
public:
    uint32_t next_version;

    /**
     * @brief Tuples
     * @details This vector contains all the tuples in the table
     * and their lifespan
     */
    std::vector<std::pair<Tuple, LifeSpan>> tuples;

    uint64_t get_table_size();

    /**
     * @brief Returns all tuples that are alive at the given version
     * @param bitset
     * @return
     */
    std::vector<Tuple> get_tuples(boost::dynamic_bitset<> bitset);

    /**
     *
     * @return number of events in the table
     */
    uint64_t get_number_of_events();


    // extremely naive approaches, just for testing
    std::vector<Tuple> time_travel(uint32_t query_version);
    std::vector<uint64_t> temporal_sum(uint16_t index);
    std::vector<uint64_t> temporal_max(uint16_t index);

};




#endif //TIMELINEINDEX_TEMPORALTABLE_H





