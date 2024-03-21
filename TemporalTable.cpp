//
// Created by Peter Pashkin on 04.12.23.
//
#include "TemporalTable.h"


TemporalTable::TemporalTable(uint32_t version_number, uint64_t tuples_size) : next_version(version_number) {
    tuples.reserve(tuples_size);
}

uint64_t TemporalTable::get_table_size() {
    return tuples.size();
}

std::vector<Tuple> TemporalTable::get_tuples(checkpoint& bitset) {
    std::vector<Tuple> result;
    result.reserve(bitset.get_set_bits());
    std::vector<uint64_t> set_bits;
    set_bits.reserve(bitset.get_set_bits());


    bitset.fill_bits(set_bits);

    for(auto index: set_bits) {
        result.push_back(tuples[index].first);
    }

    return result;
}

uint64_t TemporalTable::get_number_of_events() {
    // iterate over all tuples and add the number of events
    uint64_t result = 0;
    for(auto& [_, lifespan] : tuples) {
        result += lifespan.end.has_value() ? 2 : 1;
    }
    return result;
}
