//
// Created by Peter Pashkin on 04.12.23.
//
#include "TemporalTable.h"


size_t TemporalTable::get_table_size() {
    return tuples.size();
}

std::vector<Tuple> TemporalTable::get_tuples(boost::dynamic_bitset<> bitset) {
    std::vector<Tuple> result;
    for (size_t i = 0; i < bitset.size(); i++) {
        if (bitset[i]) {
            result.push_back(tuples[i].first);
        }
    }
    return result;
}

size_t TemporalTable::get_number_of_events() {
    // iterate over all tuples and add the number of events
    size_t result = 0;
    for(auto& tuple : tuples) {
        result += tuple.second.end.has_value() ? 2 : 1;
    }
    return result;
}
