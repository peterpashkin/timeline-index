//
// Created by Peter Pashkin on 05.12.23.
//

#include "TimelineIndex.h"

#include <set>


TimelineIndex::TimelineIndex(TemporalTable& given_table) : table(given_table), temporal_table_size(given_table.get_table_size()) {
    version_map = VersionMap(given_table);

    // for now checkpoints we will create 100 checkpoints
    uint32_t step_size = std::max(given_table.next_version / 100, 1u);
    boost::dynamic_bitset<> current_bitset(temporal_table_size, 0);

    for(int i=0; i<given_table.next_version; i++) {
        if(i % step_size == 0) {
            checkpoints.emplace_back(i, current_bitset);
        }

        auto events = version_map.get_events(i);
        for(auto& event : events) {
            if(event.second == EventType::INSERT) {
                current_bitset.set(event.first);
            } else if(event.second == EventType::DELETE) {
                current_bitset.reset(event.first);
            }
        }
    }

}



std::pair<version, checkpoint> TimelineIndex::find_nearest_checkpoint(version query_version) {
    if (query_version < checkpoints[0].first) {
        throw std::invalid_argument("Version does not exist");
    }

    // binary search
    uint64_t left = 0;
    uint64_t right = checkpoints.size() - 1;
    uint64_t middle = (left + right) / 2;

    while (left < right) {
        if (checkpoints[middle].first < query_version) {
            left = middle + 1;
        } else if (checkpoints[middle].first > query_version) {
            right = middle - 1;
        } else {
            return checkpoints[middle];
        }
        middle = (left + right) / 2;
    }

    return checkpoints[middle];
}

std::vector<Tuple> TimelineIndex::time_travel(uint32_t version) {
    auto last_checkpoint = find_nearest_checkpoint(version);
    auto last_checkpoint_version = last_checkpoint.first;
    auto bitset = last_checkpoint.second;

    auto events = version_map.get_events(last_checkpoint_version, version);
    for(auto& event : events) {
        if(event.second == EventType::INSERT) {
            bitset.set(event.first);
        } else if(event.second == EventType::DELETE) {
            bitset.reset(event.first);
        }
    }

    return table.get_tuples(bitset);
}

std::vector<uint64_t> TimelineIndex::temporal_sum(uint16_t index) {
    uint64_t current_sum = 0;
    std::vector<uint64_t> result;

    // for each version apply all changes in sum and add to result
    for(int i=0; i<version_map.current_version; i++) {
        auto events = version_map.get_events(i);
        for(auto& event : events) {
            if(event.second == EventType::INSERT) {
                current_sum += table.tuples[event.first].first[index];
            } else if(event.second == EventType::DELETE) {
                current_sum -= table.tuples[event.first].first[index];
            }
        }
        result.push_back(current_sum);
    }

    return result;
}

uint64_t get_max_element(const std::multiset<uint64_t>& max_set) {
    return *max_set.begin();
}

uint64_t get_min_element(const std::multiset<uint64_t>& max_set) {
    return *max_set.rbegin();
}


std::vector<uint64_t> TimelineIndex::temporal_max(uint16_t index) {
    std::vector<uint64_t> result;
    std::multiset<uint64_t> max_set;
    // TODO play with k
    uint16_t k = 100;

    // generally the next two vectors are mostly irrelevant.
    // our assumption is that the values are mostly taken from the max_set
    // the vectors are only used if the multiset gets empty -> all values removed in a row, highly unlikely

    std::vector<uint64_t> inserted_values;
    // use unordered_map ??
    std::vector<uint64_t> deleted_values;

    for(int i=0; i<version_map.current_version; i++) {
        auto events = version_map.get_events(i);
        for(auto& event : events) {

            auto inserting_value = table.tuples[event.first].first[index];
            auto smallest_element = get_min_element(max_set);

            if(event.second == EventType::INSERT) {
                // get smallest element in descending multiset

                if(max_set.size() < k) {
                    max_set.insert(inserting_value);
                } else if(inserting_value > smallest_element) {
                    // TODO i think it's easier to return an iterator
                    max_set.erase(max_set.find(smallest_element));
                    max_set.insert(inserting_value);
                } else {
                    inserted_values.push_back(inserting_value);
                }
            } else {
                if(max_set.empty()) {
                    // TODO painful implementation, very slow, you know what to do
                }
                if(inserting_value >= smallest_element) {
                    // erase from multi_set
                    max_set.erase(max_set.find(inserting_value));
                } else {
                    deleted_values.push_back(inserting_value);
                }
            }
        }
        result.push_back(*max_set.begin());
    }

    return result;
}
