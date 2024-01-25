#pragma once
#include <set>

#include "TimelineIndex.h"


uint64_t get_max_element_legacy(const std::multiset<uint64_t, std::greater<>>& max_set) {
    return *max_set.begin();
}

uint64_t get_min_element_legacy(const std::multiset<uint64_t, std::greater<>>& max_set) {
    return *max_set.rbegin();
}

#ifdef LEGACY
std::vector<uint64_t> TimelineIndex::temporal_max(uint16_t index) {
    std::vector<uint64_t> result;
    std::multiset<uint64_t, std::greater<>> max_set;
    const uint16_t k = 500;

    // generally the next two vectors are mostly irrelevant.
    // our assumption is that the values are mostly taken from the max_set
    // the vectors are only used if the multiset gets empty -> all values removed in a row, highly unlikely

    std::vector<uint64_t> inserted_values;
    // use unordered_map ??
    std::vector<uint64_t> deleted_values;

    bool fill_up = true;
    for(int i=0; i<version_map.current_version; i++) {
        auto events = version_map.get_events(i);
        for(auto& event : events) {

            auto inserting_value = table.tuples[event.row_id].first[index];
            auto smallest_element = max_set.empty() ? 0 : get_min_element_legacy(max_set);

            if(event.type == EventType::INSERT) {
                // get smallest element in descending multiset

                if(fill_up || max_set.empty()) {
                    max_set.insert(inserting_value);
                    if(max_set.size() >= k) fill_up = false;
                } else if(inserting_value > smallest_element) {
                    if(max_set.size() >= k) {
                        max_set.erase(max_set.find(smallest_element));
                        inserted_values.push_back(smallest_element);
                    }
                    max_set.insert(inserting_value);
                } else {
                    inserted_values.push_back(inserting_value);
                }
            } else {
                if(inserting_value >= smallest_element) {
                    // erase from multiset
                    max_set.erase(max_set.find(inserting_value));
                } else {
                    deleted_values.push_back(inserting_value);
                }

                if(max_set.empty()) {
                    // construct a new multiset from inserting values and deleted values
                    // we will achieve this by sorting the vectors and then merging them
                    std::sort(inserted_values.begin(), inserted_values.end());
                    std::sort(deleted_values.begin(), deleted_values.end());

                    auto first_r_it = inserted_values.rbegin();
                    auto second_r_it = deleted_values.rbegin();

                    while(first_r_it != inserted_values.rend() && max_set.size() < k) {
                        if(second_r_it == deleted_values.rend() || *first_r_it > *second_r_it) {
                            max_set.insert(*first_r_it);
                            ++first_r_it;
                        } else {
                            ++first_r_it;
                            ++second_r_it;
                        }
                    }
                    for(auto moved_element: max_set) {
                        deleted_values.push_back(moved_element);
                    }
                }
            }
        }
        if(max_set.empty()) result.push_back(0);
        else result.push_back(get_max_element_legacy(max_set));
    }

    return result;
}



// seems to be faster actually
std::pair<version, checkpoint> TimelineIndex::find_nearest_checkpoint(version query_version) {
    if(checkpoints.empty()) {
        // used for joined index
        return {0, dynamic_bitset(temporal_table_size)};
    }
    if (query_version < checkpoints[0].first) {
        throw std::invalid_argument("Version does not exist");
    }

    std::pair search_val{query_version, dynamic_bitset(0)};

    auto it = std::upper_bound(checkpoints.begin(), checkpoints.end(), search_val,
        [](auto x, auto y) -> bool {return x.first < y.first;});

    --it;
    return *it;
}


std::vector<Tuple> TimelineIndex::time_travel(uint32_t version) {
    auto last_checkpoint = find_nearest_checkpoint(version);
    auto last_checkpoint_version = last_checkpoint.first;
    auto bitset = last_checkpoint.second;

    auto events = version_map.get_events( last_checkpoint_version + 1, version + 1);
    for(auto& event : events) {
        if(event.type == EventType::INSERT) {
            bitset.set(event.row_id);
        } else {
            bitset.reset(event.row_id);
        }
    }

    return table.get_tuples(bitset);
}
#endif