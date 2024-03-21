#pragma once
#include <set>

#include "TimelineIndex.h"


uint64_t get_max_element_legacy(const std::multiset<uint64_t, std::greater<>>& max_set) {
    return *max_set.begin();
}

uint64_t get_min_element_legacy(const std::multiset<uint64_t, std::greater<>>& max_set) {
    return *max_set.rbegin();
}

std::vector<uint64_t> TimelineIndex::temporal_max_original(uint16_t index) {
    std::vector<uint64_t> result;
    std::multiset<uint64_t, std::greater<>> max_set;
    const uint16_t k = 100;

    // generally the next two vectors are mostly irrelevant.
    // our assumption is that the values are mostly taken from the max_set
    // the vectors are only used if the multiset gets empty -> all values removed in a row, highly unlikely

    std::vector<uint64_t> inserted_values;
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


std::vector<uint64_t> TimelineIndex::temporal_sum_original(uint16_t index) {
    uint64_t current_sum = 0;
    std::vector<uint64_t> result;

    // for each version apply all changes in sum and add to result
    for(int i=0; i<version_map.current_version; i++) {
        auto events = version_map.get_events(i);
        for(auto& event : events) {
            if(event.type == EventType::INSERT) {
                current_sum += table.tuples[event.row_id].first[index];
            } else if(event.type == EventType::DELETE) {
                current_sum -= table.tuples[event.row_id].first[index];
            }
        }
        result.push_back(current_sum);
    }

    return result;
}

std::vector<uint64_t> TimelineIndex::temporal_max_multiset(uint16_t index) {
    std::vector<uint64_t> result;
    std::multiset<uint64_t, std::greater<>> max_set;

    for(int i=0; i<version_map.current_version; i++) {
        auto events = version_map.get_events(i);
        for(auto& event: events) {
            auto inserting_value = table.tuples[event.row_id].first[index];

            if(event.type == EventType::INSERT) {
                max_set.insert(inserting_value);
            } else {
                max_set.erase(max_set.find(inserting_value));
            }
        }
        if(max_set.empty()) result.push_back(0);
        else result.push_back(get_max_element_legacy(max_set));
    }

    return result;
}


std::vector<uint64_t> TimelineIndex::temporal_max_hashmap(uint16_t index) {
    std::vector<uint64_t> result;
    std::multiset<uint64_t, std::greater<>> max_set;
    const uint16_t k = 100;


    std::unordered_map<uint64_t, uint32_t> irrelevant_values;
    irrelevant_values.reserve(5'000'000);

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
                        ++irrelevant_values[smallest_element];
                    }
                    max_set.insert(inserting_value);
                } else {
                    ++irrelevant_values[inserting_value];
                }
            } else {
                if(inserting_value >= smallest_element) {
                    // erase from multiset
                    max_set.erase(max_set.find(inserting_value));
                } else {
                    --irrelevant_values[inserting_value];
                }

                if(max_set.empty()) {
                    fill_up = true;
                    for(auto [key, amount] : irrelevant_values) {
                        for(int cnt=0; cnt<amount; cnt++) {
                            if(fill_up) {
                                max_set.insert(key);
                                --irrelevant_values[key];
                                if(max_set.size() >= k) fill_up = false;
                                continue;
                            }
                            smallest_element = get_min_element_legacy(max_set);
                            if(smallest_element >= key) break;
                            if(max_set.size() >= k) {
                                max_set.erase(max_set.find(smallest_element));
                                ++irrelevant_values[smallest_element];
                            }
                            max_set.insert(key);
                            --irrelevant_values[key];
                        }
                    }


                }
            }
        }
        if(max_set.empty()) result.push_back(0);
        else result.push_back(get_max_element_legacy(max_set));
    }

    return result;
}



std::pair<version, checkpoint> TimelineIndex::find_earlier_checkpoint(version query_version) {
    if(checkpoints.empty()) {
        // used for joined index
        return {0, checkpoint()};
    }
    if (query_version < checkpoints[0].first) {
        throw std::invalid_argument("Version does not exist");
    }

    std::pair search_val{query_version, checkpoint()};

    auto it = std::upper_bound(checkpoints.begin(), checkpoints.end(), search_val,
        [](auto x, auto y) -> bool {return x.first < y.first;});

    --it;
    return *it;
}


std::vector<Tuple> TimelineIndex::time_travel_original(uint32_t version) {
    auto last_checkpoint = find_earlier_checkpoint(version);
    auto last_checkpoint_version = last_checkpoint.first;
    auto bitset = last_checkpoint.second;


    auto events = version_map.get_events( last_checkpoint_version + 1, version + 1);

    for(auto& event : events) {
        if(event.type == EventType::INSERT) {
            bitset.insert(event.row_id);
        } else {
            bitset.remove(event.row_id);
        }
    }

    return table.get_tuples(bitset);
}
