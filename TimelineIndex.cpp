//
// Created by Peter Pashkin on 05.12.23.
//

#include "TimelineIndex.h"

#include <set>


TimelineIndex::TimelineIndex(TemporalTable& given_table) : table(given_table), temporal_table_size(given_table.get_table_size()), joined_table(given_table) {
    version_map = VersionMap(given_table);

    // for now checkpoints we will create 100 checkpoints
    uint32_t step_size = std::max(given_table.next_version / 100, 1u);
    boost::dynamic_bitset<> current_bitset(temporal_table_size, 0);

    for(int i=0; i<given_table.next_version; i++) {
        auto events = version_map.get_events(i);
        for(auto& event : events) {
            if(event.type == EventType::INSERT) {
                current_bitset.set(event.row_id);
            } else if(event.type == EventType::DELETE) {
                current_bitset.reset(event.row_id);
            }
        }
        if(i % step_size == 0) {
            checkpoints.emplace_back(i, current_bitset);
        }
    }

}

TimelineIndex::TimelineIndex(TemporalTable& given_table, TemporalTable& given_joined_table) : table(joined_table), joined_table(given_joined_table), temporal_table_size(42), version_map() {}

void TimelineIndex::append_version(std::vector<Event>& events) {
    version_map.register_version(events);
}


std::pair<version, checkpoint> TimelineIndex::find_nearest_checkpoint(version query_version) {
    if (query_version < checkpoints[0].first) {
        throw std::invalid_argument("Version does not exist");
    }

    std::pair search_val{query_version, boost::dynamic_bitset<>()};

    auto it = std::upper_bound(checkpoints.begin(), checkpoints.end(), search_val,
        [](auto x, auto y) -> bool {return x.first < y.first;});
    --it;

    return *it;
}

std::vector<Tuple> TimelineIndex::time_travel(uint32_t version) {
    auto last_checkpoint = find_nearest_checkpoint(version);
    auto last_checkpoint_version = last_checkpoint.first;
    auto bitset = last_checkpoint.second;

    auto events = version_map.get_events(last_checkpoint_version + 1, version + 1);
    for(auto& event : events) {
        if(event.type == EventType::INSERT) {
            bitset.set(event.row_id);
        } else if(event.type == EventType::DELETE) {
            bitset.reset(event.row_id);
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

uint64_t get_max_element(const std::multiset<uint64_t, std::greater<>>& max_set) {
    return *max_set.begin();
}

uint64_t get_min_element(const std::multiset<uint64_t, std::greater<>>& max_set) {
    return *max_set.rbegin();
}

bool is_in_vector(std::vector<uint64_t>& inserted_values, uint32_t value) {
    bool result = false;
    for(auto& inserted_value : inserted_values) {
        if(inserted_value == value) {
            result = true;
            break;
        }
    }
    return result;
}


// TODO bug is: if we delete an element, there is a free space in the multiset, now after the next insert that might be extremely small
// we will basically forget the elements that are new_smallest < x < old_smallest
std::vector<uint64_t> TimelineIndex::temporal_max(uint16_t index) {
    std::vector<uint64_t> result;
    std::multiset<uint64_t, std::greater<>> max_set;
    // TODO play with k
    uint16_t k = 1000;

    // generally the next two vectors are mostly irrelevant.
    // our assumption is that the values are mostly taken from the max_set
    // the vectors are only used if the multiset gets empty -> all values removed in a row, highly unlikely

    std::vector<uint64_t> inserted_values;
    // use unordered_map ??
    std::vector<uint64_t> deleted_values;

    for(int i=0; i<version_map.current_version; i++) {
        auto events = version_map.get_events(i);
        for(auto& event : events) {

            auto inserting_value = table.tuples[event.row_id].first[index];
            auto smallest_element = max_set.empty() ? 0 : get_min_element(max_set);

            if(event.type == EventType::INSERT) {
                // get smallest element in descending multiset

                if(max_set.empty()) {
                    max_set.insert(inserting_value);
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
                    #ifndef NDEBUG
                    assert(is_in_vector(inserted_values, inserting_value));
                    #endif
                    deleted_values.push_back(inserting_value);
                }

                if(max_set.empty()) {
                    std::cout << "very slow :c" << std::endl;
                    // construct a new multiset from inserting values and deleted values
                    // we will achieve this by sorting the vectors and then merging them
                    std::sort(inserted_values.begin(), inserted_values.end());
                    std::sort(deleted_values.begin(), deleted_values.end());

                    auto first_r_it = inserted_values.rbegin();
                    auto second_r_it = deleted_values.rbegin();

                    while(first_r_it != inserted_values.rend() && max_set.size() < k) {
                        if(*first_r_it > *second_r_it) {
                            max_set.insert(*first_r_it);
                            ++first_r_it;
                        } else {
                            ++first_r_it;
                            ++second_r_it;
                        }
                    }
                }
            }
        }
        if(max_set.empty()) result.push_back(0);
        else result.push_back(get_max_element(max_set));
    }

    return result;
}

TimelineIndex TimelineIndex::temporal_join(TimelineIndex other) {
    std::unordered_map<uint64_t, Intersection> intersection_map;
    TimelineIndex result(table, other.table);

    uint32_t new_latest_version = std::max(version_map.current_version, other.version_map.current_version);
    for(uint32_t i=0; i<new_latest_version; i++) {
        std::vector<Event> version_events;
        auto events_for_a = version_map.get_events(i);
        auto events_for_b = other.version_map.get_events(i);

        // iterate through events of a, only apply deletions at first
        for(auto& event : events_for_a) {
            uint64_t associated_value = table.tuples[event.row_id].first[0];
            if(event.type == EventType::DELETE) {
                auto& intersection = intersection_map[associated_value];
                intersection.row_ids_A.erase(event.row_id);
                for(auto& row_id_B : intersection.row_ids_B) {
                    version_events.emplace_back(Event(event.row_id, row_id_B, EventType::DELETE));
                }
            }
        }

        // same thing for events of b
        for(auto& event : events_for_b) {
            uint64_t associated_value = other.table.tuples[event.row_id].first[0];
            if(event.type == EventType::DELETE) {
                auto& intersection = intersection_map[associated_value];
                intersection.row_ids_B.erase(event.row_id);
                for(auto& row_id_A : intersection.row_ids_A) {
                    version_events.emplace_back(Event(row_id_A, event.row_id, EventType::DELETE));
                }
            }
        }

        // now we can apply insertions in the same order
        for(auto& event : events_for_a) {
            uint64_t associated_value = table.tuples[event.row_id].first[0];
            if(event.type == EventType::INSERT) {
                auto& intersection = intersection_map[associated_value];
                intersection.row_ids_A.insert(event.row_id);
                for(auto& row_id_B : intersection.row_ids_B) {
                    version_events.emplace_back(Event(event.row_id, row_id_B, EventType::INSERT));
                }
            }
        }

        // same for b
        for(auto& event : events_for_b) {
            uint64_t associated_value = other.table.tuples[event.row_id].first[0];
            if(event.type == EventType::INSERT) {
                auto& intersection = intersection_map[associated_value];
                intersection.row_ids_B.insert(event.row_id);
                for(auto& row_id_A : intersection.row_ids_A) {
                    version_events.emplace_back(Event(row_id_A, event.row_id, EventType::INSERT));
                }
            }
        }

        result.append_version(version_events);
    }

    return result;
}