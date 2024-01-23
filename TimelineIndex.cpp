//
// Created by Peter Pashkin on 05.12.23.
//

#include "TimelineIndex.h"
#include <assert.h>
#include <set>
#include <thread>

#define CHECKPOINT_AMOUNT 50
#define TOP_K 100
#define THREAD_AMOUNT 4

TimelineIndex::TimelineIndex(TemporalTable& given_table) : table(given_table), temporal_table_size(given_table.get_table_size()), joined_table(given_table) {
    version_map = VersionMap(given_table);

    // for now checkpoints we will create 100 checkpoints
    uint32_t step_size = std::max(given_table.next_version / CHECKPOINT_AMOUNT, 1u);
    dynamic_bitset current_bitset(temporal_table_size);

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

TimelineIndex::TimelineIndex(TemporalTable& given_table, TemporalTable& given_joined_table) : table(given_table), joined_table(given_joined_table), temporal_table_size(joined_table.get_table_size()), version_map() {}

void TimelineIndex::append_version(std::vector<Event>& events) {
    version_map.register_version(events);
}

#ifndef LEGACY
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



    if(it != checkpoints.end()) {
        int version1 = it->first;
        int version2 = (it-1)->first;
        if(version1 - query_version < query_version - version2) {
            return *it;
        } else {
            return *(it-1);
        }
    }

    --it;
    return *it;
}

//TODO time_travel doesn't work for joined tables -> evaluate values based on bitset, doesn't work because some indexes need to be added twice (x,y) (x,z)
std::vector<Tuple> TimelineIndex::time_travel(uint32_t version) {
    auto [nearest_checkpoint_version, bitset] = find_nearest_checkpoint(version);

    if(nearest_checkpoint_version <= version) {
        auto events = version_map.get_events(nearest_checkpoint_version + 1, version + 1);
        for(auto& event : events) {
            if(event.type == EventType::INSERT) {
                bitset.set(event.row_id);
            } else if(event.type == EventType::DELETE) {
                bitset.reset(event.row_id);
            }
        }
    } else {
        auto events = version_map.get_events(version+1, nearest_checkpoint_version + 1);
        auto event = events.rbegin();
        for(; event != events.rend(); ++event) {
            if(event->type == EventType::DELETE) {
                bitset.set(event->row_id);
            } else if(event->type == EventType::INSERT) {
                bitset.reset(event->row_id);
            }
        }
    }

    return table.get_tuples(bitset);
}
#endif


void TimelineIndex::threading_sum(uint32_t starting_version, uint32_t ending_version, uint16_t index, std::vector<uint64_t>& sum) {
    auto activated_tuples = time_travel(starting_version);
    uint64_t current_sum = 0;
    for(auto& tuples : activated_tuples) {
        current_sum += tuples[index];
    }
    sum[starting_version] = current_sum;

    for(int i=starting_version+1; i<ending_version; i++) {
        auto events = version_map.get_events(i);
        for(auto& event : events) {
            if(event.type == EventType::INSERT) {
                current_sum += table.tuples[event.row_id].first[index];
            } else if(event.type == EventType::DELETE) {
                current_sum -= table.tuples[event.row_id].first[index];
            }
        }
        sum[i] = current_sum;
    }
}


std::vector<uint64_t> TimelineIndex::temporal_sum(uint16_t index) {
    std::vector<uint64_t> result(version_map.current_version, 0);

    std::vector<std::thread> threads;
    std::vector<ThreadSum> args;

    uint32_t step_size = version_map.current_version / THREAD_AMOUNT;

    for(uint32_t i=0; i<THREAD_AMOUNT; i++) {
        uint32_t starting_version = i * step_size;
        uint32_t ending_version = (i+1) * step_size;
        args.emplace_back(ThreadSum{result, starting_version, ending_version, index});
        std::thread thread(&TimelineIndex::threading_sum, this, starting_version, ending_version, index, std::ref(result));
        threads.push_back(std::move(thread));
    }

    for(auto& thread : threads) {
        thread.join();
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



/* ------------------------------ BENCHMARKING ------------------------------
 * N = 200k
 * distinct = 101
 * sorting approach:
 * k = 15: 863319
 * k = 50: 219045
 * k = 100: 132.071 (2mil: 15.675.456)
 * k = 1000: 55.162 (2mil: 1.314.958)
 * k = 5000: (2mil: 642.517)
 *
 * unordered_map appraoch:
 * k = 15: 50.477
 * k = 50: 40.995
 * k = 100: 62.505 (2mil: 602.403)
 * k = 1000: 59.373 (2mil: 551.212)
 * k = 5000: (2mil: 655.069)
 *
 *


----------------------------------------------------------------------------*/


#ifndef LEGACY
void TimelineIndex::threading_max(uint32_t starting_version, uint32_t ending_version, uint16_t index, std::vector<uint64_t>& max) {
    auto activated_tuples = time_travel(starting_version);
    std::multiset<uint64_t, std::greater<>> max_set;
    std::unordered_map<uint64_t, uint32_t> irrelevant_values;
    irrelevant_values.reserve(5'000'000);

    // TODO determine if sorting is beneficial
    std::sort(activated_tuples.begin(), activated_tuples.end(), [index](auto& a, auto& b) {return a[index] > b[index];});

    //insert top-k into multiset
    for(int i=0; i<TOP_K && i<activated_tuples.size(); ++i) {
        max_set.insert(activated_tuples[i][index]);
    }

    // insert rest into map
    for(int i=TOP_K; i<activated_tuples.size(); i++) {
        ++irrelevant_values[activated_tuples[i][index]];
    }

    max[starting_version] = get_max_element(max_set);
    bool fill_up = max_set.size() < TOP_K;

    for(uint32_t i=starting_version+1; i<ending_version; ++i) {
        auto events = version_map.get_events(i);
        for(auto& event : events) {

            auto inserting_value = table.tuples[event.row_id].first[index];
            auto smallest_element = max_set.empty() ? 0 : get_min_element(max_set);

            if(event.type == EventType::INSERT) {
                // get smallest element in descending multiset

                if(fill_up || max_set.empty()) {
                    max_set.insert(inserting_value);
                    if(max_set.size() >= TOP_K) fill_up = false;
                } else if(inserting_value > smallest_element) {
                    if(max_set.size() >= TOP_K) {
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
                                if(max_set.size() >= TOP_K) fill_up = false;
                                continue;
                            }
                            smallest_element = get_min_element(max_set);
                            if(smallest_element >= key) break;
                            if(max_set.size() >= TOP_K) {
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
        if(!max_set.empty()) max[i] = get_max_element(max_set);
    }
}



std::vector<uint64_t> TimelineIndex::temporal_max(uint16_t index) {
    std::vector<uint64_t> result(version_map.current_version, 0);

    std::vector<std::thread> threads;
    std::vector<ThreadSum> args;

    uint32_t step_size = version_map.current_version / THREAD_AMOUNT;

    for(uint32_t i=0; i<THREAD_AMOUNT; i++) {
        uint32_t starting_version = i * step_size;
        uint32_t ending_version = (i+1) * step_size;
        args.emplace_back(ThreadSum{result, starting_version, ending_version, index});
        std::thread thread(&TimelineIndex::threading_max, this, starting_version, ending_version, index, std::ref(result));
        threads.push_back(std::move(thread));
    }

    for(auto& thread : threads) {
        thread.join();
    }


    return result;
}
#endif

TimelineIndex TimelineIndex::temporal_join(TimelineIndex other) {
    std::unordered_map<uint64_t, Intersection> intersection_map;
    TimelineIndex result(table, other.table);

    uint32_t new_latest_version = std::max(version_map.current_version, other.version_map.current_version);
    for(uint32_t i=0; i<new_latest_version; i++) {
        std::vector<Event> version_events;
        auto events_for_a = version_map.get_events(i);
        auto events_for_b = other.version_map.get_events(i);

        std::vector<uint32_t> a_insertions;
        std::vector<uint32_t> b_insertions;

        // iterate through events of a, only apply deletions at first
        for(const auto& event : events_for_a) {
            if(event.type == EventType::DELETE) {
                uint64_t associated_value = table.tuples[event.row_id].first[0];
                auto& intersection = intersection_map[associated_value];
                intersection.row_ids_A.erase(event.row_id);
                for(auto& row_id_B : intersection.row_ids_B) {
                    version_events.emplace_back(Event(event.row_id, row_id_B, EventType::DELETE));
                }
            } else {
                a_insertions.push_back(event.row_id);
            }
        }

        // same thing for events of b
        for(const auto& event : events_for_b) {
            if(event.type == EventType::DELETE) {
                uint64_t associated_value = other.table.tuples[event.row_id].first[0];
                auto& intersection = intersection_map[associated_value];
                intersection.row_ids_B.erase(event.row_id);
                for(auto& row_id_A : intersection.row_ids_A) {
                    version_events.emplace_back(Event(row_id_A, event.row_id, EventType::DELETE));
                }
            } else {
                b_insertions.push_back(event.row_id);
            }
        }

        // now we can apply insertions in the same order
        for(const auto row_id : a_insertions) {
            uint64_t associated_value = table.tuples[row_id].first[0];
            auto& intersection = intersection_map[associated_value];
            intersection.row_ids_A.insert(row_id);
            for(auto& row_id_B : intersection.row_ids_B) {
                version_events.emplace_back(Event(row_id, row_id_B, EventType::INSERT));
            }
        }

        // same for b
        for(const auto row_id : b_insertions) {
            uint64_t associated_value = other.table.tuples[row_id].first[0];
            auto& intersection = intersection_map[associated_value];
            intersection.row_ids_B.insert(row_id);
            for(auto& row_id_A : intersection.row_ids_A) {
                version_events.emplace_back(Event(row_id_A, row_id, EventType::INSERT));
            }
        }

        result.append_version(version_events);
    }

    return result;
}
