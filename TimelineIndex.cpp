//
// Created by Peter Pashkin on 05.12.23.
//

#include "TimelineIndex.h"



TimelineIndex::TimelineIndex(TemporalTable& given_table) : table(given_table), temporal_table_size(given_table.get_table_size()) {
    event_list = EventList(given_table.get_number_of_events());
    version_map = VersionMap(given_table, event_list);

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
    size_t left = 0;
    size_t right = checkpoints.size() - 1;
    size_t middle = (left + right) / 2;

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

std::vector<size_t> TimelineIndex::temporal_sum(uint16_t index) {
    size_t current_sum = 0;
    std::vector<size_t> result;

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
