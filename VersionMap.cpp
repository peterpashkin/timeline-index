//
// Created by Peter Pashkin on 04.12.23.
//
#include "VersionMap.h"

VersionMap::VersionMap(TemporalTable& table) : events(table.get_number_of_events()), event_number(table.get_number_of_events()), versions(table.next_version + 1) {
    // apply counting sort on the temporal table
    // offset of 1 is to avoid the double summation from the last loop
    // e.g. starting of 0 is 0 and not num of tuples with 1
    for(auto& [_, lifespan] : table.tuples) {
        versions[lifespan.start + 1] += 1;
        if(lifespan.end.has_value()) {
            versions[lifespan.end.value() + 1] += 1;
        }
    }

    // to get the first index where we insert, we need to add the previous value
    for(uint64_t i = 1; i < versions.size(); ++i) {
        versions[i] += versions[i - 1];
    }

    // now we can insert the events, this also initializes the version map correctly
    for(int i = 0; i < table.tuples.size(); ++i) {
        auto& current_tuple_lifespan = table.tuples[i].second;
        events.insert(Event(i, -1, EventType::INSERT), versions[current_tuple_lifespan.start]);
        ++versions[current_tuple_lifespan.start];

        if(current_tuple_lifespan.end.has_value()) {
            events.insert(Event(i, -1, EventType::DELETE), versions[current_tuple_lifespan.end.value()]);
            ++versions[current_tuple_lifespan.end.value()];
        }
    }

    current_version = table.next_version;
}


std::span<Event> VersionMap::get_events(uint32_t version) {
    return get_events(version, version+1);
}

std::span<Event> VersionMap::get_events(uint32_t start_version, uint32_t end_version) {
    if(start_version >= versions.size() || end_version >= versions.size()) {
        // we will allow this case and return no events
        return {};
        throw std::invalid_argument("Version does not exist");
    }

    uint32_t start_index = start_version == 0 ? 0 : versions[start_version - 1];
    uint32_t end_index = versions[end_version - 1];

    return events.get_events(start_index, end_index);
}

void VersionMap::register_version(std::vector<Event>& events) {
    this->events.append_list(events);
    ++current_version;
    event_number += events.size();
    versions.push_back(event_number);
}

