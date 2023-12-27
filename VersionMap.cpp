//
// Created by Peter Pashkin on 04.12.23.
//
#include "VersionMap.h"

VersionMap::VersionMap(TemporalTable& table, EventList& list) : events(list), versions(table.next_version) {
    // apply counting sort on the temporal table
    for(auto& tuple : table.tuples) {
        versions[tuple.second.start] += 1;
        if(tuple.second.end.has_value()) {
            versions[tuple.second.end.value()] += 1;
        }
    }

    // to get the first index where we insert, we need to add the previous value
    for(size_t i = 1; i < versions.size(); ++i) {
        versions[i] += versions[i - 1];
    }

    // now we can insert the events, this also initializes the version map correctly
    for(int i = 0; i < table.tuples.size(); ++i) {
        auto& current_tuple_lifespan = table.tuples[i].second;
        events.insert(Event(i, EventType::INSERT), versions[current_tuple_lifespan.start]);
        ++versions[current_tuple_lifespan.start];

        if(current_tuple_lifespan.end.has_value()) {
            events.insert(Event(i, EventType::DELETE), versions[current_tuple_lifespan.end.value()]);
            ++versions[current_tuple_lifespan.end.value()];
        }
    }

    current_version = table.next_version;
}

std::span<Event> VersionMap::get_events(uint32_t version) {
    if(version >= versions.size()) {
        throw std::invalid_argument("Version does not exist");
    }

    return events.get_events(versions[version]);
}

std::span<Event> VersionMap::get_events(uint32_t start_version, uint32_t end_version) {
    if(start_version >= versions.size() || end_version >= versions.size()) {
        throw std::invalid_argument("Version does not exist");
    }

    return events.get_events(versions[start_version], versions[end_version]);
}

void VersionMap::register_version(std::vector<Event> events) {
    this->events.append_list(events);
    current_version += events.size();
    versions.push_back(current_version);
}

