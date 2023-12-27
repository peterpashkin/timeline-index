//
// Created by Peter Pashkin on 04.12.23.
//
#include <iostream>
#include <utility>
#include <vector>
#include <span>

#ifndef TIMELINEINDEX_EVENTLIST_H
#define TIMELINEINDEX_EVENTLIST_H

enum EventType {
    INSERT,
    DELETE
};

// ROW_ID and EVENT_TYPE
typedef std::pair<uint32_t, EventType> Event;

/**
 * @brief EventList class
 * @details This class represents all the Events that happened since the start
 * of the Timeline. It is a list of Events, sorted by their timestamp.
 */
class EventList {
    std::vector<Event> events;

public:
    EventList() = default;
    EventList(uint32_t size);
    void append(Event event);
    std::span<Event> get_events(unsigned int version);
    std::span<Event> get_events(uint32_t start_version, uint32_t end_version);
    void append_list(std::vector<Event> events);
    void insert(Event event, int index);

};

#endif //TIMELINEINDEX_EVENTLIST_H
