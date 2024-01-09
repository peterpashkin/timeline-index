//
// Created by Peter Pashkin on 04.12.23.
//

// TODO do a parent class to cover EventList and DoubleEventList (i think generics are better)
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


struct Event {
    uint32_t row_id;
    uint32_t row_id_second = -1;
    EventType type;
};

/**
 * @brief EventList class
 * @details This class represents all the Events that happened since the start
 * of the Timeline. It is a list of Events, sorted by their timestamp.
 */
class EventList {
    friend class TimelineIndex;
    std::vector<Event> events;

public:
    EventList() = default;
    explicit EventList(uint32_t size);
    void append(Event event);
    std::span<Event> get_events(uint32_t start_version, uint32_t end_version);
    void append_list(std::vector<Event> events);
    void insert(Event event, int index);

};

#endif //TIMELINEINDEX_EVENTLIST_H
