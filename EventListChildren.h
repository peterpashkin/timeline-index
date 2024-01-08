//
// Created by Peter Pashkin on 07.01.24.
//
#include "EventList.h"

#ifndef EVENTLISTCHILDREN_H
#define EVENTLISTCHILDREN_H

class EventListSingle : public EventList {

public:
    EventListSingle() = default;
    explicit EventListSingle(uint32_t size);
    void append(Event event);
    std::span<Event> get_events(uint32_t start_version, uint32_t end_version);
    void append_list(std::vector<Event> events);
    void insert(Event event, int index);
};


class EventListDouble : public EventList {

};

#endif //EVENTLISTCHILDREN_H
