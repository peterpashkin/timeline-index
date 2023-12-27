//
// Created by Peter Pashkin on 04.12.23.
//
#include "EventList.h"

EventList::EventList(uint32_t size) : events(size) {}

void EventList::append(Event event) {
    events.push_back(event);
}

void EventList::insert(Event event, int index) {
    events[index] = event;
}

std::span<Event> EventList::get_events(unsigned int event_number) {
    std::span<Event> result(events.begin(), events.begin() + event_number);
    return result;
}

std::span<Event> EventList::get_events(uint32_t start_version, uint32_t end_version) {
    std::span<Event> result(events.begin() + start_version + 1, events.begin() + end_version);
    return result;
}


void EventList::append_list(std::vector<Event> appending_events) {
    this->events.insert(this->events.end(), appending_events.begin(), appending_events.end());
}

