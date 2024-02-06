//
// Created by Peter Pashkin on 04.12.23.
//
#pragma once
#include "EventList.h"
#include <vector>
#include "TemporalTable.h"


#ifndef TIMELINEINDEX_VERSIONMAP_H
#define TIMELINEINDEX_VERSIONMAP_H


/**
 * @brief VersionMap class
 * @details This class represents a map of all the versions of the Timeline.
 * It uses a vector instead of a map because versions are counting upwards and can be
 * better used for couting sort.
 */
class VersionMap {
    // entry versions[i] points to the last event of version i
    EventList events;
    std::vector<uint32_t> versions;

public:
    uint64_t current_version{0};
    uint64_t event_number{0};

    VersionMap() = default;
    VersionMap(TemporalTable& table);

    /**
     * @brief Inserts all events for the new version
     * @param events
     */
    void register_version(std::vector<Event>& events);

    /**

    * @brief Returns all up until the given version
    * @param version
    * @return
    */
    std::span<Event> get_events(uint32_t version);


    /**
     * @brief Returns all events between the given versions [inclusive, exclusive)
     * @param start_version
     * @param end_version
     * @return
     */
    std::span<Event> get_events(uint32_t start_version, uint32_t end_version);

};

#endif //TIMELINEINDEX_VERSIONMAP_H
