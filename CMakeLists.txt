cmake_minimum_required(VERSION 3.22)
project(TimelineIndex)

set(CMAKE_CXX_STANDARD 23)


add_compile_options("-O3")
add_compile_options("-DNDEBUG")

add_executable(TimelineIndex main.cpp
        TimelineIndex.h
        VersionMap.h
        EventList.h
        EventList.cpp
        VersionMap.cpp
        TemporalTable.h
        TimelineIndex.cpp
        TemporalTable.cpp
        TempTableTesting.cpp
        EventListChildren.h
        setup.cpp
        dynamic_bitset.cpp
        dynamic_bitset.h
)
