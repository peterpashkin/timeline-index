cmake_minimum_required(VERSION 3.22)
project(TimelineIndex)

set(CMAKE_CXX_STANDARD 23)

# Set default build type if not specified
if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

# Common compile options
add_compile_options("-pthread")

# Debug-specific settings
set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -DDEBUG -Wall -fsanitize=address")
set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fsanitize=address")

# Release-specific settings
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -O3")

# Definitions common to all builds
add_definitions("-DBENCHMARK")

# Source files
add_executable(TimelineIndex
        TimelineIndex.h
        VersionMap.h
        EventList.h
        EventList.cpp
        VersionMap.cpp
        TemporalTable.h
        TimelineIndex.cpp
        TemporalTable.cpp
        TempTableTesting.cpp
        main.cpp
        dynamic_bitset.cpp
        dynamic_bitset.h
        legacy_functions.cpp
)

