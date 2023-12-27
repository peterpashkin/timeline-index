cmake_minimum_required(VERSION 3.27)
project(TimelineIndex)

set(CMAKE_CXX_STANDARD 23)

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost 1.45.0 COMPONENTS *boost libraries here*)

find_package(Boost REQUIRED)

include_directories(${Boost_INCLUDE_DIRS})

add_executable(MyBoostProject main.cpp)

target_link_libraries(MyBoostProject ${Boost_LIBRARIES})

add_executable(TimelineIndex main.cpp
        TimelineIndex.h
        VersionMap.h
        EventList.h
        EventList.cpp
        VersionMap.cpp
        TemporalTable.h
        TimelineIndex.cpp
        TemporalTable.cpp)
