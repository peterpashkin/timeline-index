cmake_minimum_required(VERSION 3.22)
project(TimelineIndex)

set(CMAKE_CXX_STANDARD 23)


add_compile_options("-O3")
#add_compile_options("-DDEBUG")
add_compile_options("-DBENCHMARK")
#add_compile_options("-Wall")
#add_compile_options("-Wextra")
add_compile_options("-DLEGACY")



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
        legacy_functions.cpp
)
