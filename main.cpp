//
// Created by pedda on 20-1-24.
//
#pragma once

#include "TemporalTable.h"
#include "TimelineIndex.h"
#include <iostream>
#include <chrono>
#include <random>
#include <cassert>

#define TEMPORAL_TABLE_SIZE 2'200'00
#define DISTINCT_VALUES 101ull
#define LIFETIME 100 // determines how long a tuple lives, implicitly also determines the number of tuples that are still active
#define NUMBER_OF_VERSIONS 2'200'000
#define ITERATIONS 10


LifeSpan generate_life_span() {
    uint32_t start = std::rand() % (NUMBER_OF_VERSIONS-1);
    uint32_t life_span = std::rand() % LIFETIME + 1;
    uint32_t end = start + life_span;

    LifeSpan result{start, end};
    if(end >= NUMBER_OF_VERSIONS) result.end = std::nullopt;

    return result;
}

void init_random_temporal_table(TemporalTable& table) {
    for (int i=0; i<TEMPORAL_TABLE_SIZE; ++i) {
        Tuple tuple{std::rand() % DISTINCT_VALUES + 1};
        LifeSpan lifespan = generate_life_span();
        table.tuples.emplace_back(tuple, lifespan);
    }
}

void init_ascending_temporal_table(TemporalTable& table) {
    // is not really possible when TEMPORAL_TABLE_SIZE > NUMBER_OF_VERSIONS
    for(uint32_t i=0; i<TEMPORAL_TABLE_SIZE; ++i) {
        Tuple tuple{i+1};
        LifeSpan lifespan = {i % NUMBER_OF_VERSIONS, (i % NUMBER_OF_VERSIONS) + LIFETIME};
        if(lifespan.end >= NUMBER_OF_VERSIONS) lifespan.end = std::nullopt;
        table.tuples.emplace_back(tuple, lifespan);
    }
}

void time_travel_benchmark(TimelineIndex& index, TemporalTable& table) {
#ifdef BENCHMARK
    uint64_t sum = 0;
#endif

    for(int i=0; i<ITERATIONS; i++) {
        auto traveling_version = i * NUMBER_OF_VERSIONS/ITERATIONS;
#ifdef BENCHMARK
        auto start = std::chrono::high_resolution_clock::now();
#endif
        auto index_travel = index.time_travel(traveling_version);
#ifdef BENCHMARK
        auto end = std::chrono::high_resolution_clock::now();
        sum += std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
#endif

#ifdef DEBUG // check if the results are correct
        auto table_travel = table.time_travel(traveling_version);
        assert(index_travel == table_travel);
#endif
    }

#ifdef BENCHMARK
    std::cout << "Time Travel benchmarking finished" << std::endl;
    std::cout << sum/ITERATIONS << std::endl;
#endif
}

void temporal_sum_benchmark(TimelineIndex& index, TemporalTable& table) {
#ifdef BENCHMARK
    auto start = std::chrono::high_resolution_clock::now();
#endif
    auto index_sum = index.temporal_sum(0);
#ifdef BENCHMARK
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Temporal Sum benchmarking finished" << std::endl;
    std::cout << std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() << std::endl;
#endif
#ifdef DEBUG
    auto table_sum = table.temporal_sum(0);
    assert(index_sum == table_sum);
#endif
}

void temporal_max_benchmark(TimelineIndex& index, TemporalTable& table) {
#ifdef BENCHMARK
    auto start = std::chrono::high_resolution_clock::now();
#endif
    auto index_max = index.temporal_max(0);
#ifdef BENCHMARK
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Temporal Max benchmarking finished" << std::endl;
    std::cout << std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() << std::endl;
#endif
#ifdef DEBUG
    auto table_max = table.temporal_max(0);
    assert(index_max == table_max);
#endif
}

void temporal_join_benchmark(TimelineIndex& index, TimelineIndex& index2, TemporalTable& main_table, TemporalTable& second_table) {
#ifdef BENCHMARK
    auto start = std::chrono::high_resolution_clock::now();
#endif
    auto index_join = index.temporal_join(index2);
#ifdef BENCHMARK
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Temporal Join benchmarking finished" << std::endl;
    std::cout << std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() << std::endl;
#endif
#ifdef DEBUG
    auto table_join = main_table.temporal_join(second_table, 0);
    for(int i=0; i<ITERATIONS; i++) {
        auto a = index_join.time_travel(i); auto b = table_join.time_travel(i);
        assert(a == b);
    }
#endif
}


int main() {
    std::srand(420);
    TemporalTable main_table(NUMBER_OF_VERSIONS, TEMPORAL_TABLE_SIZE);
    TemporalTable second_table(NUMBER_OF_VERSIONS, TEMPORAL_TABLE_SIZE);
    TemporalTable ascending_table(NUMBER_OF_VERSIONS, TEMPORAL_TABLE_SIZE);


    init_random_temporal_table(main_table);
    init_random_temporal_table(second_table);
    init_ascending_temporal_table(ascending_table);


// ------------------ Benchmarking Construction -------------------

#ifdef BENCHMARK
    auto start = std::chrono::high_resolution_clock::now();
#endif
    TimelineIndex index(main_table);
    TimelineIndex index2(second_table);
    TimelineIndex ascending_index(ascending_table);
#ifdef BENCHMARK
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Construction finished" << std::endl;

    std::cout << std::chrono::duration_cast<std::chrono::microseconds>(end-start).count()/3 << std::endl;
#endif

// ----------------------------------------------------------------




// ------------------ Benchmarking Time Travel --------------------
    time_travel_benchmark(index, main_table);
// ----------------------------------------------------------------



//------------------ Benchmarking Temporal Sum --------------------
    temporal_sum_benchmark(index, main_table);
    temporal_sum_benchmark(ascending_index, ascending_table);
// ----------------------------------------------------------------



// ------------------ Benchmarking Temporal Max --------------------
    temporal_max_benchmark(index, main_table);
    temporal_max_benchmark(ascending_index, ascending_table);
// ----------------------------------------------------------------



// ------------------ Benchmarking Temporal Join --------------------
    temporal_join_benchmark(index, index2, main_table, second_table);
// ----------------------------------------------------------------


}
