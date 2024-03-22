//
// Created by pedda on 20-1-24.
//
#include "TemporalTable.h"
#include "TimelineIndex.h"
#include <iostream>
#include <chrono>
#include <random>
#include <cassert>
#include <iomanip>

#define TEMPORAL_TABLE_SIZE 3'400'00
#define DISTINCT_VALUES 100'000ull
#define LIFETIME 10000 // determines how long a tuple lives, implicitly also determines the number of tuples that are still active
#define NUMBER_OF_VERSIONS 2'200'00
#define ITERATIONS 100


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

void init_descending_temporal_table(TemporalTable& table) {
    for(uint32_t i=0; i<TEMPORAL_TABLE_SIZE; ++i) {
        Tuple tuple{i+1};
        uint32_t starting_version = NUMBER_OF_VERSIONS - (i % NUMBER_OF_VERSIONS) - 1;
        uint32_t ending_version = starting_version + LIFETIME;
        LifeSpan lifespan = {starting_version, ending_version};
        if(lifespan.end >= NUMBER_OF_VERSIONS) lifespan.end = std::nullopt;
        table.tuples.emplace_back(tuple, lifespan);
    }
}

uint64_t time_travel_benchmark(TimelineIndex& index, TemporalTable& table, std::vector<Tuple> (TimelineIndex::*func)(uint32_t)) {
    uint64_t sum = 0;

    for(int i=0; i<ITERATIONS; i++) {
        auto traveling_version = i * NUMBER_OF_VERSIONS/ITERATIONS;
        auto start = std::chrono::high_resolution_clock::now();
        auto index_travel = (index.*func)(traveling_version);
        auto end = std::chrono::high_resolution_clock::now();
        sum += std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();

#ifdef DEBUG // check if the results are correct
        auto table_travel = table.time_travel(traveling_version);
        assert(index_travel == table_travel);
#endif
    }

    return sum/ITERATIONS;
}

uint64_t temporal_sum_benchmark(TimelineIndex& index, TemporalTable& table, std::vector<uint64_t> (TimelineIndex::*func)(uint16_t)) {
    auto start = std::chrono::high_resolution_clock::now();
    auto index_sum = (index.*func)(0);
    auto end = std::chrono::high_resolution_clock::now();

#ifdef DEBUG
    auto table_sum = table.temporal_sum(0);
    assert(index_sum == table_sum);
#endif

    return std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();

}

uint64_t temporal_max_benchmark(TimelineIndex& index, TemporalTable& table, std::vector<uint64_t> (TimelineIndex::*func)(uint16_t)) {
    auto start = std::chrono::high_resolution_clock::now();
    auto index_max = (index.*func)(0);
    auto end = std::chrono::high_resolution_clock::now();

#ifdef DEBUG
    auto table_max = table.temporal_max(0);
    assert(index_max == table_max);
#endif

    return std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
}

uint64_t temporal_join_benchmark(TimelineIndex& index, TimelineIndex& index2, TemporalTable& main_table, TemporalTable& second_table) {
    auto start = std::chrono::high_resolution_clock::now();
    auto index_join = index.temporal_join(index2);
    auto end = std::chrono::high_resolution_clock::now();

#ifdef DEBUG
    auto table_join = main_table.temporal_join(second_table, 0);
    for(int i=0; i<ITERATIONS; i++) {
        auto traveling_version = i * NUMBER_OF_VERSIONS/ITERATIONS;
        auto a = index_join.time_travel_joined(traveling_version);
        auto b = table_join.time_travel(traveling_version);
        assert(a == b);
    }
#endif

    return std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
}


int main() {

    std::cout << "Starting tests with " << NUMBER_OF_VERSIONS << " versions and " << TEMPORAL_TABLE_SIZE << " tuples" << std::endl;
    std::cout << "Additional information: Maximal Lifetime: " << LIFETIME << ", Distinct Values: " << DISTINCT_VALUES << std::endl;
    std::cout << std::endl;


    std::srand(420);
    TemporalTable main_table(NUMBER_OF_VERSIONS, TEMPORAL_TABLE_SIZE);
    TemporalTable second_table(NUMBER_OF_VERSIONS, TEMPORAL_TABLE_SIZE);
    TemporalTable ascending_table(NUMBER_OF_VERSIONS, TEMPORAL_TABLE_SIZE);
    TemporalTable descending_table(NUMBER_OF_VERSIONS, TEMPORAL_TABLE_SIZE);


    init_random_temporal_table(main_table);
    init_random_temporal_table(second_table);
    init_ascending_temporal_table(ascending_table);
    init_descending_temporal_table(descending_table);


// ------------------ Benchmarking Construction -------------------

    auto start = std::chrono::high_resolution_clock::now();
    TimelineIndex index(main_table);
    TimelineIndex index2(second_table);
    TimelineIndex ascending_index(ascending_table);
    TimelineIndex descending_index(descending_table);
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Construction: " << std::endl;
    std::cout << std::chrono::duration_cast<std::chrono::microseconds>(end-start).count()/4 << std::endl;
    std::cout << std::endl;

// ----------------------------------------------------------------




// ------------------ Benchmarking Time Travel --------------------
    std::cout << "Time Travel testing, average on " << ITERATIONS << " iterations\n\n";
    auto random_main_travel = time_travel_benchmark(index, main_table, &TimelineIndex::time_travel);
    auto random_original_travel = time_travel_benchmark(index, main_table, &TimelineIndex::time_travel_original);

    auto ascending_main_travel = time_travel_benchmark(ascending_index, ascending_table, &TimelineIndex::time_travel);
    auto ascending_original_travel = time_travel_benchmark(ascending_index, ascending_table, &TimelineIndex::time_travel_original);

    auto descending_main_travel = time_travel_benchmark(descending_index, descending_table, &TimelineIndex::time_travel);
    auto descending_original_travel = time_travel_benchmark(descending_index, descending_table, &TimelineIndex::time_travel_original);


    std::cout << "                  Modified Time Travel      Original Time Travel" << std::endl;
    std::cout << "Random values:      " << std::setw(8) << random_main_travel << "               " << std::setw(8) << random_original_travel << std::endl;
    std::cout << "Ascending values:   " << std::setw(8) << ascending_main_travel << "               " << std::setw(8) << ascending_original_travel << std::endl;
    std::cout << "Descending values:  " << std::setw(8) << descending_main_travel << "               " << std::setw(8) << descending_original_travel << std::endl;
    std::cout << std::endl;

// ----------------------------------------------------------------



//------------------ Benchmarking Temporal Sum --------------------
    std::cout << "Temporal Sum testing" << std::endl;
    auto random_main_sum = temporal_sum_benchmark(index, main_table, &TimelineIndex::temporal_sum);
    auto random_original_sum = temporal_sum_benchmark(index, main_table, &TimelineIndex::temporal_sum_original);

    auto ascending_main_sum = temporal_sum_benchmark(ascending_index, ascending_table, &TimelineIndex::temporal_sum);
    auto ascending_original_sum = temporal_sum_benchmark(ascending_index, ascending_table, &TimelineIndex::temporal_sum_original);

    auto descending_main_sum = temporal_sum_benchmark(descending_index, descending_table, &TimelineIndex::temporal_sum);
    auto descending_original_sum = temporal_sum_benchmark(descending_index, descending_table, &TimelineIndex::temporal_sum_original);

    std::cout << "                  Modified Temporal Sum      Original Temporal Sum" << std::endl;
    std::cout << "Random values:      " << std::setw(8) << random_main_sum << "                 " << std::setw(8) << random_original_sum << std::endl;
    std::cout << "Ascending values:   " << std::setw(8) << ascending_main_sum << "                 " << std::setw(8) << ascending_original_sum << std::endl;
    std::cout << "Descending values:  " << std::setw(8) << descending_main_sum << "                 " << std::setw(8) << descending_original_sum << std::endl;
    std::cout << std::endl;
// ----------------------------------------------------------------



// ------------------ Benchmarking Temporal Max --------------------
    std::cout << "Temporal Max testing" << std::endl;
    auto random_main_max = temporal_max_benchmark(index, main_table, &TimelineIndex::temporal_max);
    auto random_original_max = temporal_max_benchmark(index, main_table, &TimelineIndex::temporal_max_original);
    auto random_hashmap_max = temporal_max_benchmark(index, main_table, &TimelineIndex::temporal_max_hashmap);
    auto random_multiset_max = temporal_max_benchmark(index, main_table, &TimelineIndex::temporal_max_multiset);

    auto ascending_main_max = temporal_max_benchmark(ascending_index, ascending_table, &TimelineIndex::temporal_max);
    auto ascending_original_max = temporal_max_benchmark(ascending_index, ascending_table, &TimelineIndex::temporal_max_original);
    auto ascending_hashmap_max = temporal_max_benchmark(ascending_index, ascending_table, &TimelineIndex::temporal_max_hashmap);
    auto ascending_multiset_max = temporal_max_benchmark(ascending_index, ascending_table, &TimelineIndex::temporal_max_multiset);

    auto descending_main_max = temporal_max_benchmark(descending_index, descending_table, &TimelineIndex::temporal_max);
    auto descending_original_max = temporal_max_benchmark(descending_index, descending_table, &TimelineIndex::temporal_max_original);
    auto descending_hashmap_max = temporal_max_benchmark(descending_index, descending_table, &TimelineIndex::temporal_max_hashmap);
    auto descending_multiset_max = temporal_max_benchmark(descending_index, descending_table, &TimelineIndex::temporal_max_multiset);

    std::cout << "                  Modified Temporal Max     HashMap Temporal Max    Original Temporal Max   Multiset Temporal Max" << std::endl;
    std::cout << "Random values:      " << std::setw(8) << random_main_max << "                 " << std::setw(8) << random_hashmap_max << "                 " << std::setw(8) << random_original_max << "                 " << std::setw(8) << random_multiset_max << std::endl;
    std::cout << "Ascending values:   " << std::setw(8) << ascending_main_max << "                 " << std::setw(8) << ascending_hashmap_max << "                 " << std::setw(8) << ascending_original_max  << "                 " << std::setw(8) << ascending_multiset_max << std::endl;
    std::cout << "Descending values:  " << std::setw(8) << descending_main_max << "                 " << std::setw(8) << descending_hashmap_max << "                 " << std::setw(8) << descending_original_max << "                 " << std::setw(8) << descending_multiset_max << std::endl;
    std::cout << std::endl;
    std::cout << std::endl;
// ----------------------------------------------------------------



// ------------------ Benchmarking Temporal Join --------------------
    std::cout << "Temporal Join testing" << std::endl;
    std::cout << "Random on random:        "; std::cout << temporal_join_benchmark(index, index2, main_table, second_table) << std::endl;
    std::cout << "Random on ascending:     "; std::cout << temporal_join_benchmark(index, ascending_index, main_table, ascending_table) << std::endl;
    std::cout << "Random on descending:    "; std::cout << temporal_join_benchmark(index, descending_index, main_table, descending_table) << std::endl;
    std::cout << "Ascending on descending: "; std::cout << temporal_join_benchmark(ascending_index, descending_index, ascending_table, descending_table) << std::endl;
// ----------------------------------------------------------------


}
