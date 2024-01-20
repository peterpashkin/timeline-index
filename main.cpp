#include <iostream>
#include <chrono>

#include "TemporalTable.h"
#include "TimelineIndex.h"
#include <assert.h>

#define TEMPORAL_TABLE_SIZE 2'200'000
#define DISTINCT_VALUES 100'000ull

int main() {
    // testing
    TemporalTable table;
    std::srand(420);

    table.tuples.reserve(TEMPORAL_TABLE_SIZE);

    // fill the table with some data
    for(uint32_t i=0; i<TEMPORAL_TABLE_SIZE; i++) {
        Tuple tuple{std::rand() % DISTINCT_VALUES};
        uint32_t first_version = std::rand() % (TEMPORAL_TABLE_SIZE - 1000) + 1;
        uint32_t second_version = (std::rand() % std::min(TEMPORAL_TABLE_SIZE - first_version - 100, 100u)) + first_version + 2;
        LifeSpan lifespan{first_version, second_version};
        if(i%100 == 0) lifespan.end = std::nullopt;
        table.tuples.emplace_back(tuple, lifespan);
    }
    std::cout << "Temporal Table constructed" << std::endl;

    table.next_version = TEMPORAL_TABLE_SIZE + 5;


    TemporalTable table2;
    table2.tuples.reserve(TEMPORAL_TABLE_SIZE);
    for(uint32_t i=0; i<TEMPORAL_TABLE_SIZE; i++) {
        Tuple tuple{std::rand() % DISTINCT_VALUES};
        uint32_t first_version = std::rand() % (TEMPORAL_TABLE_SIZE - 1000) + 1;
        uint32_t second_version = (std::rand() % std::min(TEMPORAL_TABLE_SIZE - first_version - 100, 100u)) + first_version + 2;
        LifeSpan lifespan{first_version, second_version};
        if(i%100 == 0) lifespan.end = std::nullopt;
        table2.tuples.emplace_back(tuple, lifespan);
    }

    table2.next_version = TEMPORAL_TABLE_SIZE + 5;
    TimelineIndex index2(table2);


    // BENCHMARKING


    auto start = std::chrono::high_resolution_clock::now();
    TimelineIndex index(table);
    auto end = std::chrono::high_resolution_clock::now();
    std::cout << "Construction finished" << std::endl;

    std::cout << std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() << std::endl;

    for(int i=0; i<10; i++) {
        auto traveling_version = i * TEMPORAL_TABLE_SIZE/10;
        start = std::chrono::high_resolution_clock::now();
        index.time_travel(traveling_version);
        end = std::chrono::high_resolution_clock::now();
        std::cout << std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() << std::endl;
    }

    std::cout << "Time Travel benchmarking finished" << std::endl;

    start = std::chrono::high_resolution_clock::now();
    auto index_sum1 = index.temporal_sum(0);
    end = std::chrono::high_resolution_clock::now();

    std::cout << std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() << std::endl;

    std::cout << "Temporal Sum benchmark finished" << std::endl;

    start = std::chrono::high_resolution_clock::now();
    auto index_max1 = index.temporal_max(0);
    end = std::chrono::high_resolution_clock::now();

    std::cout << std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() << std::endl;
    std::cout << "Temporal Max benchmark finished" << std::endl;



    start = std::chrono::high_resolution_clock::now();
    auto x = index.temporal_join(index2);
    end = std::chrono::high_resolution_clock::now();

    std::cout << std::chrono::duration_cast<std::chrono::microseconds>(end-start).count() << std::endl;
    std::cout << "Temporal Join finished" << std::endl;


    return 0;
    //auto x = index.temporal_join(index2);


    TemporalTable join_table = table.temporal_join(table2, 0);
    std::cout << "naive join finished" << std::endl;

    for(int i=0; i<10'00; i++) {
        auto correct_travel = join_table.time_travel(i);
        auto index_travel = x.time_travel(i);

        assert(correct_travel == index_travel);
    }

    // testing time_travel



    for(int i=0; i<1'000; i++) {
        auto correct_travel = table.time_travel(i);
        auto index_travel = index.time_travel(i);

        assert(correct_travel == index_travel);
    }

    std::cout << "Time Travel finished" << std::endl;

    auto index_sum = index.temporal_sum(0);
    auto correct_sum = table.temporal_sum(0);


    assert(correct_sum == index_sum);

    std::cout << "Temporal Sum finished" << std::endl;

    auto correct_max = table.temporal_max(0);
    auto index_max = index.temporal_max(0);

    assert(correct_max == index_max);


    std::cout << "All tests passed" << std::endl;


}
