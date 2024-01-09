#include <iostream>

#include "TemporalTable.h"
#include "TimelineIndex.h"

#define TEMPORAL_TABLE_SIZE 10'000

int main() {
    // testing
    TemporalTable table;

    // fill the table with some data
    for(uint32_t i=0; i<TEMPORAL_TABLE_SIZE; i++) {
        Tuple tuple{i};
        LifeSpan lifespan{i, i+4};
        if(i%100 == 0) lifespan.end = std::nullopt;
        table.tuples.emplace_back(tuple, lifespan);
    }

    table.next_version = TEMPORAL_TABLE_SIZE + 5;
    TimelineIndex index(table);

    std::cout << "Construction finished" << std::endl;

    // testing time_travel
    for(int i=0; i<1'000; i++) {
        auto correct_travel = table.time_travel(i);
        auto index_travel = index.time_travel(i);

        assert(correct_travel == index_travel);
    }

    std::cout << "Time Travel finished" << std::endl;

    auto correct_sum = table.temporal_sum(0);
    auto index_sum = index.temporal_sum(0);

    assert(correct_sum == index_sum);

    std::cout << "Temporal Sum finished" << std::endl;

    auto correct_max = table.temporal_max(0);
    auto index_max = index.temporal_max(0);

    assert(correct_max == index_max);


    std::cout << "All tests passed" << std::endl;


}
