#include <iostream>

#include "TemporalTable.h"
#include "TimelineIndex.h"

int main() {
    // testing
    TemporalTable table;

    // fill the table with some data
    for(uint32_t i=0; i<100; i++) {
        Tuple tuple{i};
        LifeSpan lifespan{i, i+4};
        table.tuples.emplace_back(tuple, lifespan);
    }

    table.next_version = 110;
    TimelineIndex index(table);

    // testing time_travel
    for(int i=0; i<110; i++) {
        auto correct_travel = table.time_travel(i);
        auto index_travel = index.time_travel(i);

        assert(correct_travel == index_travel);
    }




    auto correct_sum = table.temporal_sum(0);
    auto index_sum = index.temporal_sum(0);

    assert(correct_sum == index_sum);

    auto correct_max = table.temporal_max(0);
    auto index_max = index.temporal_max(0);

    assert(correct_max == index_max);


    std::cout << "All tests passed" << std::endl;


}
