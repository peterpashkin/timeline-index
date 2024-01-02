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
    auto correct_travel = table.time_travel(50);
    auto index_travel = index.time_travel(50);

    if(correct_travel != index_travel) {
        std::cout << "Time travel failed" << std::endl;
        return 1;
    }

    for(int i=0; i<100; i++) {
        auto correct_sum = table.temporal_sum(i);
        auto index_sum = index.temporal_sum(i);

        if(correct_sum != index_sum) {
            std::cout << "Temporal sum failed" << std::endl;
        }
    }

}
