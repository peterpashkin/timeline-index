//
// Created by pedda on 20-1-24.
//

#ifndef DYNAMIC_BITSET_H
#define DYNAMIC_BITSET_H
#include <cstdint>
#include <vector>


class dynamic_bitset {
    uint64_t size;
    std::vector<uint64_t> data;
public:
    uint64_t set_bits;
    explicit dynamic_bitset(uint64_t size);
    void set(uint64_t index);
    void reset(uint64_t index);
    void flip(uint64_t index);
    bool operator[](uint64_t index);
    uint64_t get_size();
    void get_set_bits(std::vector<uint64_t>& fill);
};



#endif //DYNAMIC_BITSET_H
