//
// Created by pedda on 20-1-24.
//

#include "dynamic_bitset.h"



dynamic_bitset::dynamic_bitset(uint64_t size) : size(size) {
    data = std::vector<uint64_t>(size / 64 + 1);
}

void dynamic_bitset::set(uint64_t index) {
    data[index / 64] |= 1ULL << (index % 64);
    ++set_bits;
}

void dynamic_bitset::reset(uint64_t index) {
    // TODO could use xor if guaranteed that set
    data[index / 64] &= ~(1ULL << (index % 64));
    --set_bits;
}

void dynamic_bitset::flip(uint64_t index) {
    data[index / 64] ^= 1ULL << (index % 64);
}

bool dynamic_bitset::operator[](uint64_t index) {
    return data[index / 64] & (1ULL << (index % 64));
}

uint64_t dynamic_bitset::get_size() {
    return size;
}

void dynamic_bitset::get_set_bits(std::vector<uint64_t>& fill) {
    uint64_t current_bit = 0;
    for (uint64_t i = 0; i < data.size(); i++) {
        uint64_t current = data[i];
        while (current != 0) {
            if (current & 1) {
                fill.push_back(current_bit);
            }
            current >>= 1;
            ++current_bit;
        }
    }
}