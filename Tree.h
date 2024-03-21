#ifndef TREE_H
#define TREE_H

#include <cassert>
#include <iostream>
#include <optional>
#include <vector>
#include <memory>
#include <algorithm>
#include <functional>

template <typename T, unsigned bit_length>
class Tree {
public:
    Tree(std::function<uint64_t(T)> func) : hash(func) {}
    void insert(T value) {
        actual_tree.insert(hash(value));
    }
    void remove(T value) {
        actual_tree.remove(hash(value));
    }
    bool member(T value) {
        return actual_tree.member(hash(value));
    }
    std::optional<T> min() {
        std::optional<uint64_t> result = actual_tree.min();
        if(result == std::nullopt) return std::nullopt;
        return hash(result.value());
    }

private:
    Tree<uint64_t, bit_length> actual_tree;
    std::function<uint64_t(T)> hash;
};


template <std::integral T, unsigned bit_length>
class Tree<T, bit_length> {
    public:
    Tree() : bottom_layer(1 << (bit_length / 2)), minimum(std::nullopt), maximum(std::nullopt), bitmask_high(((1<<bit_length/2) - 1) << (bit_length/2 + bit_length%2)), bitmask_low((1<<(bit_length/2 + bit_length%2)) - 1) {}
    //copy constructor and assignment operator
    Tree(const Tree<T, bit_length>& other) : bottom_layer(other.bottom_layer.size()), minimum(other.minimum), maximum(other.maximum), bitmask_high(other.bitmask_high), bitmask_low(other.bitmask_low) {
        if(other.upper_layer != nullptr) {
            upper_layer = std::make_unique<Tree<T, bit_length/2>>(*other.upper_layer);
        }
        for(int i = 0; i < bottom_layer.size(); i++) {
            if(other.bottom_layer[i] != nullptr) {
                bottom_layer[i] = std::make_unique<Tree<T, bit_length/2 + bit_length%2>>(*other.bottom_layer[i]);
            }
        }
    }



    void insert(T value) {
        if(minimum == std::nullopt) {
            minimum = value; maximum = value;
        } else {
            if(value < minimum) std::swap(minimum.value(), value);
            if(value > maximum) maximum = value;

            T high_value = value >> (bit_length/2 + bit_length%2);
            T low_value = value & bitmask_low;

            if(upper_layer == nullptr) {
                upper_layer = std::make_unique<Tree<T, bit_length/2>>();
            }
            if(bottom_layer[high_value] == nullptr) {
                bottom_layer[high_value] = std::make_unique<Tree<T, bit_length/2 + bit_length%2>>();
            }

            if(bottom_layer[high_value]->min() == std::nullopt) {
                upper_layer->insert(high_value);
                bottom_layer[high_value]->insert(low_value);
            } else {
                bottom_layer[high_value]->insert(low_value);
            }

        }
    }

    void remove(T value) {
        if(minimum == maximum) {
            minimum = maximum = std::nullopt;
            return;
        }

        if(value == minimum) {
            T firstcluster = upper_layer->min().value();
            T offset = bottom_layer[firstcluster]->min().value();
            value = (firstcluster << (bit_length / 2 + bit_length%2)) | offset;
            minimum = value;
        }

        T high_value = value >> (bit_length/2 + bit_length%2);
        T low_value = value & bitmask_low;

        bottom_layer[high_value]->remove(low_value);
        if(bottom_layer[high_value]->min() == std::nullopt) {
            upper_layer->remove(high_value);
            if(value == maximum) {
                std::optional<T> upper_max = upper_layer->max();
                if(upper_max == std::nullopt) {
                    maximum = minimum;
                } else {
                    maximum = (upper_max.value() << (bit_length/2 + bit_length%2)) | bottom_layer[upper_max.value()]->max().value();
                }
            }

        } else {
            if(value == maximum) {
                T offset = bottom_layer[high_value]->max().value();
                maximum = (high_value << (bit_length/2 + bit_length%2)) | offset;
            }
        }
    }

    bool member(T value) {
        if (value == minimum) return true;
        if (bottom_layer[value >> (bit_length/2 + bit_length%2)] == nullptr) return false;
        return bottom_layer[value >> (bit_length/2 + bit_length%2)]->member(value & bitmask_low);
    }

    std::optional<T> min() {
        return minimum;
    }
    std::optional<T> max() {
        return maximum;
    }

    std::optional<T> succ(T value) {
        if(minimum.has_value() && value < minimum) return minimum;
        std::optional<T> max_in_cluster = bottom_layer[value>>(bit_length/2 + bit_length%2)] == nullptr ? std::nullopt : bottom_layer[value >> (bit_length/2 + bit_length%2)]->max();
        if(max_in_cluster.has_value() && max_in_cluster.value() > (value & bitmask_low)) {
            T offset = bottom_layer[value >> (bit_length/2 + bit_length%2)]->succ(value & bitmask_low).value();
            return (value & bitmask_high) | offset;
        } else {
            std::optional<T> next_cluster = upper_layer == nullptr ? std::nullopt : upper_layer->succ(value >> (bit_length/2 + bit_length%2));
            if(next_cluster.has_value()) {
                T offset = bottom_layer[next_cluster.value()]->min().value();
                return (next_cluster.value() << (bit_length/2 + bit_length%2)) | offset;
            } else {
                return std::nullopt;
            }
        }
    }

    std::optional<T> pred(T value);

    uint64_t get_set_bits() {
        return set_bits;
    }

    void fill_bits(std::vector<uint64_t> &fill) {
        uint64_t current = 0;
        if(member(current)) fill.push_back(current);
        while(true) {
            std::optional<uint64_t> next = succ(current);
            if(next == std::nullopt) break;
            current = next.value();
            fill.push_back(current);
        }
    }

private:
    std::vector<std::unique_ptr<Tree<T, bit_length/2 + bit_length%2>>> bottom_layer;
    std::unique_ptr<Tree<T, bit_length/2>> upper_layer;
    std::optional<T> minimum;
    std::optional<T> maximum;

    uint64_t bitmask_high;
    uint64_t bitmask_low;
    uint64_t set_bits = 0;
};

enum class State {
    EMPTY,
    ZERO,
    ONE,
    BOTH
};

template<std::integral T>
class Tree<T, 1> {
public:
    Tree() : state(State::EMPTY) {}

    void insert(T value) {
        switch (state) {
            case State::EMPTY:
                state = value == 0 ? State::ZERO : State::ONE;
                break;
            case State::ZERO:
                if(value == 1) {
                    state = State::BOTH;
                }
                break;
            case State::ONE:
                if(value == 0) {
                    state = State::BOTH;
                }
                break;
            case State::BOTH:
                break;
        }
    }

    void remove(T value) {
        switch (state) {
            case State::ZERO:
                if(value == 0) {
                    state = State::EMPTY;
                }
                break;
            case State::ONE:
                if(value == 1) {
                    state = State::EMPTY;
                }
                break;
            case State::BOTH:
                state = value == 0 ? State::ONE : State::ZERO;
                break;
            case State::EMPTY:
                break;
        }
    }

    bool member(T value) {
        switch (state) {
            case State::EMPTY:
                return false;
            case State::ZERO:
                return value == 0;
            case State::ONE:
                return value == 1;
            case State::BOTH:
                return true;
        }
    }

    std::optional<T> min() {
        switch (state) {
            case State::EMPTY:
                return std::nullopt;
            case State::ZERO:
                return 0;
            case State::ONE:
                return 1;
            case State::BOTH:
                return 0;
        }
    }

    std::optional<T> max() {
        switch (state) {
            case State::EMPTY:
                return std::nullopt;
            case State::ZERO:
                return 0;
            case State::ONE:
                return 1;
            case State::BOTH:
                return 1;
        }
    }

    std::optional<T> succ(T value) {
        if(value == 0 && (state == State::ONE || state == State::BOTH)) {
            return 1;
        }
        return std::nullopt;
    }

    std::optional<T> pred(T value) {
        if(value == 1 && (state == State::ZERO || state == State::BOTH)) {
            return 0;
        }
        return std::nullopt;
    }

    uint64_t get_set_bits() {
        switch(state) {
            case State::EMPTY:
                return 0;
            case State::ZERO:
                return 1;
            case State::ONE:
                return 1;
            case State::BOTH:
                return 2;
        }
    }
private:
    State state;
};

#endif //TREE_H