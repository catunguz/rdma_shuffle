#pragma once

#include <cstdint>


struct Row {
    uint64_t key;
    uint64_t value;

    inline friend std::ostream& operator<<(std::ostream& os, const Row& v) {
        os << "Row { key=" << v.key << " value=" << v.value << "}";
        return os;
    }
};
