#include "shuffle.hpp"

#include "comm_helper.hpp"
#include "config.hpp"
#include <stdexcept>




Shuffle::Shuffle(Row* rows) : rows(rows) {}

std::span<Row> Shuffle::run() {

    throw std::runtime_error("not implemented");
    return std::span<Row>();
}
