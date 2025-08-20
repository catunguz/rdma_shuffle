#include "comm_helper.hpp"
#include "config.hpp"
#include "shuffle.hpp"

#include <iostream>
#include <random>


int main(int argc, char** argv) {
    auto& cfg = Config::get();
    cfg.parse(argc, argv);

    // add your code here for testing

    return 0;
}