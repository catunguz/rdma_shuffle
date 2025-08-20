#pragma once

#include "cli_parser.hpp"
#include "utils.hpp"

#include <cstdint>
#include <string>
#include <vector>




struct Config : Singleton<Config> {
    uint16_t rdma_port; // same for all
    uint32_t my_id;
    uint32_t num_nodes;
    uint32_t num_partitions;
    uint64_t num_rows; // for this node
    uint64_t mem_size; // includes local relation + rdma buffer memory

    std::vector<std::string> node_ips;

    void parse(int argc, char** argv) {
        cli::Parser parser(argc, argv);
        parser.parse("--rdma_port", rdma_port);
        parser.parse("--my_id", my_id);
        parser.parse("--num_nodes", num_nodes);
        parser.parse("--num_partitions", num_partitions);
        parser.parse("--num_rows", num_rows);
        parser.parse("--mem_size", mem_size);
        parser.parse("--node_ips", node_ips);

    }

    uint32_t part_to_node_id(uint64_t part_id) {
        return static_cast<uint32_t>(part_id % num_nodes);
    }

    uint32_t get_part_id(uint64_t key) {
        return static_cast<uint32_t>(key % num_partitions);
    }


    std::string get_ip(uint32_t node_id) {
        return node_ips.at(node_id);
    }
};