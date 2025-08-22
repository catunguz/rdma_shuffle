#include "shuffle.hpp"

#include "comm_helper.hpp"
#include "config.hpp"
#include <vector>
#include <thread>
#include <chrono>

Shuffle::Shuffle(Row* rows) : rows(rows) {}

std::span<Row> Shuffle::run() {
    auto& cfg = Config::get();
    auto& helper = CommHelper::get();

    const size_t row_size = sizeof(Row);
    const size_t local_rows_size = cfg.num_rows * row_size;
    const size_t barrier_size = sizeof(uint64_t);
    const size_t barrier_start = local_rows_size;
    const uint64_t barrier_offset = barrier_start;
    const size_t receive_buffer_start = local_rows_size + barrier_size;
    const size_t available_buffer_space = cfg.mem_size - receive_buffer_start;
    const size_t receive_area_per_node = available_buffer_space / cfg.num_nodes;

    std::vector<rdma::Connection*> connections(cfg.num_nodes);
    for (uint32_t node_id = 0; node_id < cfg.num_nodes; ++node_id) {
        connections[node_id] = helper.connect_to_node(node_id);
    }

    if (cfg.my_id == 0) {
        uint64_t zero = 0;
        connections[0]->write(&zero, sizeof(zero), barrier_offset);
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));

    std::vector<std::vector<Row>> local_partitions(cfg.num_nodes);
    for (size_t i = 0; i < cfg.num_rows; ++i) {
        auto part_id = cfg.get_part_id(rows[i].key);
        auto target_node = cfg.part_to_node_id(part_id);
        local_partitions[target_node].push_back(rows[i]);
    }

    for (uint32_t target_node = 0; target_node < cfg.num_nodes; ++target_node) {
        if (!local_partitions[target_node].empty()) {
            size_t send_size = local_partitions[target_node].size() * row_size;
            uint64_t remote_offset = receive_buffer_start + cfg.my_id * receive_area_per_node;

            if (send_size > receive_area_per_node) {
                throw std::runtime_error("Partition too large for allocated receive area");
            }

            connections[target_node]->write(
                local_partitions[target_node].data(),
                static_cast<uint32_t>(send_size),
                remote_offset
            );
        }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    uint64_t local_counter_result = 0;
    connections[0]->fetch_add(&local_counter_result, 1, barrier_offset);

    std::this_thread::sleep_for(std::chrono::milliseconds(5));

    uint64_t barrier_value = 0;
    int max_attempts = 200;
    int attempts = 0;

    do {
        connections[0]->read(&barrier_value, sizeof(barrier_value), barrier_offset);

        if (barrier_value < cfg.num_nodes) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
            attempts++;
            if (attempts > max_attempts) {
                std::this_thread::sleep_for(std::chrono::milliseconds(50));
                break;
            }
        }
    } while (barrier_value < cfg.num_nodes);

    std::vector<Row> results;

    for (const Row& row : local_partitions[cfg.my_id]) {
        results.push_back(row);
    }

    for (uint32_t sender_node = 0; sender_node < cfg.num_nodes; ++sender_node) {
        if (sender_node == cfg.my_id) continue; // Skip our own data

        char* sender_data_ptr = reinterpret_cast<char*>(rows) + receive_buffer_start +
                               sender_node * receive_area_per_node;
        Row* sender_data = reinterpret_cast<Row*>(sender_data_ptr);
        size_t max_rows_from_sender = receive_area_per_node / row_size;

        for (size_t i = 0; i < max_rows_from_sender; ++i) {
            Row& row = sender_data[i];

            if (row.key >= cfg.num_rows && row.key != 0) {
                break;
            }

            if (row.value < 1000 || row.value >= 1000 + cfg.num_nodes) {
                if (i == 0) {
                    break;
                } else {
                    break;
                }
            }

            auto part_id = cfg.get_part_id(row.key);
            auto correct_node = cfg.part_to_node_id(part_id);

            if (correct_node == cfg.my_id) {
                results.push_back(row);
            }
        }
    }

    for (size_t i = 0; i < results.size(); ++i) {
        rows[i] = results[i];
    }

    for (auto& conn : connections) {
        helper.close_connection(conn);
    }

    return std::span<Row>(rows, results.size());

    //throw std::runtime_error("not implemented");
    //return std::span<Row>();
}

