#include "shuffle.hpp"

#include "comm_helper.hpp"
#include "config.hpp"
#include "threadpool.hpp"
#include <algorithm>
#include <vector>
#include <array>
#include <memory>
#include <atomic>

Shuffle::Shuffle(Row* rows) : rows(rows) {}

std::span<Row> Shuffle::run() {
    auto& cfg = Config::get();
    auto& helper = CommHelper::get();

    // Establish RDMA connections to all nodes
    std::vector<rdma::Connection*> connections(cfg.num_nodes, nullptr);
    for (uint32_t node_id = 0; node_id < cfg.num_nodes; ++node_id) {
        if (node_id != cfg.my_id) {
            connections[node_id] = helper.connect_to_node(node_id);
        }
    }

    // Calculate memory layout
    size_t row_size = sizeof(Row);
    size_t local_rows_size = cfg.num_rows * row_size;

    // Phase 1: Local partitioning with histogram
    std::vector<std::vector<Row>> local_partitions(cfg.num_nodes);

    // Build histogram and partition data
    std::vector<uint64_t> histogram(cfg.num_nodes, 0);

    // Count tuples per destination node
    for (size_t i = 0; i < cfg.num_rows; ++i) {
        uint32_t part_id = cfg.get_part_id(rows[i].key);
        uint32_t dest_node = cfg.part_to_node_id(part_id);
        histogram[dest_node]++;
    }

    // Resize partitions based on histogram
    for (uint32_t node_id = 0; node_id < cfg.num_nodes; ++node_id) {
        local_partitions[node_id].reserve(histogram[node_id]);
    }

    // Partition rows into destination nodes
    for (size_t i = 0; i < cfg.num_rows; ++i) {
        uint32_t part_id = cfg.get_part_id(rows[i].key);
        uint32_t dest_node = cfg.part_to_node_id(part_id);
        local_partitions[dest_node].push_back(rows[i]);
    }

    // Phase 2: Exchange histogram information
    char* buffer_area = reinterpret_cast<char*>(rows) + local_rows_size;
    uint64_t* histogram_exchange_area = reinterpret_cast<uint64_t*>(buffer_area);

    // Initialize histogram exchange area
    for (uint32_t i = 0; i < cfg.num_nodes * cfg.num_nodes; ++i) {
        histogram_exchange_area[i] = 0;
    }

    // Copy local histogram to exchange area
    for (uint32_t i = 0; i < cfg.num_nodes; ++i) {
        histogram_exchange_area[cfg.my_id * cfg.num_nodes + i] = local_partitions[i].size();
    }

    // Send histogram to all other nodes
    for (uint32_t target_node = 0; target_node < cfg.num_nodes; ++target_node) {
        if (target_node != cfg.my_id) {
            uint64_t remote_offset = local_rows_size + cfg.my_id * cfg.num_nodes * sizeof(uint64_t);
            connections[target_node]->write(
                &histogram_exchange_area[cfg.my_id * cfg.num_nodes],
                cfg.num_nodes * sizeof(uint64_t),
                remote_offset);
        }
    }

    // Simple barrier - wait for histogram exchange to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    // Calculate receive layout based on histograms from all nodes
    uint64_t total_local_rows = 0;
    std::vector<uint64_t> receive_offsets(cfg.num_nodes, 0);

    for (uint32_t sender_node = 0; sender_node < cfg.num_nodes; ++sender_node) {
        uint64_t rows_from_sender = histogram_exchange_area[sender_node * cfg.num_nodes + cfg.my_id];
        receive_offsets[sender_node] = total_local_rows;
        total_local_rows += rows_from_sender;
    }

    // Phase 3: Exchange actual data
    Row* result_area = reinterpret_cast<Row*>(buffer_area + cfg.num_nodes * cfg.num_nodes * sizeof(uint64_t));

    // First, copy local data that stays on this node
    if (!local_partitions[cfg.my_id].empty()) {
        std::copy(local_partitions[cfg.my_id].begin(),
                 local_partitions[cfg.my_id].end(),
                 result_area + receive_offsets[cfg.my_id]);
    }

    // Send data to other nodes using RDMA writes
    for (uint32_t target_node = 0; target_node < cfg.num_nodes; ++target_node) {
        if (target_node != cfg.my_id && !local_partitions[target_node].empty()) {
            size_t send_size = local_partitions[target_node].size() * row_size;

            // Calculate target offset based on my position in the sender order
            uint64_t target_offset = 0;
            for (uint32_t sender = 0; sender < cfg.my_id; ++sender) {
                target_offset += histogram_exchange_area[sender * cfg.num_nodes + target_node];
            }

            uint64_t remote_offset = (reinterpret_cast<char*>(result_area) - reinterpret_cast<char*>(rows))
                                   + target_offset * row_size;

            connections[target_node]->write(local_partitions[target_node].data(),
                                          send_size, remote_offset);
        }
    }

    // Wait for all data transfers to complete
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    // Close RDMA connections
    for (uint32_t node_id = 0; node_id < cfg.num_nodes; ++node_id) {
        if (connections[node_id] != nullptr) {
            helper.close_connection(connections[node_id]);
        }
    }

    // Return span of local results
    return std::span<Row>(result_area, total_local_rows);
}
