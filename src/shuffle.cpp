#include "shuffle.hpp"
#include "comm_helper.hpp"
#include "config.hpp"
#include <vector>
#include <cstring>
#include <algorithm>
#include <thread>
#include <chrono>
#include <unordered_map>

Shuffle::Shuffle(Row* rows) : rows(rows) {}

std::span<Row> Shuffle::run() {
    auto& cfg = Config::get();
    auto& helper = CommHelper::get();

    // Get base memory pointer - rows already points to the start of RDMA memory
    uint8_t* mem_base = reinterpret_cast<uint8_t*>(rows);

    // Memory layout:
    // [Input Rows (cfg.num_rows * sizeof(Row))]
    // [Histogram Area (num_nodes * num_partitions * sizeof(uint64_t))]
    // [Barrier (64 bytes)]
    // [Output Rows (remaining memory)]

    size_t input_size = cfg.num_rows * sizeof(Row);
    size_t hist_size = cfg.num_nodes * cfg.num_partitions * sizeof(uint64_t);
    size_t barrier_offset = input_size + hist_size;
    size_t output_offset = barrier_offset + 64;

    // Setup pointers
    uint64_t* hist_area = reinterpret_cast<uint64_t*>(mem_base + input_size);
    Row* output_rows = reinterpret_cast<Row*>(mem_base + output_offset);

    // Clear histogram area and barrier
    std::memset(hist_area, 0, hist_size);
    std::memset(mem_base + barrier_offset, 0, 64);

    // Connect to all nodes
    std::vector<rdma::Connection*> conns(cfg.num_nodes);
    for (uint32_t i = 0; i < cfg.num_nodes; ++i) {
        conns[i] = helper.connect_to_node(i);
    }

    // ===== Phase 1: Compute Local Histogram =====
    std::vector<uint64_t> local_hist(cfg.num_partitions, 0);
    for (size_t i = 0; i < cfg.num_rows; ++i) {
        uint32_t part = cfg.get_part_id(rows[i].key);
        local_hist[part]++;
    }

    // Store local histogram in memory
    std::memcpy(&hist_area[cfg.my_id * cfg.num_partitions],
                local_hist.data(),
                cfg.num_partitions * sizeof(uint64_t));

    // ===== Phase 2: Exchange Histograms =====
    for (uint32_t node = 0; node < cfg.num_nodes; ++node) {
        if (node != cfg.my_id) {
            size_t remote_offset = input_size + cfg.my_id * cfg.num_partitions * sizeof(uint64_t);
            conns[node]->write(local_hist.data(),
                              static_cast<uint32_t>(cfg.num_partitions * sizeof(uint64_t)),
                              remote_offset);
        }
    }

    // Synchronization barrier - simple version
    // Node 0 is coordinator
    if (cfg.my_id == 0) {
        // Wait for all other nodes to signal
        uint64_t expected = cfg.num_nodes - 1;
        uint64_t count = 0;
        while (count < expected) {
            conns[0]->read(&count, sizeof(uint64_t), barrier_offset);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Reset barrier and signal completion
        uint64_t zero = 0;
        conns[0]->write(&zero, sizeof(uint64_t), barrier_offset);

        // Signal all other nodes
        uint64_t done = 1;
        for (uint32_t i = 1; i < cfg.num_nodes; ++i) {
            conns[i]->write(&done, sizeof(uint64_t), barrier_offset);
        }
    } else {
        // Increment counter on node 0
        uint64_t result;
        conns[0]->fetch_add(&result, 1, barrier_offset);

        // Wait for signal from coordinator
        uint64_t signal = 0;
        while (signal == 0) {
            conns[cfg.my_id]->read(&signal, sizeof(uint64_t), barrier_offset);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Reset for next barrier
        uint64_t zero = 0;
        conns[cfg.my_id]->write(&zero, sizeof(uint64_t), barrier_offset);
    }

    // ===== Phase 3: Calculate Offsets =====
    // Build complete histogram table
    std::vector<std::vector<uint64_t>> all_hists(cfg.num_nodes,
                                                 std::vector<uint64_t>(cfg.num_partitions));
    for (uint32_t n = 0; n < cfg.num_nodes; ++n) {
        std::memcpy(all_hists[n].data(),
                   &hist_area[n * cfg.num_partitions],
                   cfg.num_partitions * sizeof(uint64_t));
    }

    // Calculate how much we'll receive
    uint64_t total_recv = 0;
    std::vector<uint64_t> part_offsets(cfg.num_partitions, 0);

    for (uint32_t p = 0; p < cfg.num_partitions; ++p) {
        if (cfg.part_to_node_id(p) == cfg.my_id) {
            part_offsets[p] = total_recv;
            for (uint32_t n = 0; n < cfg.num_nodes; ++n) {
                total_recv += all_hists[n][p];
            }
        }
    }

    // Calculate per-source write positions
    std::vector<std::vector<uint64_t>> write_pos(cfg.num_partitions,
                                                 std::vector<uint64_t>(cfg.num_nodes, 0));
    for (uint32_t p = 0; p < cfg.num_partitions; ++p) {
        if (cfg.part_to_node_id(p) == cfg.my_id) {
            uint64_t off = 0;
            for (uint32_t n = 0; n < cfg.num_nodes; ++n) {
                write_pos[p][n] = part_offsets[p] + off;
                off += all_hists[n][p];
            }
        }
    }

    // ===== Phase 4: Send Data =====
    // Group data by destination
    std::unordered_map<uint32_t, std::vector<Row>> send_bufs;
    for (size_t i = 0; i < cfg.num_rows; ++i) {
        uint32_t part = cfg.get_part_id(rows[i].key);
        uint32_t dest = cfg.part_to_node_id(part);
        send_bufs[dest].push_back(rows[i]);
    }

    // Send to each destination
    for (auto& [dest, data] : send_bufs) {
        if (data.empty()) continue;

        // Group by partition
        std::unordered_map<uint32_t, std::vector<Row>> part_data;
        for (const auto& row : data) {
            uint32_t p = cfg.get_part_id(row.key);
            part_data[p].push_back(row);
        }

        // Track current write position for each partition
        for (auto& [p, rows_to_write] : part_data) {
            if (dest == cfg.my_id) {
                // Local copy - find position and copy
                uint64_t pos = write_pos[p][cfg.my_id];
                std::memcpy(&output_rows[pos],
                           rows_to_write.data(),
                           rows_to_write.size() * sizeof(Row));
            } else {
                // Remote write - calculate position on remote node
                uint64_t pos = write_pos[p][cfg.my_id];
                size_t remote_off = output_offset + pos * sizeof(Row);
                conns[dest]->write(rows_to_write.data(),
                                  static_cast<uint32_t>(rows_to_write.size() * sizeof(Row)),
                                  remote_off);
            }
        }
    }

    // ===== Final Barrier =====
    // Reuse same barrier logic
    if (cfg.my_id == 0) {
        // Wait for all other nodes
        uint64_t expected = cfg.num_nodes - 1;
        uint64_t count = 0;
        while (count < expected) {
            conns[0]->read(&count, sizeof(uint64_t), barrier_offset);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // Signal completion
        uint64_t done = 2;
        for (uint32_t i = 1; i < cfg.num_nodes; ++i) {
            conns[i]->write(&done, sizeof(uint64_t), barrier_offset);
        }
    } else {
        // Increment counter on node 0
        uint64_t result;
        conns[0]->fetch_add(&result, 1, barrier_offset);

        // Wait for signal
        uint64_t signal = 0;
        while (signal < 2) {
            conns[cfg.my_id]->read(&signal, sizeof(uint64_t), barrier_offset);
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }

    // Close connections
    for (auto& conn : conns) {
        helper.close_connection(conn);
    }

    return std::span<Row>(output_rows, total_recv);
}
