#include "shuffle.hpp"

#include "comm_helper.hpp"
#include "config.hpp"
#include <stdexcept>
#include <vector>
#include <cstring>
#include <thread>
#include <chrono>



Shuffle::Shuffle(Row* rows) : rows(rows) {}

std::span<Row> Shuffle::run() {

auto& cfg = Config::get();
    auto& helper = CommHelper::get();

    std::vector<rdma::Connection*> conns(cfg.num_nodes);
    for (uint32_t i = 0; i < cfg.num_nodes; ++i) {
        conns[i] = helper.connect_to_node(i);
    }

    std::vector<std::vector<Row>> partitions(cfg.num_nodes);
    for (size_t i = 0; i < cfg.num_rows; ++i) {
        uint32_t part_id = cfg.get_part_id(rows[i].key);
        uint32_t node_id = cfg.part_to_node_id(part_id);
        partitions[node_id].push_back(rows[i]);
    }

    char* base = reinterpret_cast<char*>(rows);
    size_t pos = cfg.num_rows * sizeof(Row);

    pos = (pos + 7) & ~7;

    uint64_t* sync_counter = reinterpret_cast<uint64_t*>(base + pos);
    *sync_counter = 0;
    size_t sync_offset = pos;
    pos += 8;

    uint64_t* sizes = reinterpret_cast<uint64_t*>(base + pos);
    std::memset(sizes, 0, cfg.num_nodes * 8);
    size_t sizes_offset = pos;
    pos += cfg.num_nodes * 8;

    size_t recv_area = pos;

    int barrier_phase = 0;
    auto do_barrier = [&]() {
        barrier_phase++;
        uint64_t target_value = barrier_phase * cfg.num_nodes;

        if (cfg.my_id == 0) {
            (*sync_counter)++;
            while (*sync_counter < target_value) {
                std::this_thread::yield();
            }
        } else {
            uint64_t inc = 1;
            conns[0]->fetch_add_sync(&inc, 1, sync_offset);

            uint64_t observed;
            do {
                conns[0]->read_sync(&observed, 8, sync_offset);
                if (observed < target_value) {
                    std::this_thread::yield();
                }
            } while (observed < target_value);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    };

    do_barrier();

    for (uint32_t dest = 0; dest < cfg.num_nodes; ++dest) {
        uint64_t my_size = partitions[dest].size() * sizeof(Row);

        if (dest == cfg.my_id) {
            sizes[cfg.my_id] = my_size;
        } else {
            size_t write_pos = sizes_offset + cfg.my_id * 8;
            conns[dest]->write_sync(&my_size, 8, write_pos);
        }
    }

    do_barrier();

    std::vector<size_t> recv_pos(cfg.num_nodes);
    size_t curr = recv_area;

    for (uint32_t i = 0; i < cfg.num_nodes; ++i) {
        recv_pos[i] = curr;
        curr += sizes[i];
    }

    for (uint32_t dest = 0; dest < cfg.num_nodes; ++dest) {
        if (dest != cfg.my_id && !partitions[dest].empty()) {
            size_t nbytes = partitions[dest].size() * sizeof(Row);
            conns[dest]->write_sync(partitions[dest].data(), nbytes, recv_pos[cfg.my_id]);
        }
    }

    do_barrier();

    std::vector<Row> output;

    output.insert(output.end(), partitions[cfg.my_id].begin(),
                  partitions[cfg.my_id].end());

    for (uint32_t src = 0; src < cfg.num_nodes; ++src) {
        if (src != cfg.my_id) {
            size_t nrows = sizes[src] / sizeof(Row);
            if (nrows > 0) {
                Row* data = reinterpret_cast<Row*>(base + recv_pos[src]);
                for (size_t i = 0; i < nrows; ++i) {
                    output.push_back(data[i]);
                }
            }
        }
    }

    std::memcpy(rows, output.data(), output.size() * sizeof(Row));

    for (auto& c : conns) {
        helper.close_connection(c);
    }

    return std::span<Row>(rows, output.size());

    //throw std::runtime_error("not implemented");
    //return std::span<Row>();
}
