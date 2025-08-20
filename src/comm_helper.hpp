#pragma once

#include "config.hpp"
#include "rdmapp/rdma.hpp"
#include "utils.hpp"

#include <chrono>
#include <memory>


class CommHelper : public Singleton<CommHelper> {
    void* mem = nullptr;
    std::unique_ptr<rdma::RDMA> server;

public:
    ~CommHelper() {
        server->wait();
        if (mem) {
            free(mem);
        }
    }

    void setup() {
        auto& cfg = Config::get();
        auto my_ip = cfg.get_ip(cfg.my_id);
        server = std::make_unique<rdma::RDMA>(my_ip, cfg.rdma_port);

        mem = malloc(cfg.mem_size);
        server->register_memory(mem, cfg.mem_size);
        server->listen(rdma::RDMA::CLOSE_AFTER_LAST | rdma::RDMA::IN_BACKGROUND);
    }

    void* get_local_mem() {
        return mem;
    }

    rdma::Connection* connect_to_node(uint32_t node_id) {
        auto& cfg = Config::get();
        auto ip = cfg.get_ip(node_id);


        auto start = std::chrono::high_resolution_clock::now();
        while (true) {
            try {
                return server->connect_to(ip, cfg.rdma_port);
            } catch (...) {
            }

            auto now = std::chrono::high_resolution_clock::now();
            std::chrono::duration<double> elapsed_sec = now - start;
            if (elapsed_sec.count() > 5.0) {
                break;
            }
        }
        throw std::runtime_error("Timeout connecting to server");
    }

    void close_connection(rdma::Connection*& conn) {
        server->close(conn);
        conn = nullptr;
    }
};
