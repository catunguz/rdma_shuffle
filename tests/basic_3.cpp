#include "comm_helper.hpp"
#include "config.hpp"
#include "shuffle.hpp"

#include <iostream>
#include <random>


int main(int argc, char** argv) {
    auto& cfg = Config::get();
    cfg.parse(argc, argv);

    auto& helper = CommHelper::get();
    helper.setup(); // uses values from Config

    std::this_thread::sleep_for(std::chrono::milliseconds(100)); // timeout to discover other RDMA server

    constexpr size_t print_until = 0;

    std::cout << "Test start basic 3\n";


    //populate key modulo a value 

    uint64_t maxKey = 10000;
    auto* rows = reinterpret_cast<Row*>(helper.get_local_mem());
    for (size_t i = 0; i < cfg.num_rows; ++i) {
        rows[i].key = i % maxKey;
        rows[i].value = 1000 + cfg.my_id;
        if (i < print_until) {
            std::cout << rows[i] << " partition: " << cfg.get_part_id(rows[i].key) << "\n";
        }
    }

    std::srand(0);
    std::random_shuffle(rows, rows + cfg.num_rows);

    Shuffle shuffle{rows};
    auto result = shuffle.run();


    std::cout << "Shuffle done\n";
    for (size_t i = 0; auto& row : result) {
        if (i++ < print_until) {
            std::cout << i - 1 << "): " << row << " partition: " << cfg.get_part_id(row.key) << "\n";
        }
        auto part_id = cfg.get_part_id(row.key);
        rdma::ensure(cfg.my_id == cfg.part_to_node_id(part_id), [&] {
            std::stringstream ss;
            ss << "Received wrong key: " << row.key << " should have been on node: " << cfg.part_to_node_id(part_id) << "\n";
            return ss.str();
        });
    }

   
    size_t numberOfExpectedRows = 0;
    for (size_t i = 0; i < cfg.num_rows; ++i) {
        auto val = i % maxKey;
        auto part_id = cfg.get_part_id(val);
        if(cfg.my_id == cfg.part_to_node_id(part_id)){
            numberOfExpectedRows++;
            //TODO check that data is received correctly 
        }

    }
    numberOfExpectedRows = numberOfExpectedRows * cfg.num_nodes;
    
    


    rdma::ensure(numberOfExpectedRows == result.size(), [&] {
            std::stringstream ss;
            ss << "Received wrong number of values: " << result.size() << " should have been: " << numberOfExpectedRows << "\n";
            return ss.str();
        });

    std::cout << "Test finished\n";

    return 0;
}