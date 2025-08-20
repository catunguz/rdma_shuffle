# ADMS Distributed Shuffle
## Task 
- Your task is to implement a key based network shuffle, using RDMA and multi threading.
Choose an approach known to you from the lecture or the papers that have been handed out.

- Row* rows in the Shuffle class should be partitioned and distributed according to the functions get_part_id and part_to_node_id of the Config class applied to the key. 
- Main function to implement is in shuffle.hpp/shuffle.cpp. 
- Use the provided librdmapp to read/write data over RDMA (see connect_to_node in comm_helper.hpp and tests in the library).
- Also have a look at the exercise slides.

Hints:
- The config includes the node id of the local node as member "my_id"
- You can use the ThreadPool we provide.
- If you don't create any RDMA connections, or create some and don't close them properly, the server will wait on them and the program will hang!
- It makes sense to implement a Barrier using RDMA atomics (fetch_add_sync).



## Usage 

1. Login into Dev-VM (replace your username)
    `ssh -p 9000 m12345678@tg.dm.informatik.tu-darmstadt.de` 

    `ssh -p 9001 m12345678@tg.dm.informatik.tu-darmstadt.de`

    Your username for the devvm is m<matriculation_nr>, for example m12345678


2. Copy / Sync project to each VM
    - We recommend lsyncd


3. Start Distexprunner Server on both shells

    `./start_server.sh`

4. Execute runner in a new shell on any of the VMs `./start_client.sh`. This will build the project on each VM and run the specified experiment distributed (see exp.py)

5. It will compile a `main.cpp` that you can use locally for testing and tests from `tests/` as separate binaries.

    - If you want to start the binaries manually make sure to synchronize and start on all nodes
    

## Important remarks

- To avoid conflicts the runner will assign different ports to each user. Do not change this logic.
- The file `exp.py` can be configured to run a debug or release build
- Each VM uses soft-rdma. RDMA is emulated by the kernel module without a dedicated HW-NIC. Performance measurements might not apply to the CI Pipeline where 100G Connect-X5 NICs are used.

