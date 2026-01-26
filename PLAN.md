# Mochi Streaming Pipeline Benchmark - Implementation Plan

## Overview

A C++ 3-process MPI benchmark for evaluating streaming data transfer strategies using Mochi's Thallium RPC/RDMA and ABT-IO libraries. Measures end-to-end throughput for transferring ~100GB of data: **Sender -> Broker -> Receiver**.

## Architecture

```
+-------------+     RPC/RDMA      +-------------+     RPC/RDMA      +-------------+
|   SENDER    | ----------------> |   BROKER    | ----------------> |  RECEIVER   |
| (MPI Rank 0)|                   | (MPI Rank 1)|                   | (MPI Rank 2)|
+-------------+                   +-------------+                   +-------------+
                                        |
                                        v
                                  [Output File]
                                   (ABT-IO)
```

## File Structure

```
src/
  common/
    config.hpp/cpp          # YAML configuration parser
    buffer_pool.hpp/cpp     # Reusable buffer management for RDMA
    batch.hpp               # Batch data structure
    timing.hpp              # High-resolution timing utilities
  sender/
    sender_provider.hpp/cpp # Thallium provider for sender
  broker/
    broker_provider.hpp/cpp # Thallium provider for broker
    file_writer.hpp/cpp     # ABT-IO file writing abstraction
  receiver/
    receiver_provider.hpp/cpp # Thallium provider for receiver
  main.cpp                  # MPI initialization and role dispatch

configs/
  default.yaml              # Sample configurations

tests/
  unit/                     # Unit tests
  integration/              # Integration tests

CMakeLists.txt
```

## Parameter Space

| Parameter | Description | Typical Range |
|-----------|-------------|---------------|
| `total_data_bytes` | Total data to transfer | ~100GB |
| `message_size` (S) | Size of individual message | 4KB - 16MB |
| `messages_per_batch` (M) | Messages grouped per batch | 1 - 1024 |
| `max_concurrent_batches` (B) | Max batches in flight | 1 - 64 |
| `transfer_mode` | RPC_INLINE, RDMA_DIRECT, RDMA_REGISTERED | - |
| `concurrent_rdma_pulls` | Broker's concurrent RDMA ops | 1 - 32 |
| `concurrent_writes` | Broker's concurrent file writes | 1 - 32 |
| `use_uring` | Enable io_uring for ABT-IO | true/false |
| `network_protocol` | Thallium network backend | tcp, ofi+verbs |

## YAML Configuration Schema

```yaml
pipeline:
  total_data_bytes: 107374182400  # 100 GB
  network_protocol: "ofi+verbs"   # or "tcp"
  verify_checksums: false         # Enable optional CRC32 verification

  thallium:
    progress_thread: true
    rpc_thread_count: 4

sender_to_broker:
  message_size: 1048576           # 1 MB (S)
  messages_per_batch: 64          # 64 messages = 64 MB per batch (M)
  max_concurrent_batches: 4       # (B)
  transfer_mode: "rdma_registered" # "rpc_inline", "rdma_direct", "rdma_registered"

broker:
  output_file: "/mnt/storage/output.dat"
  concurrent_rdma_pulls: 8

  # Acknowledgment timing: "ack_on_persist" or "ack_on_receive"
  ack_timing: "ack_on_persist"

  # Forwarding strategy: "reload_from_file", "reuse_after_persist",
  #                      "forward_immediate", or "passthrough"
  forward_strategy: "reuse_after_persist"

  # For passthrough mode: whether broker still writes to file
  passthrough_persist: true  # If false, data bypasses file entirely

  abt_io:
    concurrent_writes: 8

broker_to_receiver:
  message_size: 1048576
  messages_per_batch: 32
  max_concurrent_batches: 4
  transfer_mode: "rdma_direct"
```

## Detailed Class Interfaces

### 1. Config Classes (`src/common/config.hpp`)

```cpp
#include <yaml-cpp/yaml.h>
#include <string>
#include <cstdint>

enum class TransferMode { RPC_INLINE, RDMA_DIRECT, RDMA_REGISTERED };
enum class AckTiming { ACK_ON_PERSIST, ACK_ON_RECEIVE };
enum class ForwardStrategy { RELOAD_FROM_FILE, REUSE_AFTER_PERSIST,
                             FORWARD_IMMEDIATE, PASSTHROUGH };

struct TransferConfig {
    size_t message_size;            // S bytes per message
    size_t messages_per_batch;      // M messages per batch
    size_t max_concurrent_batches;  // B max in-flight batches
    TransferMode mode;
};

struct AbtIoConfig {
    size_t concurrent_writes;       // Max concurrent pwrite operations
};

struct BrokerConfig {
    std::string output_file;
    size_t concurrent_rdma_pulls;
    AckTiming ack_timing;
    ForwardStrategy forward_strategy;
    bool passthrough_persist;       // Write to file even in passthrough mode
    AbtIoConfig abt_io;
};

struct PipelineConfig {
    size_t total_data_bytes;
    std::string network_protocol;   // "tcp", "ofi+verbs", etc.
    bool verify_checksums;
    bool progress_thread;
    int rpc_thread_count;

    TransferConfig sender_to_broker;
    BrokerConfig broker;
    TransferConfig broker_to_receiver;

    static PipelineConfig from_yaml(const std::string& path);
    void validate() const;  // Throws on invalid combinations
};
```

### 2. BufferPool Class (`src/common/buffer_pool.hpp`)

```cpp
#include <thallium.hpp>
#include <vector>
#include <queue>

namespace tl = thallium;

class BufferPool {
public:
    struct Handle {
        void* data;
        size_t size;
        tl::bulk bulk;      // Pre-registered bulk (empty if not RDMA_REGISTERED)
        size_t index;       // Index in pool for release
    };

    // mode: bulk_mode for pre-registration (read_only, write_only, read_write)
    BufferPool(tl::engine& engine, size_t buffer_size, size_t pool_size,
               tl::bulk_mode mode);
    ~BufferPool();

    Handle acquire();                    // Blocks until buffer available
    bool try_acquire(Handle& out);       // Non-blocking, returns false if none available
    void release(const Handle& handle);  // Return buffer to pool

    size_t buffer_size() const { return buffer_size_; }
    size_t pool_size() const { return pool_size_; }
    size_t available() const;            // Number of available buffers

private:
    tl::engine& engine_;
    size_t buffer_size_;
    size_t pool_size_;
    tl::bulk_mode mode_;

    std::vector<void*> buffers_;         // Raw buffer pointers
    std::vector<tl::bulk> bulks_;        // Pre-registered bulk handles
    std::queue<size_t> available_;       // Indices of available buffers

    tl::mutex mutex_;
    tl::condition_variable cv_;
};
```

### 3. SenderProvider (`src/sender/sender_provider.hpp`)

```cpp
#include <thallium.hpp>
#include <atomic>
#include "common/config.hpp"
#include "common/buffer_pool.hpp"

class SenderProvider : public tl::provider<SenderProvider> {
public:
    SenderProvider(tl::engine& engine, uint16_t provider_id,
                   const TransferConfig& config,
                   const std::string& broker_address,
                   size_t total_bytes,
                   bool verify_checksums);
    ~SenderProvider();

    // Start the transfer (called from main after setup)
    void run();

    // Statistics
    double get_elapsed_seconds() const;
    size_t get_bytes_sent() const;
    size_t get_batches_sent() const;

private:
    // RPC handlers (broker calls these to ack)
    void on_ack(const tl::request& req, uint64_t batch_id);

    // Transfer implementations
    void send_rpc_inline(uint64_t batch_id, const void* data, size_t size);
    void send_rdma_direct(uint64_t batch_id, const void* data, size_t size);
    void send_rdma_registered(uint64_t batch_id, BufferPool::Handle& handle);

    // Generate batch data (fills buffer, returns checksum if enabled)
    uint32_t generate_batch_data(void* buffer, size_t size, uint64_t batch_id);

    TransferConfig config_;
    tl::endpoint broker_ep_;
    size_t total_bytes_;
    bool verify_checksums_;

    // Buffer pool (for RDMA_REGISTERED mode)
    std::unique_ptr<BufferPool> buffer_pool_;

    // Concurrency control
    tl::mutex inflight_mutex_;
    tl::condition_variable inflight_cv_;
    size_t inflight_count_ = 0;

    // Statistics
    std::atomic<size_t> bytes_sent_{0};
    std::atomic<size_t> batches_sent_{0};
    std::atomic<size_t> batches_acked_{0};
    std::chrono::high_resolution_clock::time_point start_time_;
    std::chrono::high_resolution_clock::time_point end_time_;

    // RPCs
    tl::remote_procedure rpc_batch_rpc_;      // Send batch via RPC arg
    tl::remote_procedure rpc_batch_rdma_;     // Notify broker of RDMA batch
};
```

### 4. BrokerProvider (`src/broker/broker_provider.hpp`)

```cpp
#include <thallium.hpp>
#include <abt-io.h>
#include <atomic>
#include "common/config.hpp"
#include "common/buffer_pool.hpp"

class BrokerProvider : public tl::provider<BrokerProvider> {
public:
    BrokerProvider(tl::engine& engine, uint16_t provider_id,
                   const BrokerConfig& broker_config,
                   const TransferConfig& recv_config,
                   const TransferConfig& send_config,
                   const std::string& receiver_address,
                   size_t total_bytes,
                   bool verify_checksums);
    ~BrokerProvider();

    // Wait for all data to be processed
    void wait_completion();

    // Statistics
    size_t get_bytes_received() const;
    size_t get_bytes_written() const;
    size_t get_bytes_forwarded() const;

private:
    // RPC handlers from sender
    void on_batch_rpc(const tl::request& req, uint64_t batch_id,
                      const std::vector<char>& data, uint32_t checksum);
    void on_batch_rdma(const tl::request& req, uint64_t batch_id,
                       size_t size, tl::bulk& remote_bulk, uint32_t checksum);
    void on_sender_done(const tl::request& req);

    // Core processing - called after receiving batch data
    void process_batch(const tl::request& req, uint64_t batch_id,
                       void* data, size_t size, uint32_t expected_checksum,
                       tl::endpoint sender_ep, tl::bulk* sender_bulk);

    // File I/O
    void write_to_file(const void* data, size_t size, off_t offset);
    void read_from_file(void* data, size_t size, off_t offset);

    // Forward to receiver
    void forward_rpc_inline(uint64_t batch_id, const void* data, size_t size, uint32_t checksum);
    void forward_rdma(uint64_t batch_id, const void* data, size_t size,
                      tl::bulk& local_bulk, uint32_t checksum);
    void forward_passthrough(uint64_t batch_id, size_t size,
                             tl::bulk& sender_bulk, const std::string& sender_addr,
                             uint32_t checksum);

    BrokerConfig broker_config_;
    TransferConfig recv_config_;
    TransferConfig send_config_;
    tl::endpoint receiver_ep_;
    size_t total_bytes_;
    bool verify_checksums_;

    // ABT-IO
    abt_io_instance_id abt_io_;
    int file_fd_;
    std::atomic<off_t> file_offset_{0};

    // Buffer pools
    std::unique_ptr<BufferPool> recv_pool_;   // For receiving from sender
    std::unique_ptr<BufferPool> send_pool_;   // For forwarding to receiver
    std::unique_ptr<BufferPool> reload_pool_; // For reload_from_file strategy

    // Completion tracking
    tl::mutex completion_mutex_;
    tl::condition_variable completion_cv_;
    std::atomic<size_t> bytes_received_{0};
    std::atomic<size_t> bytes_written_{0};
    std::atomic<size_t> bytes_forwarded_{0};
    bool sender_done_ = false;

    // RPCs to receiver
    tl::remote_procedure rpc_forward_rpc_;
    tl::remote_procedure rpc_forward_rdma_;
    tl::remote_procedure rpc_forward_passthrough_;
    tl::remote_procedure rpc_receiver_done_;
};
```

### 5. ReceiverProvider (`src/receiver/receiver_provider.hpp`)

```cpp
#include <thallium.hpp>
#include <atomic>
#include "common/config.hpp"
#include "common/buffer_pool.hpp"

class ReceiverProvider : public tl::provider<ReceiverProvider> {
public:
    ReceiverProvider(tl::engine& engine, uint16_t provider_id,
                     const TransferConfig& config,
                     size_t total_bytes,
                     bool verify_checksums);
    ~ReceiverProvider();

    // Wait for all data to be received
    void wait_completion();

    // Statistics
    size_t get_bytes_received() const;
    double get_elapsed_seconds() const;

private:
    // RPC handlers from broker
    void on_batch_rpc(const tl::request& req, uint64_t batch_id,
                      const std::vector<char>& data, uint32_t checksum);
    void on_batch_rdma(const tl::request& req, uint64_t batch_id,
                       size_t size, tl::bulk& remote_bulk, uint32_t checksum);
    void on_passthrough(const tl::request& req, uint64_t batch_id,
                        size_t size, tl::bulk& sender_bulk,
                        const std::string& sender_addr, uint32_t checksum);
    void on_done(const tl::request& req);

    // Process received data
    void process_batch(uint64_t batch_id, const void* data, size_t size,
                       uint32_t expected_checksum);

    TransferConfig config_;
    size_t total_bytes_;
    bool verify_checksums_;

    // Buffer pool for RDMA receives
    std::unique_ptr<BufferPool> buffer_pool_;

    // Completion tracking
    tl::mutex completion_mutex_;
    tl::condition_variable completion_cv_;
    std::atomic<size_t> bytes_received_{0};
    bool transfer_done_ = false;

    std::chrono::high_resolution_clock::time_point first_recv_time_;
    std::chrono::high_resolution_clock::time_point last_recv_time_;
    bool first_recv_ = true;
};
```

## Main Program Structure (`src/main.cpp`)

```cpp
#include <mpi.h>
#include <thallium.hpp>
#include <abt-io.h>
#include "common/config.hpp"
#include "sender/sender_provider.hpp"
#include "broker/broker_provider.hpp"
#include "receiver/receiver_provider.hpp"

namespace tl = thallium;

std::vector<std::string> exchange_addresses(const std::string& my_addr, MPI_Comm comm) {
    // Get max address length across all ranks
    int my_len = my_addr.size() + 1;
    int max_len;
    MPI_Allreduce(&my_len, &max_len, 1, MPI_INT, MPI_MAX, comm);

    // Gather all addresses
    int size;
    MPI_Comm_size(comm, &size);
    std::vector<char> all_addrs(size * max_len);
    std::vector<char> my_addr_buf(max_len, 0);
    std::copy(my_addr.begin(), my_addr.end(), my_addr_buf.begin());

    MPI_Allgather(my_addr_buf.data(), max_len, MPI_CHAR,
                  all_addrs.data(), max_len, MPI_CHAR, comm);

    std::vector<std::string> result(size);
    for (int i = 0; i < size; i++) {
        result[i] = std::string(&all_addrs[i * max_len]);
    }
    return result;
}

int main(int argc, char** argv) {
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <config.yaml>" << std::endl;
        return 1;
    }

    MPI_Init(&argc, &argv);
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size != 3) {
        if (rank == 0) std::cerr << "Requires exactly 3 MPI ranks" << std::endl;
        MPI_Finalize();
        return 1;
    }

    auto config = PipelineConfig::from_yaml(argv[1]);
    config.validate();

    // Initialize Thallium engine (all processes are servers)
    tl::engine engine(config.network_protocol, THALLIUM_SERVER_MODE,
                      config.progress_thread, config.rpc_thread_count);

    // Exchange addresses
    auto addresses = exchange_addresses(engine.self(), MPI_COMM_WORLD);
    // addresses[0] = sender, addresses[1] = broker, addresses[2] = receiver

    MPI_Barrier(MPI_COMM_WORLD);  // Ensure all engines are ready

    double start_time = MPI_Wtime();

    if (rank == 0) {
        // SENDER
        SenderProvider sender(engine, 1, config.sender_to_broker,
                              addresses[1], config.total_data_bytes,
                              config.verify_checksums);
        sender.run();
        // Sender done, wait for completion signal from broker
    }
    else if (rank == 1) {
        // BROKER
        BrokerProvider broker(engine, 1, config.broker,
                              config.sender_to_broker, config.broker_to_receiver,
                              addresses[2], config.total_data_bytes,
                              config.verify_checksums);
        broker.wait_completion();
    }
    else {
        // RECEIVER
        ReceiverProvider receiver(engine, 1, config.broker_to_receiver,
                                  config.total_data_bytes,
                                  config.verify_checksums);
        receiver.wait_completion();
    }

    MPI_Barrier(MPI_COMM_WORLD);
    double end_time = MPI_Wtime();

    // Report results from rank 0
    if (rank == 0) {
        double elapsed = end_time - start_time;
        double throughput_gbps = (config.total_data_bytes / elapsed) / (1024*1024*1024);
        std::cout << "=== Benchmark Results ===" << std::endl;
        std::cout << "Total data: " << config.total_data_bytes << " bytes" << std::endl;
        std::cout << "Elapsed time: " << elapsed << " seconds" << std::endl;
        std::cout << "Throughput: " << throughput_gbps << " GB/s" << std::endl;
    }

    engine.finalize();
    MPI_Finalize();
    return 0;
}
```

## CMakeLists.txt

```cmake
cmake_minimum_required(VERSION 3.15)
project(mochi-streaming-pipeline CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find packages
find_package(MPI REQUIRED)
find_package(thallium REQUIRED)
find_package(PkgConfig REQUIRED)
find_package(yaml-cpp REQUIRED)

pkg_check_modules(ABT_IO REQUIRED IMPORTED_TARGET abt-io)

# Common library
add_library(pipeline_common STATIC
    src/common/config.cpp
    src/common/buffer_pool.cpp
)
target_include_directories(pipeline_common PUBLIC
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/include
)
target_link_libraries(pipeline_common PUBLIC
    thallium
    yaml-cpp
)

# Provider libraries
add_library(pipeline_sender STATIC src/sender/sender_provider.cpp)
target_link_libraries(pipeline_sender PUBLIC pipeline_common)

add_library(pipeline_broker STATIC src/broker/broker_provider.cpp)
target_link_libraries(pipeline_broker PUBLIC pipeline_common PkgConfig::ABT_IO)

add_library(pipeline_receiver STATIC src/receiver/receiver_provider.cpp)
target_link_libraries(pipeline_receiver PUBLIC pipeline_common)

# Main executable
add_executable(pipeline_benchmark src/main.cpp)
target_link_libraries(pipeline_benchmark PRIVATE
    pipeline_sender
    pipeline_broker
    pipeline_receiver
    MPI::MPI_CXX
)

# Tests (optional)
option(BUILD_TESTS "Build unit tests" ON)
if(BUILD_TESTS)
    enable_testing()
    find_package(Catch2 QUIET)
    if(Catch2_FOUND)
        add_subdirectory(tests)
    endif()
endif()
```

## Transfer Modes

### RPC Inline Mode
```
Sender --[batch data as RPC arg]--> Broker --[batch data as RPC arg]--> Receiver
```

### RDMA Pull Mode
```
Sender: expose(buffer, read_only)
        --[notify: batch_id, size, bulk_handle]--> Broker
Broker: local_bulk << remote_bulk.on(sender_ep)  // Pull
        --[ack]--> Sender
```

### RDMA with Pre-registered Buffers
Same as RDMA Pull, but `bulk` handles are created once at startup and reused.

## Implementation Phases

### Phase 1: Foundation
**Files to create:**
- `src/common/config.hpp`, `src/common/config.cpp`
- `src/common/buffer_pool.hpp`, `src/common/buffer_pool.cpp`
- `CMakeLists.txt`

**Tasks:**
1. Set up CMake build with find_package for thallium, pkg-config for abt-io
2. Implement `PipelineConfig` with YAML parsing and validation
3. Implement `BufferPool` with Thallium bulk registration
4. Write unit tests for config parsing and buffer pool

**Validation:** `make && ./test_config && ./test_buffer_pool`

### Phase 2: Basic RPC Pipeline
**Files to create:**
- `src/sender/sender_provider.hpp`, `src/sender/sender_provider.cpp`
- `src/broker/broker_provider.hpp`, `src/broker/broker_provider.cpp`
- `src/receiver/receiver_provider.hpp`, `src/receiver/receiver_provider.cpp`
- `src/main.cpp`

**Tasks:**
1. Implement `SenderProvider` with RPC_INLINE mode only
2. Implement `BrokerProvider` with `forward_immediate` strategy (no file I/O yet)
3. Implement `ReceiverProvider` receiving RPC data
4. Implement MPI address exchange in main.cpp
5. Test end-to-end with small data (1MB)

**Validation:** `mpirun -np 3 ./pipeline_benchmark configs/rpc_only.yaml`

### Phase 3: RDMA Support
**Files to modify:**
- `src/sender/sender_provider.cpp` - add RDMA modes
- `src/broker/broker_provider.cpp` - add RDMA pull
- `src/receiver/receiver_provider.cpp` - add RDMA modes

**Tasks:**
1. Add `RDMA_DIRECT` mode to sender (expose buffer per batch)
2. Add RDMA pull logic to broker
3. Add `RDMA_REGISTERED` mode to sender (use buffer pool)
4. Add RDMA forwarding to broker
5. Add RDMA/passthrough receive to receiver
6. Test all transfer mode combinations

**Validation:** Test with RDMA config, verify bulk transfers work

### Phase 4: File I/O & Strategies
**Files to modify:**
- `src/broker/broker_provider.cpp` - add ABT-IO and all strategies

**Tasks:**
1. Initialize ABT-IO in broker (`abt_io_init`)
2. Implement `write_to_file()` with `abt_io_pwrite`
3. Implement `read_from_file()` for `reload_from_file` strategy
4. Implement all ack_timing + forward_strategy combinations
5. Implement `passthrough_persist` option
6. Add concurrent write tracking

**Validation:** Test each strategy combination, verify file contents

### Phase 5: Testing & Benchmarking
**Files to create:**
- `tests/unit/*.cpp`
- `tests/integration/*.cpp`
- `scripts/benchmark_sweep.py`

**Tasks:**
1. Write comprehensive unit tests
2. Write integration tests for each strategy
3. Create benchmark sweep script for parameter exploration
4. Generate performance reports
5. Verify correctness with checksums enabled

**Validation:** Full test suite passes, benchmark produces expected output

## Key Code Patterns

### MPI Address Exchange
```cpp
// Each process gets engine address, exchanges via MPI_Allgather
std::string my_addr = engine.self();
std::vector<std::string> all_addrs = exchange_addresses(my_addr);
```

### RDMA Bulk Transfer (Broker pulling from Sender)
```cpp
// Sender exposes
tl::bulk local_bulk = engine.expose(segments, tl::bulk_mode::read_only);
rpc_notify.on(broker)(batch_id, size, local_bulk);

// Broker pulls
tl::bulk recv_bulk = engine.expose(recv_segments, tl::bulk_mode::write_only);
remote_bulk.on(sender_ep) >> recv_bulk;  // >> = pull
```

### ABT-IO File Writes
```cpp
off_t offset = file_offset_.fetch_add(size);  // Atomic offset
ssize_t ret = abt_io_pwrite(abt_io_, fd, data, size, offset);  // Blocks ULT, not ES
// For non-blocking:
abt_io_op_t* op = abt_io_pwrite_nb(abt_io_, fd, data, size, offset, &ret);
// Later: abt_io_op_wait(op); abt_io_op_free(op);
```

### Passthrough Strategy (Receiver pulls from Sender)
```cpp
// Broker receives sender's bulk handle
void BrokerProvider::on_batch_passthrough(const tl::request& req,
                                          uint64_t batch_id, uint64_t size,
                                          tl::bulk& sender_bulk) {
    tl::endpoint sender_ep = req.get_endpoint();

    // Forward sender's bulk handle to receiver
    // Receiver will pull directly from sender's memory
    bool receiver_done = rpc_forward_passthrough_.on(receiver_ep_)(
        batch_id, size, sender_bulk, sender_ep.to_string());

    // Only ack sender AFTER receiver confirms completion
    req.respond(true);
}

// Receiver pulls directly from sender
void ReceiverProvider::on_passthrough(const tl::request& req,
                                      uint64_t batch_id, uint64_t size,
                                      tl::bulk& sender_bulk,
                                      const std::string& sender_addr) {
    tl::endpoint sender_ep = engine_.lookup(sender_addr);
    auto buffer = buffer_pool_->acquire();
    tl::bulk local_bulk = engine_.expose({{buffer.data, size}}, tl::bulk_mode::write_only);

    sender_bulk.on(sender_ep) >> local_bulk;  // Pull from sender

    process_batch(batch_id, buffer.data, size);
    buffer_pool_->release(buffer);
    req.respond(true);  // Signal broker that pull is complete
}
```

### Thallium Synchronization (NOT POSIX)
```cpp
tl::mutex mtx_;
tl::condition_variable cv_;

void wait_for_condition() {
    std::unique_lock<tl::mutex> lock(mtx_);
    cv_.wait(lock, [this]{ return condition_met_; });
}

void signal_condition() {
    std::lock_guard<tl::mutex> lock(mtx_);
    condition_met_ = true;
    cv_.notify_one();
}
```

### BrokerProvider::process_batch Implementation
```cpp
void BrokerProvider::process_batch(const tl::request& req, uint64_t batch_id,
                                   void* data, size_t size, uint32_t expected_checksum,
                                   tl::endpoint sender_ep, tl::bulk* sender_bulk) {
    // Verify checksum if enabled
    if (verify_checksums_ && expected_checksum != 0) {
        uint32_t actual = compute_crc32(data, size);
        if (actual != expected_checksum) {
            throw std::runtime_error("Checksum mismatch");
        }
    }

    // Get file offset atomically
    off_t offset = file_offset_.fetch_add(size);

    // Handle based on strategy
    switch (broker_config_.forward_strategy) {

    case ForwardStrategy::RELOAD_FROM_FILE: {
        // Write to file, wait, reload, then forward
        write_to_file(data, size, offset);

        // Ack sender based on timing
        if (broker_config_.ack_timing == AckTiming::ACK_ON_PERSIST) {
            req.respond(true);
        }

        // Reload from file into fresh buffer
        auto reload_handle = reload_pool_->acquire();
        read_from_file(reload_handle.data, size, offset);

        // Forward to receiver
        forward_data(batch_id, reload_handle.data, size, reload_handle.bulk, expected_checksum);
        reload_pool_->release(reload_handle);
        break;
    }

    case ForwardStrategy::REUSE_AFTER_PERSIST: {
        // Write to file, wait, then forward from same buffer
        write_to_file(data, size, offset);

        // Ack sender after persist
        if (broker_config_.ack_timing == AckTiming::ACK_ON_PERSIST) {
            req.respond(true);
        }

        // Forward from same buffer (must be read-write)
        // Get the bulk handle associated with this buffer
        tl::bulk& local_bulk = get_bulk_for_buffer(data);
        forward_data(batch_id, data, size, local_bulk, expected_checksum);
        break;
    }

    case ForwardStrategy::FORWARD_IMMEDIATE: {
        // Ack sender immediately (before file write)
        if (broker_config_.ack_timing == AckTiming::ACK_ON_RECEIVE) {
            req.respond(true);
        }

        // Start file write (can be async)
        ssize_t* write_ret = new ssize_t;
        abt_io_op_t* write_op = abt_io_pwrite_nb(abt_io_, file_fd_,
                                                  data, size, offset, write_ret);

        // Forward to receiver in parallel
        tl::bulk& local_bulk = get_bulk_for_buffer(data);
        forward_data(batch_id, data, size, local_bulk, expected_checksum);

        // Wait for file write to complete before releasing buffer
        abt_io_op_wait(write_op);
        abt_io_op_free(write_op);
        delete write_ret;

        // Ack sender after persist if not already done
        if (broker_config_.ack_timing == AckTiming::ACK_ON_PERSIST) {
            req.respond(true);
        }
        break;
    }

    case ForwardStrategy::PASSTHROUGH: {
        // sender_bulk must be valid for passthrough
        if (!sender_bulk) {
            throw std::runtime_error("Passthrough requires sender bulk handle");
        }

        bool file_write_started = false;
        abt_io_op_t* write_op = nullptr;
        ssize_t* write_ret = nullptr;

        // Optionally write to file in parallel
        if (broker_config_.passthrough_persist) {
            // Need to pull data first for file write
            auto local_handle = recv_pool_->acquire();
            tl::bulk local_bulk = get_engine().expose(
                {{local_handle.data, size}}, tl::bulk_mode::write_only);
            sender_bulk->on(sender_ep) >> local_bulk;

            write_ret = new ssize_t;
            write_op = abt_io_pwrite_nb(abt_io_, file_fd_,
                                        local_handle.data, size, offset, write_ret);
            file_write_started = true;
        }

        // Forward sender's bulk handle to receiver
        // Receiver will pull directly from sender
        std::string sender_addr = sender_ep.to_string();
        bool receiver_done = rpc_forward_passthrough_.on(receiver_ep_)(
            batch_id, size, *sender_bulk, sender_addr, expected_checksum);

        // Wait for file write if started
        if (file_write_started) {
            abt_io_op_wait(write_op);
            abt_io_op_free(write_op);
            delete write_ret;
        }

        // IMPORTANT: Only ack sender AFTER receiver confirms
        // This is true regardless of ack_timing setting for passthrough
        req.respond(true);
        break;
    }
    }

    bytes_received_ += size;
}
```

### Concurrency Control Pattern
```cpp
// Sender: Limit concurrent in-flight batches using condition variable
void SenderProvider::wait_for_slot() {
    std::unique_lock<tl::mutex> lock(inflight_mutex_);
    inflight_cv_.wait(lock, [this] {
        return inflight_count_ < config_.max_concurrent_batches;
    });
    inflight_count_++;
}

void SenderProvider::on_ack(const tl::request& req, uint64_t batch_id) {
    {
        std::lock_guard<tl::mutex> lock(inflight_mutex_);
        inflight_count_--;
        batches_acked_++;
    }
    inflight_cv_.notify_one();
    req.respond();  // Ack the ack (or disable_response)
}

// Main send loop
void SenderProvider::run() {
    size_t batch_size = config_.message_size * config_.messages_per_batch;
    size_t total_batches = (total_bytes_ + batch_size - 1) / batch_size;

    start_time_ = std::chrono::high_resolution_clock::now();

    for (uint64_t batch_id = 0; batch_id < total_batches; batch_id++) {
        wait_for_slot();  // Block if max concurrent reached

        size_t this_batch_size = std::min(batch_size, total_bytes_ - batch_id * batch_size);

        switch (config_.mode) {
        case TransferMode::RPC_INLINE:
            send_rpc_inline(batch_id, /* generate data */, this_batch_size);
            break;
        case TransferMode::RDMA_DIRECT:
            send_rdma_direct(batch_id, /* generate data */, this_batch_size);
            break;
        case TransferMode::RDMA_REGISTERED:
            auto handle = buffer_pool_->acquire();
            generate_batch_data(handle.data, this_batch_size, batch_id);
            send_rdma_registered(batch_id, handle);
            // Note: buffer released in on_ack callback
            break;
        }

        batches_sent_++;
        bytes_sent_ += this_batch_size;
    }

    // Wait for all acks
    {
        std::unique_lock<tl::mutex> lock(inflight_mutex_);
        inflight_cv_.wait(lock, [this, total_batches] {
            return batches_acked_ >= total_batches;
        });
    }

    end_time_ = std::chrono::high_resolution_clock::now();

    // Signal broker that sender is done
    rpc_sender_done_.on(broker_ep_)();
}
```

## Verification & Testing

### Correctness Verification
1. **Throughput measurement**: Time from first send to last receive
2. **Data integrity**: Optional CRC32 checksum verification (configurable)
3. **Byte count**: Total received == total sent
4. **Batch ordering**: Track batch IDs if ordered delivery needed
5. **Completion semantics**: Sender considers batch "done" upon broker acknowledgment

### Testing Strategy

#### Unit Tests (`tests/unit/`)
```cpp
// test_buffer_pool.cpp
TEST_CASE("BufferPool acquire/release") {
    tl::engine engine("tcp", THALLIUM_SERVER_MODE);
    BufferPool pool(engine, 4096, 4, tl::bulk_mode::read_write);

    auto h1 = pool.acquire();
    auto h2 = pool.acquire();
    REQUIRE(pool.available() == 2);

    pool.release(h1);
    REQUIRE(pool.available() == 3);
}

// test_config.cpp
TEST_CASE("Config validation") {
    // Invalid: passthrough with reload_from_file
    YAML::Node node;
    node["broker"]["forward_strategy"] = "passthrough";
    node["sender_to_broker"]["transfer_mode"] = "rpc_inline";
    // passthrough requires RDMA, should fail validation
    REQUIRE_THROWS(PipelineConfig::from_yaml(node));
}
```

#### Integration Tests (`tests/integration/`)
```cpp
// test_full_pipeline.cpp
// Launch 3 processes, verify data transfer
TEST_CASE("Full pipeline RPC mode") {
    // Fork 3 processes or use MPI test runner
    // Verify total bytes match, checksums pass
}
```

### Benchmarking Output Format

```
=== Mochi Streaming Pipeline Benchmark ===
Configuration: config.yaml
Date: 2024-01-15 10:30:00

--- Parameters ---
Total data:           100 GB (107374182400 bytes)
Message size (S):     1 MB
Messages/batch (M):   64
Max concurrent (B):   4
Transfer mode:        RDMA_REGISTERED
Ack timing:           ack_on_persist
Forward strategy:     reuse_after_persist
Network protocol:     ofi+verbs

--- Results ---
Total time:           45.23 seconds
Throughput:           2.21 GB/s
Sender time:          44.89 seconds
Receiver time:        45.01 seconds

Batches sent:         1600
Batches received:     1600
Checksum errors:      0

--- Detailed Timing ---
Avg batch latency:    28.1 ms
Min batch latency:    12.3 ms
Max batch latency:    89.7 ms
File write time:      23.4 seconds
RDMA transfer time:   21.8 seconds
```

### Sample Configuration Files

#### `configs/high_throughput.yaml` (Weak consistency, max speed)
```yaml
pipeline:
  total_data_bytes: 107374182400
  network_protocol: "ofi+verbs"
  verify_checksums: false
  thallium:
    progress_thread: true
    rpc_thread_count: 8

sender_to_broker:
  message_size: 4194304        # 4 MB
  messages_per_batch: 16       # 64 MB batches
  max_concurrent_batches: 8
  transfer_mode: "rdma_registered"

broker:
  output_file: "/mnt/nvme/output.dat"
  concurrent_rdma_pulls: 8
  ack_timing: "ack_on_receive"
  forward_strategy: "forward_immediate"
  passthrough_persist: false
  abt_io:
    concurrent_writes: 16

broker_to_receiver:
  message_size: 4194304
  messages_per_batch: 16
  max_concurrent_batches: 8
  transfer_mode: "rdma_registered"
```

#### `configs/strong_consistency.yaml` (Durability guarantee)
```yaml
pipeline:
  total_data_bytes: 107374182400
  network_protocol: "ofi+verbs"
  verify_checksums: true
  thallium:
    progress_thread: true
    rpc_thread_count: 4

sender_to_broker:
  message_size: 1048576
  messages_per_batch: 32
  max_concurrent_batches: 4
  transfer_mode: "rdma_registered"

broker:
  output_file: "/mnt/nvme/output.dat"
  concurrent_rdma_pulls: 4
  ack_timing: "ack_on_persist"
  forward_strategy: "reload_from_file"
  passthrough_persist: true
  abt_io:
    concurrent_writes: 8

broker_to_receiver:
  message_size: 1048576
  messages_per_batch: 32
  max_concurrent_batches: 4
  transfer_mode: "rdma_direct"
```

#### `configs/passthrough.yaml` (Direct sender->receiver)
```yaml
pipeline:
  total_data_bytes: 107374182400
  network_protocol: "ofi+verbs"
  verify_checksums: false
  thallium:
    progress_thread: true
    rpc_thread_count: 4

sender_to_broker:
  message_size: 4194304
  messages_per_batch: 16
  max_concurrent_batches: 8
  transfer_mode: "rdma_direct"  # Must be RDMA for passthrough

broker:
  output_file: "/mnt/nvme/output.dat"
  concurrent_rdma_pulls: 1      # Not used much in passthrough
  ack_timing: "ack_on_receive"  # Ignored in passthrough
  forward_strategy: "passthrough"
  passthrough_persist: false    # Skip file I/O entirely
  abt_io:
    concurrent_writes: 1

broker_to_receiver:
  message_size: 4194304         # Must match sender
  messages_per_batch: 16
  max_concurrent_batches: 8
  transfer_mode: "rdma_direct"  # Receiver pulls from sender
```

## Consistency & Forwarding Strategies

### Sender Acknowledgment Timing

| Strategy | Description |
|----------|-------------|
| `ack_on_persist` | Sender receives ack after data is persisted to disk (strong durability) |
| `ack_on_receive` | Sender receives ack after data is pulled into broker buffer (faster, weaker guarantee) |

### Broker Forwarding Strategies

| Strategy | Description | Buffer Requirements |
|----------|-------------|---------------------|
| `reload_from_file` | Wait for persistence, reload data from file, then forward to receiver | Receive buffer can be released after write |
| `reuse_after_persist` | Wait for persistence, reuse receive buffer to forward (no reload) | Buffer must be exposed as read-write |
| `forward_immediate` | Forward to receiver immediately from buffer, don't wait for persistence | Buffer held until forward completes |
| `passthrough` | Pass sender's bulk handle to receiver; receiver pulls directly from sender's memory | No broker buffer needed for data; broker must delay sender ack until receiver confirms |

### Strategy Implications

```
ack_on_persist + reload_from_file     : Strongest consistency, highest latency
ack_on_persist + reuse_after_persist  : Strong consistency, moderate latency
ack_on_receive + forward_immediate    : Weak consistency, low latency
ack_on_receive + passthrough          : Sender ack delayed until receiver done (special case)
```

**Passthrough special handling:** When using `passthrough`, the broker receives the sender's bulk handle and forwards it to the receiver. The receiver pulls directly from sender's memory. The broker must NOT ack the sender until the receiver confirms completion, regardless of the `ack_on_receive` setting.

**Passthrough file I/O:** Configurable via `passthrough_persist`:
- `true`: Broker still pulls data and writes to file (in parallel with receiver pull from sender)
- `false`: Data bypasses broker file I/O entirely; sender->receiver direct transfer

### Detailed Strategy Flow Diagrams

#### Strategy: ack_on_persist + reload_from_file
```
Sender                      Broker                          Receiver
   |                          |                                |
   |--[batch,bulk]----------->|                                |
   |                          |--RDMA pull from sender         |
   |                          |--abt_io_pwrite(data)           |
   |                          |  (wait for completion)         |
   |                          |--abt_io_pread(reload_buf)      |
   |<---------[ack]-----------| (after pwrite done)            |
   |                          |--[forward batch]-------------->|
   |                          |                                |--process
   |                          |<--------------[ack]------------|
```

#### Strategy: ack_on_persist + reuse_after_persist
```
Sender                      Broker                          Receiver
   |                          |                                |
   |--[batch,bulk]----------->|                                |
   |                          |--RDMA pull from sender         |
   |                          |--abt_io_pwrite(recv_buf)       |
   |                          |  (wait for completion)         |
   |<---------[ack]-----------| (after pwrite done)            |
   |                          |--[forward from recv_buf]------>|
   |                          |                                |--process
   |                          |<--------------[ack]------------|
   |                          |--release recv_buf              |
```

#### Strategy: ack_on_receive + forward_immediate
```
Sender                      Broker                          Receiver
   |                          |                                |
   |--[batch,bulk]----------->|                                |
   |                          |--RDMA pull from sender         |
   |<---------[ack]-----------| (immediately after pull)       |
   |                          |--abt_io_pwrite(data)           |
   |                          |--[forward batch]-------------->| (parallel)
   |                          |                                |--process
   |                          |<--------------[ack]------------|
   |                          |  (pwrite may still be ongoing) |
```

#### Strategy: passthrough (passthrough_persist=false)
```
Sender                      Broker                          Receiver
   |                          |                                |
   |--[batch,sender_bulk]---->|                                |
   |                          |--[sender_bulk,sender_addr]---->|
   |                          |                                |--RDMA pull from SENDER
   |                          |                                |--process
   |                          |<--------------[ack]------------|
   |<---------[ack]-----------| (only after receiver done)     |
```

#### Strategy: passthrough (passthrough_persist=true)
```
Sender                      Broker                          Receiver
   |                          |                                |
   |--[batch,sender_bulk]---->|                                |
   |                          |--RDMA pull from sender         |
   |                          |--abt_io_pwrite(data)           | (parallel)
   |                          |--[sender_bulk,sender_addr]---->|
   |                          |                                |--RDMA pull from SENDER
   |                          |                                |--process
   |                          |<--------------[ack]------------|
   |<---------[ack]-----------| (after BOTH file write AND receiver done)
```

## Threading Requirements

**Important:** All threading and synchronization must use Argobots/Thallium primitives, NOT POSIX threads.

| Use This | NOT This |
|----------|----------|
| `tl::mutex` | `std::mutex`, `pthread_mutex_t` |
| `tl::condition_variable` | `std::condition_variable`, `pthread_cond_t` |
| `tl::thread` | `std::thread`, `pthread_create` |
| `ABT_eventual` | `std::future` |
| `tl::xstream`, `tl::pool` | Thread pools |

ABT-IO handles io_uring internally - just use the ABT-IO API (`abt_io_pwrite`, etc.) without any io_uring setup.

## Dependencies

- CMake >= 3.15, C++17 compiler
- MPI (OpenMPI or MPICH)
- mochi-thallium, mochi-margo, mochi-abt-io
- mercury, libfabric (tcp,rxm or verbs)
- yaml-cpp

## Configuration Validation Rules

The `PipelineConfig::validate()` method should enforce:

1. **Passthrough requires RDMA**: If `forward_strategy == passthrough`, then `sender_to_broker.transfer_mode` must be `RDMA_DIRECT` or `RDMA_REGISTERED`

2. **Reuse buffer needs read-write**: If `forward_strategy == reuse_after_persist`, the broker's receive buffer must be exposed as `tl::bulk_mode::read_write`

3. **Passthrough message sizes match**: If `forward_strategy == passthrough`, then `sender_to_broker.message_size == broker_to_receiver.message_size`

4. **Concurrent limits**: `max_concurrent_batches > 0`, `concurrent_writes > 0`

5. **File path valid**: `broker.output_file` directory exists and is writable

## Error Handling

```cpp
// Wrap RPC handlers with try-catch
void BrokerProvider::on_batch_rdma(const tl::request& req, ...) {
    try {
        // ... processing
        req.respond(true);
    } catch (const std::exception& e) {
        std::cerr << "Error processing batch: " << e.what() << std::endl;
        req.respond(false);  // Signal error to sender
    }
}

// Check ABT-IO return values
void BrokerProvider::write_to_file(const void* data, size_t size, off_t offset) {
    ssize_t ret = abt_io_pwrite(abt_io_, file_fd_, data, size, offset);
    if (ret != static_cast<ssize_t>(size)) {
        throw std::runtime_error("abt_io_pwrite failed: wrote " +
                                 std::to_string(ret) + " of " + std::to_string(size));
    }
    bytes_written_ += size;
}

// Handle MPI errors
int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);
    // ... check return values of MPI calls
}
```

## Edge Cases

1. **Last batch smaller**: Total data may not divide evenly by batch size
2. **Empty batches**: Handle `size == 0` gracefully
3. **Engine finalization order**: Finalize engine before MPI_Finalize
4. **Buffer pool exhaustion**: `acquire()` blocks; ensure enough buffers for concurrency level
5. **File size mismatch**: Verify final file size matches `total_data_bytes`

## Running the Benchmark

```bash
# Build
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make -j

# Run with 3 MPI ranks
mpirun -np 3 ./pipeline_benchmark ../configs/default.yaml

# Run on multiple nodes (example with SLURM)
srun -N 3 -n 3 --ntasks-per-node=1 ./pipeline_benchmark config.yaml

# Run parameter sweep
python3 scripts/benchmark_sweep.py --config-template template.yaml --output results/
```

## Critical Reference Files

- `/home/ubuntu/doc-update/mochi-thallium/include/thallium/engine.hpp`
- `/home/ubuntu/doc-update/mochi-thallium/include/thallium/bulk.hpp`
- `/home/ubuntu/doc-update/mochi-thallium/include/thallium/provider.hpp`
- `/home/ubuntu/doc-update/mochi-thallium/include/thallium/mutex.hpp`
- `/home/ubuntu/doc-update/mochi-thallium/include/thallium/condition_variable.hpp`
- `/home/ubuntu/doc-update/mochi-abt-io/include/abt-io.h`
- `/home/ubuntu/doc-update/mochi-thallium/docs/examples/thallium/08_rdma/`
