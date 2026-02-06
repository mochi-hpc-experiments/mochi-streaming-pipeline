# Mochi Streaming Pipeline Benchmark

A high-performance benchmark for evaluating streaming data transfer strategies
using the [Mochi](https://www.mcs.anl.gov/research/projects/mochi/) software
ecosystem. This benchmark measures throughput and latency for different combinations
of transfer modes, acknowledgment timing, and forwarding strategies in a 3-process
pipeline where the middle process also needs to persist the data.

## Architecture

The benchmark implements a streaming pipeline with three MPI processes:

```
┌────────────┐         ┌────────────┐         ┌────────────┐
│   Sender   │ ──────> │   Broker   │ ──────> │  Receiver  │
│  (Rank 0)  │  RPC/   │  (Rank 1)  │  RPC/   │  (Rank 2)  │
│            │  RDMA   │            │  RDMA   │            │
└────────────┘         └────────────┘         └────────────┘
                             │
                             ▼
                       ┌──────────┐
                       │   Disk   │
                       │ (Output) │
                       └──────────┘
```

- **Sender**: Generates synthetic data and sends it in batches to the Broker
- **Broker**: Receives data, persists it to disk, and forwards it to the Receiver
- **Receiver**: Receives forwarded data and verifies integrity

The benchmark uses:
- **Thallium** for RPC and RDMA communication
- **ABT-IO** for asynchronous file I/O with optional io_uring support
- **MPI** for process coordination and address exchange
- **YamlCPP** for configuration

## Dependencies

- C++17 compiler (GCC 8+ or Clang 10+)
- CMake 3.14+
- MPI implementation (OpenMPI, MPICH, etc.)
- Mochi libraries:
  - Thallium
  - Margo
  - Mercury
  - ABT-IO
  - Argobots
- yaml-cpp
- libfabric (for RDMA support)
- liburing (optional, for io_uring support)

## Installing the dependencieswith Spack

From the cloned repository, execute the following.

```bash
spack env create stream-env spack.yaml
spack env activate stream-env
spack install
```

## Building

With the above spack environment activated, execute the following.

```bash
mkdir build && cd build
cmake ..
make
```

The benchmark executable will be created at `build/pipeline_benchmark`.

## Running

The benchmark requires exactly 3 MPI processes:

```bash
mpirun -np 3 ./build/pipeline_benchmark <config.yaml>
```

### Example

```bash
# Run with the default configuration template
mpirun -np 3 ./build/pipeline_benchmark configs/config_template.yaml

# Run a quick test with minimal data
mpirun -np 3 ./build/pipeline_benchmark configs/test_small.yaml
```

## Configuration

All benchmark parameters are specified in a YAML configuration file.
See `configs/config_template.yaml` for a fully documented template with
all available options.

### Key Configuration Sections

```yaml
pipeline:
  total_data_bytes: 107374182400   # Total data to transfer (100 GB)
  network_protocol: "tcp"          # Network protocol
  verify_checksums: true           # Enable CRC32 verification

sender_to_broker:
  message_size: 1048576            # 1 MB per message
  messages_per_batch: 4            # 4 messages per batch (4 MB batches)
  max_concurrent_batches: 4        # Max in-flight batches
  transfer_mode: "rdma_registered" # Transfer mode

broker:
  output_file: "/tmp/output.dat"
  ack_timing: "ack_on_persist"
  forward_strategy: "reuse_after_persist"
  abt_io:
    concurrent_writes: 4
    num_urings: 0                  # Set > 0 to enable io_uring

broker_to_receiver:
  # Similar to sender_to_broker
```

## Transfer Modes

The benchmark supports three data transfer modes:

### RPC Inline (`rpc_inline`)

Data is serialized directly into the RPC message body.

- **Pros**: Simple, works without RDMA hardware, no memory registration overhead
- **Cons**: Limited by RPC message size, requires data copy
- **Best for**: Small messages, systems without RDMA support

### RDMA Direct (`rdma_direct`)

Each batch uses a freshly registered memory buffer for RDMA transfer.

- **Pros**: Flexible buffer management, no pre-allocation required
- **Cons**: Memory registration overhead per transfer
- **Best for**: Variable-size transfers, memory-constrained environments

### RDMA Registered (`rdma_registered`)

Uses a pre-registered buffer pool for zero-copy RDMA transfers.

- **Pros**: Best performance, no per-transfer registration overhead
- **Cons**: Requires upfront memory allocation
- **Best for**: High-throughput scenarios with RDMA hardware

## Acknowledgment Timing

Controls when the Broker acknowledges receipt to the Sender:

### Ack on Persist (`ack_on_persist`)

The Broker acknowledges only after data is written to disk.

- **Behavior**: Sender blocks until disk write completes
- **Guarantees**: Data is durable before Sender continues
- **Trade-off**: Higher latency, stronger durability

### Ack on Receive (`ack_on_receive`)

The Broker acknowledges as soon as data is received in memory.

- **Behavior**: Sender can continue while disk write happens
- **Guarantees**: Data is in Broker memory, may not be on disk
- **Trade-off**: Lower latency, weaker durability

## Forwarding Strategies

The Broker supports four strategies for forwarding data to the Receiver:

### Reload from File (`reload_from_file`)

```
Sender → Broker → Disk → Broker → Receiver
                   ↓
              Read back
```

1. Write data to disk
2. Wait for write completion
3. Read data back from disk into a new buffer
4. Forward to Receiver

- **Use case**: Verifying disk persistence, testing read performance
- **Latency**: Highest (write + read + forward)

### Reuse After Persist (`reuse_after_persist`)

```
Sender → Broker → Disk
            └→ Receiver
```

1. Write data to disk
2. Wait for write completion
3. Forward from the same memory buffer

- **Use case**: Balanced durability and performance
- **Latency**: Medium (write + forward)

### Forward Immediate (`forward_immediate`)

```
Sender → Broker ──→ Receiver
              ↘
           Disk (parallel)
```

1. Start async disk write
2. Immediately forward to Receiver (in parallel)
3. Wait for disk write to complete

- **Use case**: Maximum throughput, data remains in memory
- **Latency**: Lowest for Receiver, write happens in background

### Passthrough (`passthrough`)

```
Sender ─────────────────→ Receiver
          ↓ (metadata)
        Broker
          ↓ (optional)
        Disk
```

1. Broker receives Sender's RDMA handle (metadata only)
2. Broker forwards the handle to Receiver
3. Receiver pulls directly from Sender's memory
4. (Optional) Broker writes to disk in parallel

- **Use case**: Minimum latency, Sender memory held longer
- **Latency**: Lowest (direct Sender→Receiver transfer)
- **Requirements**: RDMA transfer mode required

## io_uring Support

The benchmark supports Linux io_uring for high-performance asynchronous I/O:

```yaml
broker:
  abt_io:
    num_urings: 2                    # Enable with 2 io_uring instances
    liburing_flags:                  # Optional tuning flags
      - "IORING_SETUP_SQPOLL"
      - "IORING_SETUP_SINGLE_ISSUER"
```

Requires Linux kernel 5.1+ and ABT-IO built with liburing support.

## Example Configurations

The `configs/` directory contains several pre-configured examples:

| File | Description |
|------|-------------|
| `config_template.yaml` | Fully documented template with defaults |
| `test_small.yaml` | Quick test with 64 MB data |
| `test_uring.yaml` | Test with io_uring enabled |
| `rpc_only.yaml` | TCP/RPC-only mode (no RDMA) |
| `high_throughput.yaml` | Optimized for maximum throughput |
| `strong_consistency.yaml` | Maximum durability guarantees |
| `passthrough.yaml` | Direct sender-to-receiver transfer |

## Output

The benchmark outputs timing statistics:

```
=== Mochi Streaming Pipeline Benchmark ===
Configuration: configs/test_small.yaml

--- Parameters ---
Total data:           64.00 MB (67108864 bytes)
Transfer mode:        rdma_registered
Forward strategy:     reuse_after_persist
...

--- Results ---
Total time:           1.168 s
Throughput:           0.05 GB/s
Batches sent:         16
Batches acked:        16

--- Detailed Timing ---
Avg batch latency:    145.4 ms
Min batch latency:    143.1 ms
Max batch latency:    152.8 ms

=== Benchmark Complete ===
```

## Acknowledgments

This benchmark uses the [Mochi](https://www.mcs.anl.gov/research/projects/mochi/) software ecosystem developed at Argonne National Laboratory.
