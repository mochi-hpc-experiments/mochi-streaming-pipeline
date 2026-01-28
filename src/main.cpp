#include <mpi.h>
#include <thallium.hpp>
#include <iostream>
#include <iomanip>
#include <vector>
#include <string>
#include <chrono>
#include <ctime>
#include <memory>

#include "common/config.hpp"
#include "common/timing.hpp"
#include "sender/sender_provider.hpp"
#include "broker/broker_provider.hpp"
#include "receiver/receiver_provider.hpp"

namespace tl = thallium;
using namespace mochi_pipeline;

/**
 * @brief Exchange Thallium addresses across all MPI ranks.
 *
 * @param my_addr This process's Thallium address
 * @param comm MPI communicator
 * @return Vector of all addresses indexed by rank
 */
std::vector<std::string> exchange_addresses(const std::string& my_addr, MPI_Comm comm) {
    // Get max address length across all ranks
    int my_len = static_cast<int>(my_addr.size() + 1);
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

/**
 * @brief Print benchmark configuration.
 */
void print_config(const PipelineConfig& config) {
    std::cout << "\n--- Parameters ---" << std::endl;
    std::cout << "Total data:           " << format_bytes(config.total_data_bytes)
              << " (" << config.total_data_bytes << " bytes)" << std::endl;
    std::cout << "Message size (S):     " << format_bytes(config.sender_to_broker.message_size)
              << std::endl;
    std::cout << "Messages/batch (M):   " << config.sender_to_broker.messages_per_batch
              << std::endl;
    std::cout << "Batch size:           " << format_bytes(config.sender_to_broker.batch_size())
              << std::endl;
    std::cout << "Max concurrent (B):   " << config.sender_to_broker.max_concurrent_batches
              << std::endl;
    std::cout << "Transfer mode:        " << to_string(config.sender_to_broker.mode)
              << std::endl;
    std::cout << "Ack timing:           " << to_string(config.broker.ack_timing)
              << std::endl;
    std::cout << "Forward strategy:     " << to_string(config.broker.forward_strategy)
              << std::endl;
    std::cout << "Network protocol:     " << config.network_protocol << std::endl;
    std::cout << "Verify checksums:     " << (config.verify_checksums ? "true" : "false")
              << std::endl;
    std::cout << "Output file:          " << config.broker.output_file << std::endl;
    std::cout << std::endl;
}

/**
 * @brief Print benchmark results.
 */
void print_results(const PipelineConfig& config, double elapsed,
                   const SenderProvider* sender) {
    double throughput_gbps = static_cast<double>(config.total_data_bytes) / elapsed /
                             (1024.0 * 1024.0 * 1024.0);

    std::cout << "\n--- Results ---" << std::endl;
    std::cout << "Total time:           " << format_duration(elapsed) << std::endl;
    std::cout << "Throughput:           " << std::fixed << std::setprecision(2)
              << throughput_gbps << " GB/s" << std::endl;

    if (sender) {
        std::cout << "Sender time:          " << format_duration(sender->get_elapsed_seconds())
                  << std::endl;
        std::cout << "Batches sent:         " << sender->get_batches_sent() << std::endl;
        std::cout << "Batches acked:        " << sender->get_batches_acked() << std::endl;

        const auto& stats = sender->get_latency_stats();
        if (stats.count() > 0) {
            std::cout << "\n--- Detailed Timing ---" << std::endl;
            std::cout << "Avg batch latency:    " << std::fixed << std::setprecision(1)
                      << stats.avg_ms() << " ms" << std::endl;
            std::cout << "Min batch latency:    " << stats.min_ms() << " ms" << std::endl;
            std::cout << "Max batch latency:    " << stats.max_ms() << " ms" << std::endl;
            std::cout << "Stddev:               " << stats.stddev_ms() << " ms" << std::endl;
        }
    }
}

int main(int argc, char** argv) {
    // Initialize MPI
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    // Require exactly 3 ranks
    if (size != 3) {
        if (rank == 0) {
            std::cerr << "Error: Requires exactly 3 MPI ranks (got " << size << ")"
                      << std::endl;
            std::cerr << "Usage: mpirun -np 3 " << argv[0] << " <config.yaml>"
                      << std::endl;
        }
        MPI_Finalize();
        return 1;
    }

    // Check command line arguments
    if (argc != 2) {
        if (rank == 0) {
            std::cerr << "Usage: " << argv[0] << " <config.yaml>" << std::endl;
        }
        MPI_Finalize();
        return 1;
    }

    // Load and validate configuration
    PipelineConfig config;
    try {
        config = PipelineConfig::from_yaml(argv[1]);
        config.validate();
    } catch (const std::exception& e) {
        if (rank == 0) {
            std::cerr << "Error loading configuration: " << e.what() << std::endl;
        }
        MPI_Finalize();
        return 1;
    }

    // Print header and config (rank 0 only)
    if (rank == 0) {
        // Get current time
        auto now = std::chrono::system_clock::now();
        auto time = std::chrono::system_clock::to_time_t(now);

        std::cout << "=== Mochi Streaming Pipeline Benchmark ===" << std::endl;
        std::cout << "Configuration: " << argv[1] << std::endl;
        std::cout << "Date: " << std::put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S")
                  << std::endl;

        print_config(config);
    }

    // Initialize Thallium engine
    // All processes are servers since they handle RPCs
    tl::engine engine(config.network_protocol, THALLIUM_SERVER_MODE,
                      config.thallium.progress_thread, config.thallium.rpc_thread_count);

    // Exchange addresses
    std::string my_addr = static_cast<std::string>(engine.self());
    auto addresses = exchange_addresses(my_addr, MPI_COMM_WORLD);

    // Print addresses
    if (rank == 0) {
        std::cout << "Addresses:" << std::endl;
        std::cout << "  Sender (rank 0):   " << addresses[0] << std::endl;
        std::cout << "  Broker (rank 1):   " << addresses[1] << std::endl;
        std::cout << "  Receiver (rank 2): " << addresses[2] << std::endl;
        std::cout << std::endl;
    }

    // Ensure all engines are ready
    MPI_Barrier(MPI_COMM_WORLD);

    // Pointers to providers (only set on their respective ranks)
    std::unique_ptr<SenderProvider> sender;
    std::unique_ptr<BrokerProvider> broker;
    std::unique_ptr<ReceiverProvider> receiver;

    // Create providers on each rank
    try {
        if (rank == 0) {
            sender = std::make_unique<SenderProvider>(
                engine, 1, config.sender_to_broker,
                addresses[1], config.total_data_bytes,
                config.verify_checksums,
                config.thallium.rpc_thread_count);
        }
        else if (rank == 1) {
            broker = std::make_unique<BrokerProvider>(
                engine, 1, config.broker,
                config.sender_to_broker, config.broker_to_receiver,
                addresses[2], config.total_data_bytes,
                config.verify_checksums);
        }
        else {
            receiver = std::make_unique<ReceiverProvider>(
                engine, 1, config.broker_to_receiver,
                config.total_data_bytes,
                config.verify_checksums);
        }
    } catch (const std::exception& e) {
        std::cerr << "[Rank " << rank << "] Error creating provider: " << e.what() << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Ensure all providers are created and RPCs are registered
    MPI_Barrier(MPI_COMM_WORLD);

    // Record start time
    double start_time = MPI_Wtime();

    // Run the pipeline
    try {
        if (rank == 0) {
            sender->run();
        }
        else if (rank == 1) {
            broker->wait_completion();
        }
        else {
            receiver->wait_completion();
        }
    } catch (const std::exception& e) {
        std::cerr << "[Rank " << rank << "] Error: " << e.what() << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Ensure all processes complete
    MPI_Barrier(MPI_COMM_WORLD);

    double end_time = MPI_Wtime();
    double elapsed = end_time - start_time;

    // Report results from rank 0
    if (rank == 0) {
        print_results(config, elapsed, sender.get());
        std::cout << "\n=== Benchmark Complete ===" << std::endl;
    }

    // Clean shutdown
    engine.finalize();
    MPI_Finalize();

    return 0;
}
