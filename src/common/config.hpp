#ifndef MOCHI_PIPELINE_CONFIG_HPP
#define MOCHI_PIPELINE_CONFIG_HPP

#include <yaml-cpp/yaml.h>
#include <string>
#include <cstdint>
#include <stdexcept>

namespace mochi_pipeline {

// Transfer mode enumeration
enum class TransferMode {
    RPC_INLINE,        // Send batch data as RPC argument
    RDMA_DIRECT,       // Expose buffer per batch, broker pulls
    RDMA_REGISTERED    // Use pre-registered buffer pool, broker pulls
};

// Acknowledgment timing enumeration
enum class AckTiming {
    ACK_ON_PERSIST,    // Ack after data is persisted to disk
    ACK_ON_RECEIVE     // Ack after data is received into buffer
};

// Forwarding strategy enumeration
enum class ForwardStrategy {
    RELOAD_FROM_FILE,    // Wait for persist, reload from file, then forward
    REUSE_AFTER_PERSIST, // Wait for persist, forward from same buffer
    FORWARD_IMMEDIATE,   // Forward immediately, don't wait for persist
    PASSTHROUGH          // Pass sender's bulk handle to receiver directly
};

// Transfer configuration for sender->broker or broker->receiver
struct TransferConfig {
    size_t message_size;            // S bytes per message
    size_t messages_per_batch;      // M messages per batch
    size_t max_concurrent_batches;  // B max in-flight batches
    TransferMode mode;

    // Computed helper
    size_t batch_size() const { return message_size * messages_per_batch; }
};

// ABT-IO configuration
struct AbtIoConfig {
    size_t concurrent_writes;       // Max concurrent pwrite operations
};

// Thallium configuration
struct ThalliumConfig {
    bool progress_thread;           // Use dedicated progress thread
    int rpc_thread_count;           // Number of RPC handler threads
};

// Broker-specific configuration
struct BrokerConfig {
    std::string output_file;
    size_t concurrent_rdma_pulls;
    AckTiming ack_timing;
    ForwardStrategy forward_strategy;
    bool passthrough_persist;       // Write to file even in passthrough mode
    AbtIoConfig abt_io;
};

// Top-level pipeline configuration
struct PipelineConfig {
    size_t total_data_bytes;
    std::string network_protocol;   // "tcp", "ofi+verbs", etc.
    bool verify_checksums;
    ThalliumConfig thallium;

    TransferConfig sender_to_broker;
    BrokerConfig broker;
    TransferConfig broker_to_receiver;

    // Load configuration from YAML file
    static PipelineConfig from_yaml(const std::string& path);

    // Validate configuration, throws on invalid combinations
    void validate() const;

    // Compute total number of batches
    size_t total_batches() const {
        size_t batch_size = sender_to_broker.batch_size();
        return (total_data_bytes + batch_size - 1) / batch_size;
    }
};

// String conversion helpers
std::string to_string(TransferMode mode);
std::string to_string(AckTiming timing);
std::string to_string(ForwardStrategy strategy);

TransferMode transfer_mode_from_string(const std::string& str);
AckTiming ack_timing_from_string(const std::string& str);
ForwardStrategy forward_strategy_from_string(const std::string& str);

// Configuration validation exception
class ConfigValidationError : public std::runtime_error {
public:
    explicit ConfigValidationError(const std::string& msg)
        : std::runtime_error("Configuration validation error: " + msg) {}
};

} // namespace mochi_pipeline

#endif // MOCHI_PIPELINE_CONFIG_HPP
