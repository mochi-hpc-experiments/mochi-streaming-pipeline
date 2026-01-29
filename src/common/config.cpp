#include "common/config.hpp"
#include <fstream>
#include <filesystem>
#include <algorithm>

namespace mochi_pipeline {

// String conversion implementations
std::string to_string(TransferMode mode) {
    switch (mode) {
        case TransferMode::RPC_INLINE: return "rpc_inline";
        case TransferMode::RDMA_DIRECT: return "rdma_direct";
        case TransferMode::RDMA_REGISTERED: return "rdma_registered";
    }
    return "unknown";
}

std::string to_string(AckTiming timing) {
    switch (timing) {
        case AckTiming::ACK_ON_PERSIST: return "ack_on_persist";
        case AckTiming::ACK_ON_RECEIVE: return "ack_on_receive";
    }
    return "unknown";
}

std::string to_string(ForwardStrategy strategy) {
    switch (strategy) {
        case ForwardStrategy::RELOAD_FROM_FILE: return "reload_from_file";
        case ForwardStrategy::REUSE_AFTER_PERSIST: return "reuse_after_persist";
        case ForwardStrategy::FORWARD_IMMEDIATE: return "forward_immediate";
        case ForwardStrategy::PASSTHROUGH: return "passthrough";
    }
    return "unknown";
}

TransferMode transfer_mode_from_string(const std::string& str) {
    std::string lower = str;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

    if (lower == "rpc_inline") return TransferMode::RPC_INLINE;
    if (lower == "rdma_direct") return TransferMode::RDMA_DIRECT;
    if (lower == "rdma_registered") return TransferMode::RDMA_REGISTERED;

    throw ConfigValidationError("Invalid transfer mode: " + str);
}

AckTiming ack_timing_from_string(const std::string& str) {
    std::string lower = str;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

    if (lower == "ack_on_persist") return AckTiming::ACK_ON_PERSIST;
    if (lower == "ack_on_receive") return AckTiming::ACK_ON_RECEIVE;

    throw ConfigValidationError("Invalid ack timing: " + str);
}

ForwardStrategy forward_strategy_from_string(const std::string& str) {
    std::string lower = str;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);

    if (lower == "reload_from_file") return ForwardStrategy::RELOAD_FROM_FILE;
    if (lower == "reuse_after_persist") return ForwardStrategy::REUSE_AFTER_PERSIST;
    if (lower == "forward_immediate") return ForwardStrategy::FORWARD_IMMEDIATE;
    if (lower == "passthrough") return ForwardStrategy::PASSTHROUGH;

    throw ConfigValidationError("Invalid forward strategy: " + str);
}

// Helper to get value with default
template<typename T>
T get_or_default(const YAML::Node& node, const std::string& key, T default_val) {
    if (node[key]) {
        return node[key].as<T>();
    }
    return default_val;
}

// Parse TransferConfig from YAML node
TransferConfig parse_transfer_config(const YAML::Node& node) {
    TransferConfig config;

    config.message_size = get_or_default<size_t>(node, "message_size", 1048576);  // 1 MB default
    config.messages_per_batch = get_or_default<size_t>(node, "messages_per_batch", 64);
    config.max_concurrent_batches = get_or_default<size_t>(node, "max_concurrent_batches", 4);

    std::string mode_str = get_or_default<std::string>(node, "transfer_mode", "rdma_registered");
    config.mode = transfer_mode_from_string(mode_str);

    return config;
}

// Parse AbtIoConfig from YAML node
AbtIoConfig parse_abt_io_config(const YAML::Node& node) {
    AbtIoConfig config;
    config.concurrent_writes = get_or_default<size_t>(node, "concurrent_writes", 8);
    config.num_urings = get_or_default<size_t>(node, "num_urings", 0);

    // Parse liburing_flags array
    if (node["liburing_flags"]) {
        for (const auto& flag : node["liburing_flags"]) {
            config.liburing_flags.push_back(flag.as<std::string>());
        }
    }

    return config;
}

// Parse ThalliumConfig from YAML node
ThalliumConfig parse_thallium_config(const YAML::Node& node) {
    ThalliumConfig config;
    config.progress_thread = get_or_default<bool>(node, "progress_thread", true);
    config.rpc_thread_count = get_or_default<int>(node, "rpc_thread_count", 4);
    return config;
}

// Parse BrokerConfig from YAML node
BrokerConfig parse_broker_config(const YAML::Node& node) {
    BrokerConfig config;

    config.output_file = get_or_default<std::string>(node, "output_file", "/tmp/pipeline_output.dat");
    config.concurrent_rdma_pulls = get_or_default<size_t>(node, "concurrent_rdma_pulls", 8);

    std::string ack_str = get_or_default<std::string>(node, "ack_timing", "ack_on_persist");
    config.ack_timing = ack_timing_from_string(ack_str);

    std::string fwd_str = get_or_default<std::string>(node, "forward_strategy", "reuse_after_persist");
    config.forward_strategy = forward_strategy_from_string(fwd_str);

    config.passthrough_persist = get_or_default<bool>(node, "passthrough_persist", true);

    if (node["abt_io"]) {
        config.abt_io = parse_abt_io_config(node["abt_io"]);
    } else {
        config.abt_io.concurrent_writes = 8;
        config.abt_io.num_urings = 0;
        // liburing_flags left empty by default
    }

    return config;
}

PipelineConfig PipelineConfig::from_yaml(const std::string& path) {
    // Check file exists
    if (!std::filesystem::exists(path)) {
        throw ConfigValidationError("Configuration file not found: " + path);
    }

    YAML::Node root;
    try {
        root = YAML::LoadFile(path);
    } catch (const YAML::Exception& e) {
        throw ConfigValidationError("Failed to parse YAML: " + std::string(e.what()));
    }

    PipelineConfig config;

    // Parse top-level pipeline settings
    if (root["pipeline"]) {
        const auto& pipeline = root["pipeline"];
        config.total_data_bytes = get_or_default<size_t>(pipeline, "total_data_bytes", 107374182400ULL);  // 100 GB
        config.network_protocol = get_or_default<std::string>(pipeline, "network_protocol", "tcp");
        config.verify_checksums = get_or_default<bool>(pipeline, "verify_checksums", false);

        if (pipeline["thallium"]) {
            config.thallium = parse_thallium_config(pipeline["thallium"]);
        } else {
            config.thallium.progress_thread = true;
            config.thallium.rpc_thread_count = 4;
        }
    } else {
        // Use defaults
        config.total_data_bytes = 107374182400ULL;
        config.network_protocol = "tcp";
        config.verify_checksums = false;
        config.thallium.progress_thread = true;
        config.thallium.rpc_thread_count = 4;
    }

    // Parse sender_to_broker config
    if (root["sender_to_broker"]) {
        config.sender_to_broker = parse_transfer_config(root["sender_to_broker"]);
    } else {
        config.sender_to_broker.message_size = 1048576;
        config.sender_to_broker.messages_per_batch = 64;
        config.sender_to_broker.max_concurrent_batches = 4;
        config.sender_to_broker.mode = TransferMode::RDMA_REGISTERED;
    }

    // Parse broker config
    if (root["broker"]) {
        config.broker = parse_broker_config(root["broker"]);
    } else {
        config.broker.output_file = "/tmp/pipeline_output.dat";
        config.broker.concurrent_rdma_pulls = 8;
        config.broker.ack_timing = AckTiming::ACK_ON_PERSIST;
        config.broker.forward_strategy = ForwardStrategy::REUSE_AFTER_PERSIST;
        config.broker.passthrough_persist = true;
        config.broker.abt_io.concurrent_writes = 8;
        config.broker.abt_io.num_urings = 0;
    }

    // Parse broker_to_receiver config
    if (root["broker_to_receiver"]) {
        config.broker_to_receiver = parse_transfer_config(root["broker_to_receiver"]);
    } else {
        config.broker_to_receiver.message_size = 1048576;
        config.broker_to_receiver.messages_per_batch = 32;
        config.broker_to_receiver.max_concurrent_batches = 4;
        config.broker_to_receiver.mode = TransferMode::RDMA_DIRECT;
    }

    return config;
}

void PipelineConfig::validate() const {
    // Rule 1: Passthrough requires RDMA mode for sender_to_broker
    if (broker.forward_strategy == ForwardStrategy::PASSTHROUGH) {
        if (sender_to_broker.mode == TransferMode::RPC_INLINE) {
            throw ConfigValidationError(
                "Passthrough strategy requires RDMA mode (rdma_direct or rdma_registered) "
                "for sender_to_broker, got rpc_inline");
        }
    }

    // Rule 2: Passthrough message sizes should match (warning, not error)
    if (broker.forward_strategy == ForwardStrategy::PASSTHROUGH) {
        if (sender_to_broker.message_size != broker_to_receiver.message_size) {
            // Log warning but don't fail - sizes can differ
        }
    }

    // Rule 3: Concurrent limits must be positive
    if (sender_to_broker.max_concurrent_batches == 0) {
        throw ConfigValidationError("sender_to_broker.max_concurrent_batches must be > 0");
    }
    if (broker_to_receiver.max_concurrent_batches == 0) {
        throw ConfigValidationError("broker_to_receiver.max_concurrent_batches must be > 0");
    }
    if (broker.concurrent_rdma_pulls == 0) {
        throw ConfigValidationError("broker.concurrent_rdma_pulls must be > 0");
    }
    if (broker.abt_io.concurrent_writes == 0) {
        throw ConfigValidationError("broker.abt_io.concurrent_writes must be > 0");
    }

    // Rule 4: Message size must be positive
    if (sender_to_broker.message_size == 0) {
        throw ConfigValidationError("sender_to_broker.message_size must be > 0");
    }
    if (broker_to_receiver.message_size == 0) {
        throw ConfigValidationError("broker_to_receiver.message_size must be > 0");
    }

    // Rule 5: Messages per batch must be positive
    if (sender_to_broker.messages_per_batch == 0) {
        throw ConfigValidationError("sender_to_broker.messages_per_batch must be > 0");
    }
    if (broker_to_receiver.messages_per_batch == 0) {
        throw ConfigValidationError("broker_to_receiver.messages_per_batch must be > 0");
    }

    // Rule 6: Total data bytes must be positive
    if (total_data_bytes == 0) {
        throw ConfigValidationError("total_data_bytes must be > 0");
    }

    // Rule 7: Check output file directory exists
    std::filesystem::path output_path(broker.output_file);
    std::filesystem::path parent_dir = output_path.parent_path();
    if (!parent_dir.empty() && !std::filesystem::exists(parent_dir)) {
        throw ConfigValidationError(
            "Output file directory does not exist: " + parent_dir.string());
    }

    // Rule 8: RPC thread count must be non-negative
    if (thallium.rpc_thread_count < 0) {
        throw ConfigValidationError("thallium.rpc_thread_count must be >= 0");
    }
}

} // namespace mochi_pipeline
