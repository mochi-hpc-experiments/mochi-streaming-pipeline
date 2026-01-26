#ifndef MOCHI_PIPELINE_BROKER_PROVIDER_HPP
#define MOCHI_PIPELINE_BROKER_PROVIDER_HPP

#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <abt-io.h>
#include <atomic>
#include <memory>
#include <unordered_map>

#include "common/config.hpp"
#include "common/buffer_pool.hpp"
#include "common/batch.hpp"
#include "common/timing.hpp"

namespace tl = thallium;

namespace mochi_pipeline {

/**
 * @brief BrokerProvider handles receiving data from sender, persisting to file,
 *        and forwarding to receiver.
 *
 * Supports multiple forwarding strategies:
 * - RELOAD_FROM_FILE: Wait for persist, reload from file, then forward
 * - REUSE_AFTER_PERSIST: Wait for persist, forward from same buffer
 * - FORWARD_IMMEDIATE: Forward immediately, don't wait for persist
 * - PASSTHROUGH: Pass sender's bulk handle directly to receiver
 */
class BrokerProvider : public tl::provider<BrokerProvider> {
public:
    /**
     * @brief Construct a broker provider.
     *
     * @param engine Thallium engine
     * @param provider_id Provider ID
     * @param broker_config Broker configuration
     * @param recv_config Configuration for receiving from sender
     * @param send_config Configuration for sending to receiver
     * @param receiver_address Address of the receiver
     * @param total_bytes Total bytes expected
     * @param verify_checksums Enable checksum verification
     */
    BrokerProvider(tl::engine& engine, uint16_t provider_id,
                   const BrokerConfig& broker_config,
                   const TransferConfig& recv_config,
                   const TransferConfig& send_config,
                   const std::string& receiver_address,
                   size_t total_bytes,
                   bool verify_checksums);

    ~BrokerProvider();

    /**
     * @brief Wait for all data to be processed.
     */
    void wait_completion();

    /**
     * @brief Get bytes received from sender.
     */
    size_t get_bytes_received() const { return bytes_received_.load(); }

    /**
     * @brief Get bytes written to file.
     */
    size_t get_bytes_written() const { return bytes_written_.load(); }

    /**
     * @brief Get bytes forwarded to receiver.
     */
    size_t get_bytes_forwarded() const { return bytes_forwarded_.load(); }

private:
    // RPC handlers from sender
    void on_batch_rpc(const tl::request& req, uint64_t batch_id,
                      const std::vector<char>& data, uint32_t checksum);
    void on_batch_rdma(const tl::request& req, uint64_t batch_id,
                       size_t size, tl::bulk& remote_bulk, uint32_t checksum);
    void on_sender_done(const tl::request& req);

    // RPC handler for acknowledgment from receiver
    void on_receiver_ack(const tl::request& req, uint64_t batch_id);

    // Core processing after receiving batch data
    void process_batch(const tl::request& req, uint64_t batch_id,
                       void* data, size_t size, uint32_t expected_checksum,
                       tl::endpoint sender_ep, tl::bulk* sender_bulk,
                       BufferPool::Handle* recv_handle);

    // File I/O
    void write_to_file(const void* data, size_t size, off_t offset);
    void read_from_file(void* data, size_t size, off_t offset);

    // Forward to receiver
    void forward_rpc_inline(uint64_t batch_id, const void* data, size_t size,
                            uint32_t checksum);
    void forward_rdma(uint64_t batch_id, void* data, size_t size,
                      tl::bulk& local_bulk, uint32_t checksum);
    void forward_passthrough(uint64_t batch_id, size_t size,
                             tl::bulk& sender_bulk, const std::string& sender_addr,
                             uint32_t checksum);

    // Acknowledge sender
    void ack_sender(const tl::endpoint& sender_ep, uint64_t batch_id);

    // Configuration
    BrokerConfig broker_config_;
    TransferConfig recv_config_;
    TransferConfig send_config_;
    size_t total_bytes_;
    bool verify_checksums_;

    // Engine and endpoints
    tl::engine& engine_;
    tl::endpoint receiver_ep_;

    // ABT-IO
    abt_io_instance_id abt_io_;
    int file_fd_;
    std::atomic<off_t> file_offset_{0};

    // Buffer pools
    std::unique_ptr<BufferPool> recv_pool_;    // For receiving from sender
    std::unique_ptr<BufferPool> reload_pool_;  // For reload_from_file strategy

    // Tracking in-flight forwards
    struct InFlightForward {
        BufferPool::Handle recv_handle;   // Handle to release after forward completes
        bool has_recv_handle;
        Timer timer;
    };
    std::unordered_map<uint64_t, InFlightForward> inflight_forwards_;
    tl::mutex forward_mutex_;
    tl::condition_variable forward_cv_;
    size_t inflight_forward_count_ = 0;

    // Completion tracking
    tl::mutex completion_mutex_;
    tl::condition_variable completion_cv_;
    std::atomic<size_t> bytes_received_{0};
    std::atomic<size_t> bytes_written_{0};
    std::atomic<size_t> bytes_forwarded_{0};
    std::atomic<size_t> batches_received_{0};
    std::atomic<size_t> batches_forwarded_{0};
    bool sender_done_ = false;
    bool all_forwarded_ = false;

    // Timing
    Timer total_timer_;

    // RPCs
    tl::remote_procedure rpc_sender_ack_;        // Ack sender
    tl::remote_procedure rpc_forward_rpc_;       // Forward via RPC
    tl::remote_procedure rpc_forward_rdma_;      // Forward via RDMA
    tl::remote_procedure rpc_forward_passthrough_; // Forward passthrough
    tl::remote_procedure rpc_receiver_done_;     // Signal receiver completion
};

} // namespace mochi_pipeline

#endif // MOCHI_PIPELINE_BROKER_PROVIDER_HPP
