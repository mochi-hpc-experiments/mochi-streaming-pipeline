#ifndef MOCHI_PIPELINE_RECEIVER_PROVIDER_HPP
#define MOCHI_PIPELINE_RECEIVER_PROVIDER_HPP

#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <atomic>
#include <memory>

#include "common/config.hpp"
#include "common/buffer_pool.hpp"
#include "common/batch.hpp"
#include "common/timing.hpp"

namespace tl = thallium;

namespace mochi_pipeline {

/**
 * @brief ReceiverProvider handles receiving data from the broker.
 *
 * Supports receiving via:
 * - RPC_INLINE: Data received as RPC argument
 * - RDMA: Pull data from broker's buffer
 * - PASSTHROUGH: Pull data directly from sender's buffer
 */
class ReceiverProvider : public tl::provider<ReceiverProvider> {
public:
    /**
     * @brief Construct a receiver provider.
     *
     * @param engine Thallium engine
     * @param provider_id Provider ID
     * @param config Transfer configuration
     * @param total_bytes Total bytes expected
     * @param verify_checksums Enable checksum verification
     */
    ReceiverProvider(tl::engine& engine, uint16_t provider_id,
                     const TransferConfig& config,
                     size_t total_bytes,
                     bool verify_checksums);

    ~ReceiverProvider();

    /**
     * @brief Wait for all data to be received.
     */
    void wait_completion();

    /**
     * @brief Get total bytes received.
     */
    size_t get_bytes_received() const { return bytes_received_.load(); }

    /**
     * @brief Get elapsed time in seconds.
     */
    double get_elapsed_seconds() const;

    /**
     * @brief Get latency statistics.
     */
    const LatencyStats& get_latency_stats() const { return latency_stats_; }

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

    // Acknowledge broker
    void ack_broker(const tl::endpoint& broker_ep, uint64_t batch_id);

    // Configuration
    TransferConfig config_;
    size_t total_bytes_;
    bool verify_checksums_;

    // Engine
    tl::engine& engine_;

    // Buffer pool for RDMA receives
    std::unique_ptr<BufferPool> buffer_pool_;

    // Completion tracking
    tl::mutex completion_mutex_;
    tl::condition_variable completion_cv_;
    std::atomic<size_t> bytes_received_{0};
    std::atomic<size_t> batches_received_{0};
    bool transfer_done_ = false;

    // Timing
    Timer total_timer_;
    bool first_recv_ = true;
    LatencyStats latency_stats_;
    tl::mutex stats_mutex_;

    // RPCs
    tl::remote_procedure rpc_broker_ack_;
};

} // namespace mochi_pipeline

#endif // MOCHI_PIPELINE_RECEIVER_PROVIDER_HPP
