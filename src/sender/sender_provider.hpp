#ifndef MOCHI_PIPELINE_SENDER_PROVIDER_HPP
#define MOCHI_PIPELINE_SENDER_PROVIDER_HPP

#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/string.hpp>
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
 * @brief SenderProvider handles sending data batches to the broker.
 *
 * Supports three transfer modes:
 * - RPC_INLINE: Data sent as RPC argument
 * - RDMA_DIRECT: Buffer exposed per batch, broker pulls
 * - RDMA_REGISTERED: Pre-registered buffer pool, broker pulls
 */
class SenderProvider : public tl::provider<SenderProvider> {
public:
    /**
     * @brief Construct a sender provider.
     *
     * @param engine Thallium engine
     * @param provider_id Provider ID
     * @param config Transfer configuration
     * @param broker_address Address of the broker
     * @param total_bytes Total bytes to send
     * @param verify_checksums Enable checksum verification
     */
    SenderProvider(tl::engine& engine, uint16_t provider_id,
                   const TransferConfig& config,
                   const std::string& broker_address,
                   size_t total_bytes,
                   bool verify_checksums);

    ~SenderProvider();

    /**
     * @brief Start the data transfer (blocking).
     */
    void run();

    /**
     * @brief Get elapsed time in seconds.
     */
    double get_elapsed_seconds() const;

    /**
     * @brief Get total bytes sent.
     */
    size_t get_bytes_sent() const { return bytes_sent_.load(); }

    /**
     * @brief Get total batches sent.
     */
    size_t get_batches_sent() const { return batches_sent_.load(); }

    /**
     * @brief Get total batches acknowledged.
     */
    size_t get_batches_acked() const { return batches_acked_.load(); }

    /**
     * @brief Get latency statistics.
     */
    const LatencyStats& get_latency_stats() const { return latency_stats_; }

private:
    // RPC handler called by broker to acknowledge batch
    void on_ack(const tl::request& req, uint64_t batch_id);

    // Transfer implementations
    void send_rpc_inline(uint64_t batch_id, void* data, size_t size);
    void send_rdma_direct(uint64_t batch_id, void* data, size_t size);
    void send_rdma_registered(uint64_t batch_id, BufferPool::Handle& handle, size_t actual_size);

    // Generate batch data and return checksum if enabled
    uint32_t generate_batch_data(void* buffer, size_t size, uint64_t batch_id);

    // Wait for an available slot in the in-flight batches
    void wait_for_slot();

    // Release a slot after batch is acknowledged
    void release_slot(uint64_t batch_id);

    // Configuration
    TransferConfig config_;
    size_t total_bytes_;
    bool verify_checksums_;

    // Engine and broker endpoint
    tl::engine& engine_;
    tl::endpoint broker_ep_;

    // Buffer pool for RDMA_REGISTERED mode
    std::unique_ptr<BufferPool> buffer_pool_;

    // Scratch buffer for RPC_INLINE and RDMA_DIRECT modes
    std::unique_ptr<char[]> scratch_buffer_;

    // Tracking in-flight batches for RDMA modes (need to keep bulk handles alive)
    struct InFlightBatch {
        tl::bulk bulk;                   // Bulk handle (for RDMA_DIRECT)
        BufferPool::Handle pool_handle;  // Pool handle (for RDMA_REGISTERED)
        Timer timer;                     // For latency measurement
        bool is_pool_buffer;
    };
    std::unordered_map<uint64_t, InFlightBatch> inflight_batches_;

    // Concurrency control using Argobots primitives
    tl::mutex inflight_mutex_;
    tl::condition_variable inflight_cv_;
    size_t inflight_count_ = 0;

    // Statistics
    std::atomic<size_t> bytes_sent_{0};
    std::atomic<size_t> batches_sent_{0};
    std::atomic<size_t> batches_acked_{0};
    LatencyStats latency_stats_;
    tl::mutex stats_mutex_;

    Timer total_timer_;

    // RPCs to broker
    tl::remote_procedure rpc_batch_rpc_;      // Send batch via RPC arg
    tl::remote_procedure rpc_batch_rdma_;     // Notify broker of RDMA batch
    tl::remote_procedure rpc_sender_done_;    // Signal completion
};

} // namespace mochi_pipeline

#endif // MOCHI_PIPELINE_SENDER_PROVIDER_HPP
