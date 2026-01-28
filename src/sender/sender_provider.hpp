#ifndef MOCHI_PIPELINE_SENDER_PROVIDER_HPP
#define MOCHI_PIPELINE_SENDER_PROVIDER_HPP

#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <atomic>
#include <memory>
#include <vector>

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
 *
 * Acknowledgement is implicit via RPC response - no separate ack RPC needed.
 */
class SenderProvider : public tl::provider<SenderProvider> {
public:
    SenderProvider(tl::engine& engine, uint16_t provider_id,
                   const TransferConfig& config,
                   const std::string& broker_address,
                   size_t total_bytes,
                   bool verify_checksums,
                   size_t num_sender_xstreams = 4);

    ~SenderProvider();

    /**
     * @brief Start the data transfer (blocking).
     */
    void run();

    double get_elapsed_seconds() const;
    size_t get_bytes_sent() const { return bytes_sent_.load(); }
    size_t get_batches_sent() const { return batches_sent_.load(); }
    size_t get_batches_acked() const { return batches_acked_.load(); }
    const LatencyStats& get_latency_stats() const { return latency_stats_; }

private:
    // Send batch in a ULT (allows concurrency, RPC response is ack)
    void send_batch_ult(uint64_t batch_id, size_t size);

    // Generate batch data and return checksum if enabled
    uint32_t generate_batch_data(void* buffer, size_t size, uint64_t batch_id);

    // Concurrency control
    void acquire_slot();
    void release_slot();

    // Configuration
    TransferConfig config_;
    size_t total_bytes_;
    bool verify_checksums_;

    // Engine and broker endpoint
    tl::engine& engine_;
    tl::provider_handle broker_ph_;

    // Buffer pool for RDMA_REGISTERED mode
    std::unique_ptr<BufferPool> buffer_pool_;

    // Pool and execution streams for spawning sender ULTs
    tl::managed<tl::pool> sender_pool_;
    std::vector<tl::managed<tl::xstream>> sender_xstreams_;

    // Concurrency control
    tl::mutex slot_mutex_;
    tl::condition_variable slot_cv_;
    size_t inflight_count_ = 0;

    // Statistics
    std::atomic<size_t> bytes_sent_{0};
    std::atomic<size_t> batches_sent_{0};
    std::atomic<size_t> batches_acked_{0};
    LatencyStats latency_stats_;
    tl::mutex stats_mutex_;

    Timer total_timer_;

    // RPCs to broker
    tl::remote_procedure rpc_batch_rpc_;
    tl::remote_procedure rpc_batch_rdma_;
    tl::remote_procedure rpc_sender_done_;
};

} // namespace mochi_pipeline

#endif // MOCHI_PIPELINE_SENDER_PROVIDER_HPP
