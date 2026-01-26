#include "sender/sender_provider.hpp"
#include <iostream>
#include <cstring>

namespace mochi_pipeline {

SenderProvider::SenderProvider(tl::engine& engine, uint16_t provider_id,
                               const TransferConfig& config,
                               const std::string& broker_address,
                               size_t total_bytes,
                               bool verify_checksums)
    : tl::provider<SenderProvider>(engine, provider_id)
    , config_(config)
    , total_bytes_(total_bytes)
    , verify_checksums_(verify_checksums)
    , engine_(engine)
{
    // Look up broker endpoint
    broker_ep_ = engine_.lookup(broker_address);

    // Set up buffer pool for RDMA_REGISTERED mode
    size_t batch_size = config_.batch_size();
    if (config_.mode == TransferMode::RDMA_REGISTERED) {
        // Create buffer pool with read_only mode (broker will pull)
        buffer_pool_ = std::make_unique<BufferPool>(
            engine_, batch_size,
            config_.max_concurrent_batches + 2,  // Extra buffers for overlap
            tl::bulk_mode::read_only,
            true  // Register for RDMA
        );
    }

    // Create scratch buffer for RPC_INLINE and RDMA_DIRECT modes
    if (config_.mode == TransferMode::RPC_INLINE ||
        config_.mode == TransferMode::RDMA_DIRECT) {
        scratch_buffer_ = std::make_unique<char[]>(batch_size);
    }

    // Define RPC handler for acknowledgments from broker
    define("sender_ack", &SenderProvider::on_ack);

    // Get remote procedures to broker
    rpc_batch_rpc_ = engine_.define("broker_batch_rpc");
    rpc_batch_rdma_ = engine_.define("broker_batch_rdma");
    rpc_sender_done_ = engine_.define("broker_sender_done");
}

SenderProvider::~SenderProvider() {
    // Buffer pool cleanup handled by unique_ptr
}

void SenderProvider::on_ack(const tl::request& req, uint64_t batch_id) {
    double latency = 0.0;
    bool is_pool_buffer = false;

    {
        std::lock_guard<tl::mutex> lock(inflight_mutex_);

        auto it = inflight_batches_.find(batch_id);
        if (it != inflight_batches_.end()) {
            latency = it->second.timer.elapsed_seconds();
            is_pool_buffer = it->second.is_pool_buffer;

            // Release pool buffer if applicable
            if (is_pool_buffer && buffer_pool_) {
                buffer_pool_->release(it->second.pool_handle);
            }

            inflight_batches_.erase(it);
        }

        inflight_count_--;
        batches_acked_++;
    }

    // Record latency stats (outside lock)
    if (latency > 0) {
        std::lock_guard<tl::mutex> lock(stats_mutex_);
        latency_stats_.record(latency);
    }

    // Signal that a slot is available
    inflight_cv_.notify_one();

    req.respond(true);
}

void SenderProvider::wait_for_slot() {
    std::unique_lock<tl::mutex> lock(inflight_mutex_);
    inflight_cv_.wait(lock, [this] {
        return inflight_count_ < config_.max_concurrent_batches;
    });
    inflight_count_++;
}

void SenderProvider::release_slot(uint64_t batch_id) {
    // This is called if we need to release without waiting for ack (error case)
    std::lock_guard<tl::mutex> lock(inflight_mutex_);

    auto it = inflight_batches_.find(batch_id);
    if (it != inflight_batches_.end()) {
        if (it->second.is_pool_buffer && buffer_pool_) {
            buffer_pool_->release(it->second.pool_handle);
        }
        inflight_batches_.erase(it);
    }

    inflight_count_--;
    inflight_cv_.notify_one();
}

uint32_t SenderProvider::generate_batch_data(void* buffer, size_t size, uint64_t batch_id) {
    // Fill with deterministic pattern
    fill_test_pattern(buffer, size, batch_id);

    // Compute checksum if verification is enabled
    if (verify_checksums_) {
        return compute_crc32(buffer, size);
    }
    return 0;
}

void SenderProvider::send_rpc_inline(uint64_t batch_id, void* data, size_t size) {
    // Generate data
    uint32_t checksum = generate_batch_data(data, size, batch_id);

    // Create vector from data (will be serialized as RPC argument)
    std::vector<char> data_vec(static_cast<char*>(data),
                                static_cast<char*>(data) + size);

    // Track in-flight batch for latency measurement
    {
        std::lock_guard<tl::mutex> lock(inflight_mutex_);
        InFlightBatch batch;
        batch.timer.start();
        batch.is_pool_buffer = false;
        inflight_batches_[batch_id] = std::move(batch);
    }

    // Send RPC with data as argument
    // Broker will respond with ack
    rpc_batch_rpc_.on(broker_ep_)(batch_id, data_vec, checksum);
}

void SenderProvider::send_rdma_direct(uint64_t batch_id, void* data, size_t size) {
    // Generate data
    uint32_t checksum = generate_batch_data(data, size, batch_id);

    // Expose buffer for RDMA (broker will pull)
    std::vector<std::pair<void*, size_t>> segments;
    segments.emplace_back(data, size);
    tl::bulk bulk = engine_.expose(segments, tl::bulk_mode::read_only);

    // Track in-flight batch
    {
        std::lock_guard<tl::mutex> lock(inflight_mutex_);
        InFlightBatch batch;
        batch.bulk = std::move(bulk);
        batch.timer.start();
        batch.is_pool_buffer = false;
        inflight_batches_[batch_id] = std::move(batch);
    }

    // Notify broker to pull data
    // We need to get the bulk handle back to pass to the RPC
    tl::bulk& batch_bulk = inflight_batches_[batch_id].bulk;
    rpc_batch_rdma_.on(broker_ep_)(batch_id, size, batch_bulk, checksum);
}

void SenderProvider::send_rdma_registered(uint64_t batch_id,
                                          BufferPool::Handle& handle,
                                          size_t actual_size) {
    // Generate data into the pool buffer
    uint32_t checksum = generate_batch_data(handle.data, actual_size, batch_id);

    // Track in-flight batch (buffer remains exposed via pool)
    {
        std::lock_guard<tl::mutex> lock(inflight_mutex_);
        InFlightBatch batch;
        batch.pool_handle = std::move(handle);
        batch.timer.start();
        batch.is_pool_buffer = true;
        inflight_batches_[batch_id] = std::move(batch);
    }

    // Get reference to the bulk handle from the tracked batch
    tl::bulk& batch_bulk = inflight_batches_[batch_id].pool_handle.bulk;

    // Notify broker to pull data
    rpc_batch_rdma_.on(broker_ep_)(batch_id, actual_size, batch_bulk, checksum);
}

void SenderProvider::run() {
    size_t batch_size = config_.batch_size();
    size_t total_batches = (total_bytes_ + batch_size - 1) / batch_size;

    std::cout << "[Sender] Starting transfer: " << total_bytes_ << " bytes in "
              << total_batches << " batches (batch size: " << batch_size << ")"
              << std::endl;

    total_timer_.start();

    for (uint64_t batch_id = 0; batch_id < total_batches; batch_id++) {
        // Wait for a slot if at max concurrent batches
        wait_for_slot();

        // Calculate actual size for this batch (last batch may be smaller)
        size_t remaining = total_bytes_ - batch_id * batch_size;
        size_t this_batch_size = std::min(batch_size, remaining);

        switch (config_.mode) {
            case TransferMode::RPC_INLINE:
                send_rpc_inline(batch_id, scratch_buffer_.get(), this_batch_size);
                break;

            case TransferMode::RDMA_DIRECT:
                send_rdma_direct(batch_id, scratch_buffer_.get(), this_batch_size);
                break;

            case TransferMode::RDMA_REGISTERED: {
                auto handle = buffer_pool_->acquire();
                send_rdma_registered(batch_id, handle, this_batch_size);
                break;
            }
        }

        batches_sent_++;
        bytes_sent_ += this_batch_size;

        // Progress output every 10% or 100 batches
        if (total_batches >= 10 && batch_id > 0 &&
            (batch_id % (total_batches / 10) == 0 || batch_id % 100 == 0)) {
            std::cout << "[Sender] Progress: " << batch_id << "/" << total_batches
                      << " batches (" << (batch_id * 100 / total_batches) << "%)"
                      << std::endl;
        }
    }

    // Wait for all acknowledgments
    {
        std::unique_lock<tl::mutex> lock(inflight_mutex_);
        inflight_cv_.wait(lock, [this, total_batches] {
            return batches_acked_ >= total_batches;
        });
    }

    total_timer_.stop();

    // Signal broker that sender is done
    rpc_sender_done_.on(broker_ep_)();

    std::cout << "[Sender] Transfer complete: " << bytes_sent_.load() << " bytes in "
              << total_timer_.elapsed_seconds() << " seconds" << std::endl;
}

double SenderProvider::get_elapsed_seconds() const {
    return total_timer_.elapsed_seconds();
}

} // namespace mochi_pipeline
