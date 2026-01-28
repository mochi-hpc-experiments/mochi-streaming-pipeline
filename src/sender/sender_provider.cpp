#include "sender/sender_provider.hpp"
#include <iostream>
#include <cstring>

namespace mochi_pipeline {

SenderProvider::SenderProvider(tl::engine& engine, uint16_t provider_id,
                               const TransferConfig& config,
                               const std::string& broker_address,
                               size_t total_bytes,
                               bool verify_checksums,
                               size_t num_sender_xstreams)
    : tl::provider<SenderProvider>(engine, provider_id)
    , config_(config)
    , total_bytes_(total_bytes)
    , verify_checksums_(verify_checksums)
    , engine_(engine)
{
    // Look up broker endpoint
    broker_ph_ = tl::provider_handle{engine_.lookup(broker_address), 1};

    // Set up buffer pool for RDMA_REGISTERED mode
    size_t batch_size = config_.batch_size();
    if (config_.mode == TransferMode::RDMA_REGISTERED) {
        buffer_pool_ = std::make_unique<BufferPool>(
            engine_, batch_size,
            config_.max_concurrent_batches + 2,
            tl::bulk_mode::read_only,
            true
        );
    }

    // Create pool and execution streams for sender ULTs
    sender_pool_ = tl::pool::create(tl::pool::access::mpmc);
    for (size_t i = 0; i < num_sender_xstreams; i++) {
        sender_xstreams_.push_back(
            tl::xstream::create(tl::scheduler::predef::basic_wait, *sender_pool_)
        );
    }

    // Get remote procedures to broker
    rpc_batch_rpc_ = engine_.define("broker_batch_rpc");
    rpc_batch_rdma_ = engine_.define("broker_batch_rdma");
    rpc_sender_done_ = engine_.define("broker_sender_done");
}

SenderProvider::~SenderProvider() {
    // Join and clean up xstreams
    for (auto& xs : sender_xstreams_) {
        xs->join();
    }
}

void SenderProvider::acquire_slot() {
    std::unique_lock<tl::mutex> lock(slot_mutex_);
    slot_cv_.wait(lock, [this] {
        return inflight_count_ < config_.max_concurrent_batches;
    });
    inflight_count_++;
}

void SenderProvider::release_slot() {
    {
        std::lock_guard<tl::mutex> lock(slot_mutex_);
        inflight_count_--;
    }
    slot_cv_.notify_one();
}

uint32_t SenderProvider::generate_batch_data(void* buffer, size_t size, uint64_t batch_id) {
    fill_test_pattern(buffer, size, batch_id);
    if (verify_checksums_) {
        return compute_crc32(buffer, size);
    }
    return 0;
}

void SenderProvider::send_batch_ult(uint64_t batch_id, size_t size) {
    Timer batch_timer;
    batch_timer.start();

    bool success = false;

    switch (config_.mode) {
        case TransferMode::RPC_INLINE: {
            // Allocate temporary buffer, generate data, send as RPC argument
            std::vector<char> data_vec(size);
            uint32_t checksum = generate_batch_data(data_vec.data(), size, batch_id);

            // Blocking RPC - response is the acknowledgement
            success = rpc_batch_rpc_.on(broker_ph_)(batch_id, data_vec, checksum);
            break;
        }

        case TransferMode::RDMA_DIRECT: {
            // Allocate temporary buffer, generate data, expose for RDMA
            std::vector<char> buffer(size);
            uint32_t checksum = generate_batch_data(buffer.data(), size, batch_id);

            std::vector<std::pair<void*, size_t>> segments;
            segments.emplace_back(buffer.data(), size);
            tl::bulk bulk = engine_.expose(segments, tl::bulk_mode::read_only);

            // Blocking RPC - broker pulls data, response is acknowledgement
            success = rpc_batch_rdma_.on(broker_ph_)(batch_id, size, bulk, checksum);
            // bulk handle and buffer remain valid until RPC returns
            break;
        }

        case TransferMode::RDMA_REGISTERED: {
            // Acquire pre-registered buffer from pool
            auto handle = buffer_pool_->acquire();
            uint32_t checksum = generate_batch_data(handle.data, size, batch_id);

            // Blocking RPC - broker pulls data, response is acknowledgement
            success = rpc_batch_rdma_.on(broker_ph_)(batch_id, size, handle.bulk, checksum);

            // Release buffer back to pool after RPC completes
            buffer_pool_->release(handle);
            break;
        }
    }

    batch_timer.stop();

    // Update statistics
    if (success) {
        batches_acked_++;
        std::lock_guard<tl::mutex> lock(stats_mutex_);
        latency_stats_.record(batch_timer.elapsed_seconds());
    }

    // Release slot to allow next batch
    release_slot();
}

void SenderProvider::run() {
    size_t batch_size = config_.batch_size();
    size_t total_batches = (total_bytes_ + batch_size - 1) / batch_size;

    std::cout << "[Sender] Starting transfer: " << total_bytes_ << " bytes in "
              << total_batches << " batches (batch size: " << batch_size << ")"
              << std::endl;

    total_timer_.start();

    // Spawn ULTs for each batch to allow concurrent sends
    std::vector<tl::managed<tl::thread>> ults;
    ults.reserve(total_batches);

    for (uint64_t batch_id = 0; batch_id < total_batches; batch_id++) {
        // Wait for a slot before spawning
        acquire_slot();

        // Calculate actual size for this batch
        size_t remaining = total_bytes_ - batch_id * batch_size;
        size_t this_batch_size = std::min(batch_size, remaining);

        batches_sent_++;
        bytes_sent_ += this_batch_size;

        // Spawn ULT on sender pool to send this batch
        auto ult = sender_pool_->make_thread(
            [this, batch_id, this_batch_size]() {
                send_batch_ult(batch_id, this_batch_size);
            }
        );
        ults.push_back(std::move(ult));

        // Progress output
        if (total_batches >= 10 && batch_id > 0 &&
            batch_id % (total_batches / 10) == 0) {
            std::cout << "[Sender] Progress: " << batch_id << "/" << total_batches
                      << " batches (" << (batch_id * 100 / total_batches) << "%)"
                      << std::endl;
        }
    }

    // Wait for all ULTs to complete
    for (auto& ult : ults) {
        ult->join();
    }

    total_timer_.stop();

    // Signal broker that sender is done
    rpc_sender_done_.on(broker_ph_)();

    std::cout << "[Sender] Transfer complete: " << bytes_sent_.load() << " bytes, "
              << batches_acked_.load() << " batches acked in "
              << total_timer_.elapsed_seconds() << " seconds" << std::endl;
}

double SenderProvider::get_elapsed_seconds() const {
    return total_timer_.elapsed_seconds();
}

} // namespace mochi_pipeline
