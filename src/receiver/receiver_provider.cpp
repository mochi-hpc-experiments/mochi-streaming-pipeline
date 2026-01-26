#include "receiver/receiver_provider.hpp"
#include <iostream>

namespace mochi_pipeline {

ReceiverProvider::ReceiverProvider(tl::engine& engine, uint16_t provider_id,
                                   const TransferConfig& config,
                                   size_t total_bytes,
                                   bool verify_checksums)
    : tl::provider<ReceiverProvider>(engine, provider_id)
    , config_(config)
    , total_bytes_(total_bytes)
    , verify_checksums_(verify_checksums)
    , engine_(engine)
{
    // Create buffer pool for RDMA receives
    size_t batch_size = config_.batch_size();
    if (config_.mode != TransferMode::RPC_INLINE) {
        buffer_pool_ = std::make_unique<BufferPool>(
            engine_, batch_size,
            config_.max_concurrent_batches + 2,
            tl::bulk_mode::write_only,  // We pull into these buffers
            true
        );
    }

    // Define RPC handlers
    define("receiver_batch_rpc", &ReceiverProvider::on_batch_rpc);
    define("receiver_batch_rdma", &ReceiverProvider::on_batch_rdma);
    define("receiver_batch_passthrough", &ReceiverProvider::on_passthrough);
    define("receiver_done", &ReceiverProvider::on_done);

    // Get remote procedure for acking broker
    rpc_broker_ack_ = engine_.define("broker_receiver_ack");

    std::cout << "[Receiver] Initialized, waiting for data..." << std::endl;
}

ReceiverProvider::~ReceiverProvider() {
    // Buffer pool cleanup handled by unique_ptr
}

void ReceiverProvider::on_batch_rpc(const tl::request& req, uint64_t batch_id,
                                     const std::vector<char>& data,
                                     uint32_t checksum) {
    if (first_recv_) {
        total_timer_.start();
        first_recv_ = false;
    }

    process_batch(batch_id, data.data(), data.size(), checksum);

    // Ack broker
    ack_broker(req.get_endpoint(), batch_id);
    req.respond(true);
}

void ReceiverProvider::on_batch_rdma(const tl::request& req, uint64_t batch_id,
                                      size_t size, tl::bulk& remote_bulk,
                                      uint32_t checksum) {
    if (first_recv_) {
        total_timer_.start();
        first_recv_ = false;
    }

    tl::endpoint broker_ep = req.get_endpoint();

    // Acquire buffer and pull data
    auto handle = buffer_pool_->acquire();

    std::vector<std::pair<void*, size_t>> segments;
    segments.emplace_back(handle.data, size);
    tl::bulk local_bulk = engine_.expose(segments, tl::bulk_mode::write_only);

    // Pull from broker
    remote_bulk.on(broker_ep) >> local_bulk;

    process_batch(batch_id, handle.data, size, checksum);

    // Release buffer
    buffer_pool_->release(handle);

    // Ack broker
    ack_broker(broker_ep, batch_id);
    req.respond(true);
}

void ReceiverProvider::on_passthrough(const tl::request& req, uint64_t batch_id,
                                       size_t size, tl::bulk& sender_bulk,
                                       const std::string& sender_addr,
                                       uint32_t checksum) {
    if (first_recv_) {
        total_timer_.start();
        first_recv_ = false;
    }

    // Look up sender endpoint from address
    tl::endpoint sender_ep = engine_.lookup(sender_addr);

    // Acquire buffer and pull data directly from sender
    auto handle = buffer_pool_->acquire();

    std::vector<std::pair<void*, size_t>> segments;
    segments.emplace_back(handle.data, size);
    tl::bulk local_bulk = engine_.expose(segments, tl::bulk_mode::write_only);

    // Pull from SENDER (not broker)
    sender_bulk.on(sender_ep) >> local_bulk;

    process_batch(batch_id, handle.data, size, checksum);

    // Release buffer
    buffer_pool_->release(handle);

    // Respond to broker (this is the ack that tells broker to ack sender)
    req.respond(true);
}

void ReceiverProvider::on_done(const tl::request& req) {
    std::cout << "[Receiver] Broker signaled completion" << std::endl;

    {
        std::lock_guard<tl::mutex> lock(completion_mutex_);
        transfer_done_ = true;
    }
    completion_cv_.notify_all();

    req.respond(true);
}

void ReceiverProvider::process_batch(uint64_t batch_id, const void* data,
                                      size_t size, uint32_t expected_checksum) {
    // Verify checksum if enabled
    if (verify_checksums_ && expected_checksum != 0) {
        uint32_t actual = compute_crc32(data, size);
        if (actual != expected_checksum) {
            std::cerr << "[Receiver] Checksum mismatch for batch " << batch_id
                      << ": expected " << expected_checksum << ", got " << actual
                      << std::endl;
        }
    }

    // Optionally verify data pattern (more thorough but slower)
    // if (verify_checksums_) {
    //     if (!verify_test_pattern(data, size, batch_id)) {
    //         std::cerr << "[Receiver] Pattern verification failed for batch "
    //                   << batch_id << std::endl;
    //     }
    // }

    bytes_received_ += size;
    batches_received_++;

    // Progress output periodically
    size_t total_batches = (total_bytes_ + config_.batch_size() - 1) /
                           config_.batch_size();
    if (total_batches >= 10 && batches_received_ % (total_batches / 10) == 0) {
        std::cout << "[Receiver] Progress: " << batches_received_.load() << "/"
                  << total_batches << " batches (" << bytes_received_.load()
                  << " bytes)" << std::endl;
    }
}

void ReceiverProvider::ack_broker(const tl::endpoint& broker_ep, uint64_t batch_id) {
    rpc_broker_ack_.on(broker_ep)(batch_id);
}

void ReceiverProvider::wait_completion() {
    // Wait for done signal from broker
    {
        std::unique_lock<tl::mutex> lock(completion_mutex_);
        completion_cv_.wait(lock, [this] { return transfer_done_; });
    }

    total_timer_.stop();

    std::cout << "[Receiver] Complete: " << bytes_received_.load()
              << " bytes in " << total_timer_.elapsed_seconds() << " seconds"
              << std::endl;
}

double ReceiverProvider::get_elapsed_seconds() const {
    return total_timer_.elapsed_seconds();
}

} // namespace mochi_pipeline
