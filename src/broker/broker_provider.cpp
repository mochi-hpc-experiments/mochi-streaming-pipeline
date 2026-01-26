#include "broker/broker_provider.hpp"
#include <iostream>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace mochi_pipeline {

BrokerProvider::BrokerProvider(tl::engine& engine, uint16_t provider_id,
                               const BrokerConfig& broker_config,
                               const TransferConfig& recv_config,
                               const TransferConfig& send_config,
                               const std::string& receiver_address,
                               size_t total_bytes,
                               bool verify_checksums)
    : tl::provider<BrokerProvider>(engine, provider_id)
    , broker_config_(broker_config)
    , recv_config_(recv_config)
    , send_config_(send_config)
    , total_bytes_(total_bytes)
    , verify_checksums_(verify_checksums)
    , engine_(engine)
    , abt_io_(ABT_IO_INSTANCE_NULL)
    , file_fd_(-1)
{
    // Look up receiver endpoint
    receiver_ep_ = engine_.lookup(receiver_address);

    // Initialize ABT-IO
    // Use JSON config for io_uring support if needed
    std::string json_config = "{\"backing_thread_count\": " +
                               std::to_string(broker_config_.abt_io.concurrent_writes) + "}";
    struct abt_io_init_info init_info;
    memset(&init_info, 0, sizeof(init_info));
    init_info.json_config = json_config.c_str();

    abt_io_ = abt_io_init_ext(&init_info);
    if (abt_io_ == ABT_IO_INSTANCE_NULL) {
        throw std::runtime_error("Failed to initialize ABT-IO");
    }

    // Open output file
    file_fd_ = abt_io_open(abt_io_, broker_config_.output_file.c_str(),
                           O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (file_fd_ < 0) {
        abt_io_finalize(abt_io_);
        throw std::runtime_error("Failed to open output file: " +
                                 broker_config_.output_file);
    }

    // Create buffer pools
    size_t recv_batch_size = recv_config_.batch_size();

    // Determine bulk mode for receive pool based on forwarding strategy
    tl::bulk_mode recv_mode = tl::bulk_mode::write_only;
    if (broker_config_.forward_strategy == ForwardStrategy::REUSE_AFTER_PERSIST ||
        broker_config_.forward_strategy == ForwardStrategy::FORWARD_IMMEDIATE) {
        // Need read-write if we'll forward from the same buffer
        recv_mode = tl::bulk_mode::read_write;
    }

    // Only create recv_pool for RDMA modes (not needed for RPC_INLINE)
    if (recv_config_.mode != TransferMode::RPC_INLINE) {
        recv_pool_ = std::make_unique<BufferPool>(
            engine_, recv_batch_size,
            recv_config_.max_concurrent_batches + 2,
            recv_mode,
            true  // Register for RDMA
        );
    }

    // Create reload pool for RELOAD_FROM_FILE strategy
    if (broker_config_.forward_strategy == ForwardStrategy::RELOAD_FROM_FILE) {
        reload_pool_ = std::make_unique<BufferPool>(
            engine_, recv_batch_size,
            send_config_.max_concurrent_batches + 2,
            tl::bulk_mode::read_only,  // Read-only since we're pushing to receiver
            true
        );
    }

    // Define RPC handlers
    define("broker_batch_rpc", &BrokerProvider::on_batch_rpc);
    define("broker_batch_rdma", &BrokerProvider::on_batch_rdma);
    define("broker_sender_done", &BrokerProvider::on_sender_done);
    define("broker_receiver_ack", &BrokerProvider::on_receiver_ack);

    // Get remote procedures
    rpc_sender_ack_ = engine_.define("sender_ack");
    rpc_forward_rpc_ = engine_.define("receiver_batch_rpc");
    rpc_forward_rdma_ = engine_.define("receiver_batch_rdma");
    rpc_forward_passthrough_ = engine_.define("receiver_batch_passthrough");
    rpc_receiver_done_ = engine_.define("receiver_done");

    total_timer_.start();

    std::cout << "[Broker] Initialized with strategy: "
              << to_string(broker_config_.forward_strategy)
              << ", ack timing: " << to_string(broker_config_.ack_timing)
              << std::endl;
}

BrokerProvider::~BrokerProvider() {
    if (file_fd_ >= 0) {
        abt_io_close(abt_io_, file_fd_);
    }
    if (abt_io_ != ABT_IO_INSTANCE_NULL) {
        abt_io_finalize(abt_io_);
    }
}

void BrokerProvider::on_batch_rpc(const tl::request& req, uint64_t batch_id,
                                   const std::vector<char>& data, uint32_t checksum) {
    // Data received inline via RPC
    void* data_ptr = const_cast<char*>(data.data());
    size_t size = data.size();

    tl::endpoint sender_ep = req.get_endpoint();

    process_batch(req, batch_id, data_ptr, size, checksum,
                  sender_ep, nullptr, nullptr);
}

void BrokerProvider::on_batch_rdma(const tl::request& req, uint64_t batch_id,
                                    size_t size, tl::bulk& remote_bulk,
                                    uint32_t checksum) {
    tl::endpoint sender_ep = req.get_endpoint();

    // Acquire a receive buffer
    BufferPool::Handle recv_handle = recv_pool_->acquire();

    // Pull data from sender via RDMA
    std::vector<std::pair<void*, size_t>> segments;
    segments.emplace_back(recv_handle.data, size);
    tl::bulk local_bulk = engine_.expose(segments, tl::bulk_mode::write_only);

    // Pull: local << remote.on(sender)
    remote_bulk.on(sender_ep) >> local_bulk;

    process_batch(req, batch_id, recv_handle.data, size, checksum,
                  sender_ep, &remote_bulk, &recv_handle);
}

void BrokerProvider::on_sender_done(const tl::request& req) {
    std::cout << "[Broker] Sender signaled completion" << std::endl;

    {
        std::lock_guard<tl::mutex> lock(completion_mutex_);
        sender_done_ = true;
    }
    completion_cv_.notify_all();

    req.respond(true);
}

void BrokerProvider::on_receiver_ack(const tl::request& req, uint64_t batch_id) {
    {
        std::lock_guard<tl::mutex> lock(forward_mutex_);

        auto it = inflight_forwards_.find(batch_id);
        if (it != inflight_forwards_.end()) {
            // Release receive buffer if applicable
            if (it->second.has_recv_handle && recv_pool_) {
                recv_pool_->release(it->second.recv_handle);
            }
            inflight_forwards_.erase(it);
        }

        inflight_forward_count_--;
        batches_forwarded_++;
    }

    forward_cv_.notify_one();
    req.respond(true);
}

void BrokerProvider::ack_sender(const tl::endpoint& sender_ep, uint64_t batch_id) {
    rpc_sender_ack_.on(sender_ep)(batch_id);
}

void BrokerProvider::write_to_file(const void* data, size_t size, off_t offset) {
    ssize_t ret = abt_io_pwrite(abt_io_, file_fd_, data, size, offset);
    if (ret != static_cast<ssize_t>(size)) {
        throw std::runtime_error("abt_io_pwrite failed: wrote " +
                                 std::to_string(ret) + " of " + std::to_string(size));
    }
    bytes_written_ += size;
}

void BrokerProvider::read_from_file(void* data, size_t size, off_t offset) {
    ssize_t ret = abt_io_pread(abt_io_, file_fd_, data, size, offset);
    if (ret != static_cast<ssize_t>(size)) {
        throw std::runtime_error("abt_io_pread failed: read " +
                                 std::to_string(ret) + " of " + std::to_string(size));
    }
}

void BrokerProvider::forward_rpc_inline(uint64_t batch_id, const void* data,
                                         size_t size, uint32_t checksum) {
    std::vector<char> data_vec(static_cast<const char*>(data),
                                static_cast<const char*>(data) + size);

    // Call receiver's RPC handler
    rpc_forward_rpc_.on(receiver_ep_)(batch_id, data_vec, checksum);
    bytes_forwarded_ += size;
}

void BrokerProvider::forward_rdma(uint64_t batch_id, void* data, size_t size,
                                   tl::bulk& local_bulk, uint32_t checksum) {
    // Notify receiver to pull from broker
    rpc_forward_rdma_.on(receiver_ep_)(batch_id, size, local_bulk, checksum);
    bytes_forwarded_ += size;
}

void BrokerProvider::forward_passthrough(uint64_t batch_id, size_t size,
                                          tl::bulk& sender_bulk,
                                          const std::string& sender_addr,
                                          uint32_t checksum) {
    // Forward sender's bulk handle to receiver
    // Receiver will pull directly from sender
    rpc_forward_passthrough_.on(receiver_ep_)(batch_id, size, sender_bulk,
                                               sender_addr, checksum);
    bytes_forwarded_ += size;
}

void BrokerProvider::process_batch(const tl::request& req, uint64_t batch_id,
                                    void* data, size_t size, uint32_t expected_checksum,
                                    tl::endpoint sender_ep, tl::bulk* sender_bulk,
                                    BufferPool::Handle* recv_handle) {
    // Verify checksum if enabled
    if (verify_checksums_ && expected_checksum != 0) {
        uint32_t actual = compute_crc32(data, size);
        if (actual != expected_checksum) {
            std::cerr << "[Broker] Checksum mismatch for batch " << batch_id
                      << ": expected " << expected_checksum << ", got " << actual
                      << std::endl;
            req.respond(false);
            return;
        }
    }

    bytes_received_ += size;
    batches_received_++;

    // Get file offset atomically
    off_t offset = file_offset_.fetch_add(size);

    // Handle based on strategy
    switch (broker_config_.forward_strategy) {

    case ForwardStrategy::RELOAD_FROM_FILE: {
        // Write to file first
        write_to_file(data, size, offset);

        // Ack sender after persist
        if (broker_config_.ack_timing == AckTiming::ACK_ON_PERSIST) {
            ack_sender(sender_ep, batch_id);
        }
        req.respond(true);

        // Release receive buffer if we have one (no longer needed)
        if (recv_handle && recv_pool_) {
            recv_pool_->release(*recv_handle);
        }

        // Reload from file into fresh buffer
        auto reload_handle = reload_pool_->acquire();
        read_from_file(reload_handle.data, size, offset);

        // Wait for forward slot
        {
            std::unique_lock<tl::mutex> lock(forward_mutex_);
            forward_cv_.wait(lock, [this] {
                return inflight_forward_count_ < send_config_.max_concurrent_batches;
            });
            inflight_forward_count_++;

            InFlightForward fwd;
            fwd.recv_handle = std::move(reload_handle);
            fwd.has_recv_handle = true;
            fwd.timer.start();
            inflight_forwards_[batch_id] = std::move(fwd);
        }

        // Forward to receiver
        if (send_config_.mode == TransferMode::RPC_INLINE) {
            forward_rpc_inline(batch_id, inflight_forwards_[batch_id].recv_handle.data,
                               size, expected_checksum);
        } else {
            forward_rdma(batch_id, inflight_forwards_[batch_id].recv_handle.data, size,
                         inflight_forwards_[batch_id].recv_handle.bulk, expected_checksum);
        }
        break;
    }

    case ForwardStrategy::REUSE_AFTER_PERSIST: {
        // Write to file first
        write_to_file(data, size, offset);

        // Ack sender after persist
        if (broker_config_.ack_timing == AckTiming::ACK_ON_PERSIST) {
            ack_sender(sender_ep, batch_id);
        }
        req.respond(true);

        // Wait for forward slot
        {
            std::unique_lock<tl::mutex> lock(forward_mutex_);
            forward_cv_.wait(lock, [this] {
                return inflight_forward_count_ < send_config_.max_concurrent_batches;
            });
            inflight_forward_count_++;

            InFlightForward fwd;
            if (recv_handle) {
                fwd.recv_handle = std::move(*recv_handle);
                fwd.has_recv_handle = true;
            } else {
                fwd.has_recv_handle = false;
            }
            fwd.timer.start();
            inflight_forwards_[batch_id] = std::move(fwd);
        }

        // Forward from same buffer
        if (send_config_.mode == TransferMode::RPC_INLINE) {
            forward_rpc_inline(batch_id, data, size, expected_checksum);
            // Release buffer after inline forward (data was copied)
            if (inflight_forwards_[batch_id].has_recv_handle) {
                recv_pool_->release(inflight_forwards_[batch_id].recv_handle);
                inflight_forwards_[batch_id].has_recv_handle = false;
            }
        } else {
            // For RDMA, need to expose the buffer we have
            if (recv_handle) {
                forward_rdma(batch_id, data, size,
                             inflight_forwards_[batch_id].recv_handle.bulk, expected_checksum);
            } else {
                // RPC inline data - need to create bulk
                std::vector<std::pair<void*, size_t>> segments;
                segments.emplace_back(data, size);
                tl::bulk bulk = engine_.expose(segments, tl::bulk_mode::read_only);
                forward_rdma(batch_id, data, size, bulk, expected_checksum);
            }
        }
        break;
    }

    case ForwardStrategy::FORWARD_IMMEDIATE: {
        // Ack sender immediately
        if (broker_config_.ack_timing == AckTiming::ACK_ON_RECEIVE) {
            ack_sender(sender_ep, batch_id);
        }
        req.respond(true);

        // Start async file write
        ssize_t* write_ret = new ssize_t;
        abt_io_op_t* write_op = abt_io_pwrite_nb(abt_io_, file_fd_,
                                                  data, size, offset, write_ret);

        // Wait for forward slot
        {
            std::unique_lock<tl::mutex> lock(forward_mutex_);
            forward_cv_.wait(lock, [this] {
                return inflight_forward_count_ < send_config_.max_concurrent_batches;
            });
            inflight_forward_count_++;

            InFlightForward fwd;
            if (recv_handle) {
                fwd.recv_handle = std::move(*recv_handle);
                fwd.has_recv_handle = true;
            } else {
                fwd.has_recv_handle = false;
            }
            fwd.timer.start();
            inflight_forwards_[batch_id] = std::move(fwd);
        }

        // Forward to receiver in parallel with file write
        if (send_config_.mode == TransferMode::RPC_INLINE) {
            forward_rpc_inline(batch_id, data, size, expected_checksum);
        } else {
            if (recv_handle) {
                forward_rdma(batch_id, data, size,
                             inflight_forwards_[batch_id].recv_handle.bulk, expected_checksum);
            } else {
                std::vector<std::pair<void*, size_t>> segments;
                segments.emplace_back(data, size);
                tl::bulk bulk = engine_.expose(segments, tl::bulk_mode::read_only);
                forward_rdma(batch_id, data, size, bulk, expected_checksum);
            }
        }

        // Wait for file write to complete before we can release buffer
        abt_io_op_wait(write_op);
        bytes_written_ += size;
        abt_io_op_free(write_op);
        delete write_ret;

        // Ack sender after persist if not already done
        if (broker_config_.ack_timing == AckTiming::ACK_ON_PERSIST) {
            ack_sender(sender_ep, batch_id);
        }
        break;
    }

    case ForwardStrategy::PASSTHROUGH: {
        // Passthrough requires RDMA (sender_bulk must be valid)
        if (!sender_bulk) {
            std::cerr << "[Broker] Passthrough strategy requires RDMA transfer mode"
                      << std::endl;
            req.respond(false);
            return;
        }

        bool file_write_started = false;
        abt_io_op_t* write_op = nullptr;
        ssize_t* write_ret = nullptr;

        // Optionally write to file in parallel
        if (broker_config_.passthrough_persist) {
            // We already pulled the data for the write
            write_ret = new ssize_t;
            write_op = abt_io_pwrite_nb(abt_io_, file_fd_,
                                        data, size, offset, write_ret);
            file_write_started = true;
        }

        // Forward sender's bulk handle to receiver
        auto sender_addr = static_cast<std::string>(sender_ep);
        forward_passthrough(batch_id, size, *sender_bulk, sender_addr, expected_checksum);

        // Wait for file write if started
        if (file_write_started) {
            abt_io_op_wait(write_op);
            bytes_written_ += size;
            abt_io_op_free(write_op);
            delete write_ret;
        }

        // Release receive buffer if we have one
        if (recv_handle && recv_pool_) {
            recv_pool_->release(*recv_handle);
        }

        // Ack sender after receiver has pulled (passthrough completes synchronously
        // in our implementation since forward_passthrough is blocking RPC)
        ack_sender(sender_ep, batch_id);
        req.respond(true);
        break;
    }
    }

    // Progress output periodically
    size_t total_batches = (total_bytes_ + recv_config_.batch_size() - 1) /
                           recv_config_.batch_size();
    if (total_batches >= 10 && batches_received_ % (total_batches / 10) == 0) {
        std::cout << "[Broker] Progress: " << batches_received_.load() << "/"
                  << total_batches << " batches received" << std::endl;
    }
}

void BrokerProvider::wait_completion() {
    // Wait for sender to signal done
    {
        std::unique_lock<tl::mutex> lock(completion_mutex_);
        completion_cv_.wait(lock, [this] { return sender_done_; });
    }

    // Wait for all forwards to complete
    {
        std::unique_lock<tl::mutex> lock(forward_mutex_);
        forward_cv_.wait(lock, [this] {
            return inflight_forwards_.empty();
        });
    }

    // Signal receiver that we're done
    rpc_receiver_done_.on(receiver_ep_)();

    total_timer_.stop();

    std::cout << "[Broker] Complete: received " << bytes_received_.load()
              << " bytes, wrote " << bytes_written_.load()
              << " bytes, forwarded " << bytes_forwarded_.load()
              << " bytes in " << total_timer_.elapsed_seconds() << " seconds"
              << std::endl;
}

} // namespace mochi_pipeline
