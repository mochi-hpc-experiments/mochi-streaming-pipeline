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
    // Look up receiver endpoint and create provider handle (receiver uses provider_id 1)
    tl::endpoint receiver_ep = engine_.lookup(receiver_address);
    receiver_ph_ = tl::provider_handle(receiver_ep, 1);

    // Initialize ABT-IO with JSON configuration
    std::string json_config = "{\"backing_thread_count\": " +
                               std::to_string(broker_config_.abt_io.concurrent_writes);

    // Add liburing support if num_urings > 0
    if (broker_config_.abt_io.num_urings > 0) {
        json_config += ", \"num_urings\": " +
                       std::to_string(broker_config_.abt_io.num_urings);

        // Add liburing flags if specified
        if (!broker_config_.abt_io.liburing_flags.empty()) {
            json_config += ", \"liburing_flags\": [";
            for (size_t i = 0; i < broker_config_.abt_io.liburing_flags.size(); ++i) {
                if (i > 0) json_config += ", ";
                json_config += "\"" + broker_config_.abt_io.liburing_flags[i] + "\"";
            }
            json_config += "]";
        }
    }

    json_config += "}";

    struct abt_io_init_info init_info;
    memset(&init_info, 0, sizeof(init_info));
    init_info.json_config = json_config.c_str();

    abt_io_ = abt_io_init_ext(&init_info);
    if (abt_io_ == ABT_IO_INSTANCE_NULL) {
        throw std::runtime_error("Failed to initialize ABT-IO with config: " + json_config);
    }

    // Open output file
    // Use O_RDWR if we need to read back (reload_from_file strategy)
    int open_flags = O_CREAT | O_TRUNC;
    if (broker_config_.forward_strategy == ForwardStrategy::RELOAD_FROM_FILE) {
        open_flags |= O_RDWR;
    } else {
        open_flags |= O_WRONLY;
    }
    file_fd_ = abt_io_open(abt_io_, broker_config_.output_file.c_str(),
                           open_flags, 0644);
    if (file_fd_ < 0) {
        abt_io_finalize(abt_io_);
        throw std::runtime_error("Failed to open output file: " +
                                 broker_config_.output_file);
    }

    // Create buffer pools
    size_t recv_batch_size = recv_config_.batch_size();

    tl::bulk_mode recv_mode = tl::bulk_mode::write_only;
    if (broker_config_.forward_strategy == ForwardStrategy::REUSE_AFTER_PERSIST ||
        broker_config_.forward_strategy == ForwardStrategy::FORWARD_IMMEDIATE) {
        recv_mode = tl::bulk_mode::read_write;
    }

    if (recv_config_.mode != TransferMode::RPC_INLINE) {
        recv_pool_ = std::make_unique<BufferPool>(
            engine_, recv_batch_size,
            recv_config_.max_concurrent_batches + 2,
            recv_mode,
            true
        );
    }

    if (broker_config_.forward_strategy == ForwardStrategy::RELOAD_FROM_FILE) {
        reload_pool_ = std::make_unique<BufferPool>(
            engine_, recv_batch_size,
            send_config_.max_concurrent_batches + 2,
            tl::bulk_mode::read_only,
            true
        );
    }

    // Define RPC handlers
    define("broker_batch_rpc", &BrokerProvider::on_batch_rpc);
    define("broker_batch_rdma", &BrokerProvider::on_batch_rdma);
    define("broker_sender_done", &BrokerProvider::on_sender_done);

    // Get remote procedures to receiver
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

    BufferPool::Handle recv_handle = recv_pool_->acquire();

    std::vector<std::pair<void*, size_t>> segments;
    segments.emplace_back(recv_handle.data, size);
    tl::bulk local_bulk = engine_.expose(segments, tl::bulk_mode::write_only);

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

bool BrokerProvider::forward_rpc_inline(uint64_t batch_id, const void* data,
                                         size_t size, uint32_t checksum) {
    std::vector<char> data_vec(static_cast<const char*>(data),
                                static_cast<const char*>(data) + size);

    bool success = rpc_forward_rpc_.on(receiver_ph_)(batch_id, data_vec, checksum);
    if (success) {
        bytes_forwarded_ += size;
        batches_forwarded_++;
    }
    return success;
}

bool BrokerProvider::forward_rdma(uint64_t batch_id, void* data, size_t size,
                                   tl::bulk& local_bulk, uint32_t checksum) {
    bool success = rpc_forward_rdma_.on(receiver_ph_)(batch_id, size, local_bulk, checksum);
    if (success) {
        bytes_forwarded_ += size;
        batches_forwarded_++;
    }
    return success;
}

bool BrokerProvider::forward_passthrough(uint64_t batch_id, size_t size,
                                          tl::bulk& sender_bulk,
                                          const std::string& sender_addr,
                                          uint32_t checksum) {
    bool success = rpc_forward_passthrough_.on(receiver_ph_)(batch_id, size, sender_bulk,
                                                              sender_addr, checksum);
    if (success) {
        bytes_forwarded_ += size;
        batches_forwarded_++;
    }
    return success;
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
            if (recv_handle && recv_pool_) {
                recv_pool_->release(*recv_handle);
            }
            req.respond(false);
            return;
        }
    }

    bytes_received_ += size;
    batches_received_++;

    off_t offset = file_offset_.fetch_add(size);

    switch (broker_config_.forward_strategy) {

    case ForwardStrategy::RELOAD_FROM_FILE: {
        // Write to file first
        write_to_file(data, size, offset);

        // Release receive buffer (no longer needed)
        if (recv_handle && recv_pool_) {
            recv_pool_->release(*recv_handle);
            recv_handle = nullptr;
        }

        // Reload from file into fresh buffer
        auto reload_handle = reload_pool_->acquire();
        read_from_file(reload_handle.data, size, offset);

        // Forward to receiver (blocking - response is ack)
        bool forward_ok;
        if (send_config_.mode == TransferMode::RPC_INLINE) {
            forward_ok = forward_rpc_inline(batch_id, reload_handle.data, size, expected_checksum);
        } else {
            forward_ok = forward_rdma(batch_id, reload_handle.data, size,
                                      reload_handle.bulk, expected_checksum);
        }

        reload_pool_->release(reload_handle);

        // Respond to sender (this is the sender's ack)
        req.respond(forward_ok);
        break;
    }

    case ForwardStrategy::REUSE_AFTER_PERSIST: {
        // Write to file first
        write_to_file(data, size, offset);

        // Forward from same buffer (blocking - response is ack)
        bool forward_ok;
        if (send_config_.mode == TransferMode::RPC_INLINE) {
            forward_ok = forward_rpc_inline(batch_id, data, size, expected_checksum);
        } else if (recv_handle) {
            forward_ok = forward_rdma(batch_id, data, size,
                                      recv_handle->bulk, expected_checksum);
        } else {
            // RPC inline data - need to create bulk for forwarding
            std::vector<std::pair<void*, size_t>> segments;
            segments.emplace_back(data, size);
            tl::bulk bulk = engine_.expose(segments, tl::bulk_mode::read_only);
            forward_ok = forward_rdma(batch_id, data, size, bulk, expected_checksum);
        }

        // Release receive buffer after forward completes
        if (recv_handle && recv_pool_) {
            recv_pool_->release(*recv_handle);
        }

        // Respond to sender
        req.respond(forward_ok);
        break;
    }

    case ForwardStrategy::FORWARD_IMMEDIATE: {
        // Start async file write
        ssize_t write_ret;
        abt_io_op_t* write_op = abt_io_pwrite_nb(abt_io_, file_fd_,
                                                  data, size, offset, &write_ret);

        // Forward to receiver in parallel (blocking RPC)
        bool forward_ok;
        if (send_config_.mode == TransferMode::RPC_INLINE) {
            forward_ok = forward_rpc_inline(batch_id, data, size, expected_checksum);
        } else if (recv_handle) {
            forward_ok = forward_rdma(batch_id, data, size,
                                      recv_handle->bulk, expected_checksum);
        } else {
            std::vector<std::pair<void*, size_t>> segments;
            segments.emplace_back(data, size);
            tl::bulk bulk = engine_.expose(segments, tl::bulk_mode::read_only);
            forward_ok = forward_rdma(batch_id, data, size, bulk, expected_checksum);
        }

        // Wait for file write to complete
        abt_io_op_wait(write_op);
        if (write_ret == static_cast<ssize_t>(size)) {
            bytes_written_ += size;
        }
        abt_io_op_free(write_op);

        // Release receive buffer
        if (recv_handle && recv_pool_) {
            recv_pool_->release(*recv_handle);
        }

        // Respond to sender based on ack_timing
        // For FORWARD_IMMEDIATE, we've already forwarded, so respond based on config
        req.respond(forward_ok);
        break;
    }

    case ForwardStrategy::PASSTHROUGH: {
        if (!sender_bulk) {
            std::cerr << "[Broker] Passthrough strategy requires RDMA transfer mode"
                      << std::endl;
            if (recv_handle && recv_pool_) {
                recv_pool_->release(*recv_handle);
            }
            req.respond(false);
            return;
        }

        bool file_write_started = false;
        abt_io_op_t* write_op = nullptr;
        ssize_t write_ret;

        // Optionally write to file in parallel
        if (broker_config_.passthrough_persist) {
            write_op = abt_io_pwrite_nb(abt_io_, file_fd_,
                                        data, size, offset, &write_ret);
            file_write_started = true;
        }

        // Forward sender's bulk handle to receiver (blocking - receiver pulls from sender)
        auto sender_addr = static_cast<std::string>(sender_ep);
        bool forward_ok = forward_passthrough(batch_id, size, *sender_bulk,
                                               sender_addr, expected_checksum);

        // Wait for file write if started
        if (file_write_started) {
            abt_io_op_wait(write_op);
            if (write_ret == static_cast<ssize_t>(size)) {
                bytes_written_ += size;
            }
            abt_io_op_free(write_op);
        }

        // Release receive buffer
        if (recv_handle && recv_pool_) {
            recv_pool_->release(*recv_handle);
        }

        // Respond to sender (receiver has completed its pull)
        req.respond(forward_ok);
        break;
    }
    }

    // Progress output
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

    // Signal receiver that we're done
    rpc_receiver_done_.on(receiver_ph_)();

    total_timer_.stop();

    std::cout << "[Broker] Complete: received " << bytes_received_.load()
              << " bytes, wrote " << bytes_written_.load()
              << " bytes, forwarded " << bytes_forwarded_.load()
              << " bytes in " << total_timer_.elapsed_seconds() << " seconds"
              << std::endl;
}

} // namespace mochi_pipeline
