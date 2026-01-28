#ifndef MOCHI_PIPELINE_BROKER_PROVIDER_HPP
#define MOCHI_PIPELINE_BROKER_PROVIDER_HPP

#include <thallium.hpp>
#include <thallium/serialization/stl/vector.hpp>
#include <thallium/serialization/stl/string.hpp>
#include <abt-io.h>
#include <atomic>
#include <memory>

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
 * Acknowledgement flow:
 * - Sender->Broker: broker's req.respond() acks the sender
 * - Broker->Receiver: receiver's req.respond() acks the broker
 * No separate ack RPCs needed.
 */
class BrokerProvider : public tl::provider<BrokerProvider> {
public:
    BrokerProvider(tl::engine& engine, uint16_t provider_id,
                   const BrokerConfig& broker_config,
                   const TransferConfig& recv_config,
                   const TransferConfig& send_config,
                   const std::string& receiver_address,
                   size_t total_bytes,
                   bool verify_checksums);

    ~BrokerProvider();

    void wait_completion();

    size_t get_bytes_received() const { return bytes_received_.load(); }
    size_t get_bytes_written() const { return bytes_written_.load(); }
    size_t get_bytes_forwarded() const { return bytes_forwarded_.load(); }

private:
    // RPC handlers from sender
    void on_batch_rpc(const tl::request& req, uint64_t batch_id,
                      const std::vector<char>& data, uint32_t checksum);
    void on_batch_rdma(const tl::request& req, uint64_t batch_id,
                       size_t size, tl::bulk& remote_bulk, uint32_t checksum);
    void on_sender_done(const tl::request& req);

    // Core processing after receiving batch data
    void process_batch(const tl::request& req, uint64_t batch_id,
                       void* data, size_t size, uint32_t expected_checksum,
                       tl::endpoint sender_ep, tl::bulk* sender_bulk,
                       BufferPool::Handle* recv_handle);

    // File I/O
    void write_to_file(const void* data, size_t size, off_t offset);
    void read_from_file(void* data, size_t size, off_t offset);

    // Forward to receiver (blocking RPCs - response is ack)
    bool forward_rpc_inline(uint64_t batch_id, const void* data, size_t size,
                            uint32_t checksum);
    bool forward_rdma(uint64_t batch_id, void* data, size_t size,
                      tl::bulk& local_bulk, uint32_t checksum);
    bool forward_passthrough(uint64_t batch_id, size_t size,
                             tl::bulk& sender_bulk, const std::string& sender_addr,
                             uint32_t checksum);

    // Configuration
    BrokerConfig broker_config_;
    TransferConfig recv_config_;
    TransferConfig send_config_;
    size_t total_bytes_;
    bool verify_checksums_;

    // Engine and endpoints
    tl::engine& engine_;
    tl::provider_handle receiver_ph_;

    // ABT-IO
    abt_io_instance_id abt_io_;
    int file_fd_;
    std::atomic<off_t> file_offset_{0};

    // Buffer pools
    std::unique_ptr<BufferPool> recv_pool_;
    std::unique_ptr<BufferPool> reload_pool_;

    // Completion tracking
    tl::mutex completion_mutex_;
    tl::condition_variable completion_cv_;
    std::atomic<size_t> bytes_received_{0};
    std::atomic<size_t> bytes_written_{0};
    std::atomic<size_t> bytes_forwarded_{0};
    std::atomic<size_t> batches_received_{0};
    std::atomic<size_t> batches_forwarded_{0};
    bool sender_done_ = false;

    Timer total_timer_;

    // RPCs to receiver
    tl::remote_procedure rpc_forward_rpc_;
    tl::remote_procedure rpc_forward_rdma_;
    tl::remote_procedure rpc_forward_passthrough_;
    tl::remote_procedure rpc_receiver_done_;
};

} // namespace mochi_pipeline

#endif // MOCHI_PIPELINE_BROKER_PROVIDER_HPP
