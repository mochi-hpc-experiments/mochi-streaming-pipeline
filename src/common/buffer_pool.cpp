#include "common/buffer_pool.hpp"
#include <cstdlib>
#include <stdexcept>
#include <cstring>

namespace mochi_pipeline {

BufferPool::BufferPool(tl::engine& engine, size_t buffer_size, size_t pool_size,
                       tl::bulk_mode mode, bool register_bulk)
    : engine_(engine)
    , buffer_size_(buffer_size)
    , pool_size_(pool_size)
    , mode_(mode)
    , register_bulk_(register_bulk)
{
    if (buffer_size == 0) {
        throw std::invalid_argument("BufferPool: buffer_size must be > 0");
    }
    if (pool_size == 0) {
        throw std::invalid_argument("BufferPool: pool_size must be > 0");
    }

    buffers_.reserve(pool_size);
    if (register_bulk_) {
        bulks_.reserve(pool_size);
    }

    // Allocate and optionally register all buffers
    for (size_t i = 0; i < pool_size; i++) {
        // Allocate aligned memory for better performance
        void* buf = nullptr;
        int ret = posix_memalign(&buf, 4096, buffer_size);
        if (ret != 0 || buf == nullptr) {
            // Clean up already allocated buffers
            for (auto* ptr : buffers_) {
                free(ptr);
            }
            throw std::runtime_error("BufferPool: failed to allocate buffer");
        }

        // Zero initialize
        std::memset(buf, 0, buffer_size);
        buffers_.push_back(buf);

        // Register with Thallium for RDMA if requested
        if (register_bulk_) {
            std::vector<std::pair<void*, size_t>> segments;
            segments.emplace_back(buf, buffer_size);
            tl::bulk bulk = engine_.expose(segments, mode_);
            bulks_.push_back(std::move(bulk));
        }

        // Mark as available
        available_indices_.push(i);
    }
}

BufferPool::~BufferPool() {
    // Bulk handles will be freed automatically by their destructors
    bulks_.clear();

    // Free all raw buffers
    for (auto* buf : buffers_) {
        if (buf != nullptr) {
            free(buf);
        }
    }
    buffers_.clear();
}

BufferPool::Handle BufferPool::acquire() {
    std::unique_lock<tl::mutex> lock(mutex_);

    // Wait until a buffer is available
    cv_.wait(lock, [this] { return !available_indices_.empty(); });

    size_t index = available_indices_.front();
    available_indices_.pop();

    // Return handle with bulk reference if registered
    if (register_bulk_) {
        return Handle(buffers_[index], buffer_size_, bulks_[index], index);
    } else {
        return Handle(buffers_[index], buffer_size_, tl::bulk(), index);
    }
}

bool BufferPool::try_acquire(Handle& out) {
    std::unique_lock<tl::mutex> lock(mutex_);

    if (available_indices_.empty()) {
        return false;
    }

    size_t index = available_indices_.front();
    available_indices_.pop();

    if (register_bulk_) {
        out = Handle(buffers_[index], buffer_size_, bulks_[index], index);
    } else {
        out = Handle(buffers_[index], buffer_size_, tl::bulk(), index);
    }

    return true;
}

void BufferPool::release(Handle& handle) {
    if (!handle.is_valid()) {
        return;
    }

    {
        std::lock_guard<tl::mutex> lock(mutex_);
        available_indices_.push(handle.index);
    }

    // Signal one waiting thread that a buffer is available
    cv_.notify_one();

    // Invalidate the handle
    handle.data = nullptr;
    handle.size = 0;
}

size_t BufferPool::available() const {
    std::lock_guard<tl::mutex> lock(mutex_);
    return available_indices_.size();
}

} // namespace mochi_pipeline
