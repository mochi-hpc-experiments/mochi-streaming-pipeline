#ifndef MOCHI_PIPELINE_BUFFER_POOL_HPP
#define MOCHI_PIPELINE_BUFFER_POOL_HPP

#include <thallium.hpp>
#include <thallium/bulk_mode.hpp>
#include <vector>
#include <queue>
#include <memory>
#include <mutex>

namespace tl = thallium;

namespace mochi_pipeline {

/**
 * @brief BufferPool manages a pool of reusable buffers for RDMA operations.
 *
 * Buffers can be pre-registered with Thallium for RDMA, avoiding the overhead
 * of registration on each transfer. The pool uses Argobots-compatible
 * synchronization primitives.
 */
class BufferPool {
public:
    /**
     * @brief Handle to an acquired buffer from the pool.
     */
    struct Handle {
        void* data;           // Pointer to buffer data
        size_t size;          // Size of the buffer
        tl::bulk bulk;        // Pre-registered bulk handle (may be empty if not registered)
        size_t index;         // Index in pool for release

        Handle() : data(nullptr), size(0), index(0) {}
        Handle(void* d, size_t s, tl::bulk b, size_t i)
            : data(d), size(s), bulk(std::move(b)), index(i) {}

        // Move only
        Handle(const Handle&) = delete;
        Handle& operator=(const Handle&) = delete;
        Handle(Handle&& other) noexcept
            : data(other.data), size(other.size),
              bulk(std::move(other.bulk)), index(other.index) {
            other.data = nullptr;
            other.size = 0;
        }
        Handle& operator=(Handle&& other) noexcept {
            if (this != &other) {
                data = other.data;
                size = other.size;
                bulk = std::move(other.bulk);
                index = other.index;
                other.data = nullptr;
                other.size = 0;
            }
            return *this;
        }

        bool is_valid() const { return data != nullptr; }
    };

    /**
     * @brief Construct a new buffer pool.
     *
     * @param engine Thallium engine for bulk registration
     * @param buffer_size Size of each buffer in bytes
     * @param pool_size Number of buffers in the pool
     * @param mode Bulk mode for pre-registration (read_only, write_only, read_write)
     * @param register_bulk If true, pre-register buffers with Thallium for RDMA
     */
    BufferPool(tl::engine& engine, size_t buffer_size, size_t pool_size,
               tl::bulk_mode mode, bool register_bulk = true);

    /**
     * @brief Destructor - frees all buffers.
     */
    ~BufferPool();

    // Non-copyable
    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;

    // Movable
    BufferPool(BufferPool&&) = default;
    BufferPool& operator=(BufferPool&&) = default;

    /**
     * @brief Acquire a buffer from the pool.
     *
     * Blocks using Argobots condition variable if no buffers are available.
     *
     * @return Handle to the acquired buffer
     */
    Handle acquire();

    /**
     * @brief Try to acquire a buffer without blocking.
     *
     * @param out Handle to store the acquired buffer
     * @return true if a buffer was acquired, false if pool is empty
     */
    bool try_acquire(Handle& out);

    /**
     * @brief Release a buffer back to the pool.
     *
     * @param handle Handle to the buffer to release
     */
    void release(Handle& handle);

    /**
     * @brief Get the size of each buffer.
     */
    size_t buffer_size() const { return buffer_size_; }

    /**
     * @brief Get the total number of buffers in the pool.
     */
    size_t pool_size() const { return pool_size_; }

    /**
     * @brief Get the number of currently available buffers.
     */
    size_t available() const;

    /**
     * @brief Get the bulk mode used for registration.
     */
    tl::bulk_mode mode() const { return mode_; }

private:
    tl::engine& engine_;
    size_t buffer_size_;
    size_t pool_size_;
    tl::bulk_mode mode_;
    bool register_bulk_;

    std::vector<void*> buffers_;           // Raw buffer pointers (owned)
    std::vector<tl::bulk> bulks_;          // Pre-registered bulk handles
    std::queue<size_t> available_indices_; // Indices of available buffers

    mutable tl::mutex mutex_;
    tl::condition_variable cv_;
};

} // namespace mochi_pipeline

#endif // MOCHI_PIPELINE_BUFFER_POOL_HPP
