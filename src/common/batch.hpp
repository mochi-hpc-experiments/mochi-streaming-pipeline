#ifndef MOCHI_PIPELINE_BATCH_HPP
#define MOCHI_PIPELINE_BATCH_HPP

#include <cstdint>
#include <cstddef>

namespace mochi_pipeline {

/**
 * @brief Batch metadata for tracking batch transfers.
 */
struct BatchInfo {
    uint64_t batch_id;        // Unique batch identifier
    size_t size;              // Size of batch data in bytes
    uint32_t checksum;        // CRC32 checksum (0 if not computed)
    off_t file_offset;        // Offset in output file (for broker)

    BatchInfo()
        : batch_id(0), size(0), checksum(0), file_offset(0) {}

    BatchInfo(uint64_t id, size_t sz, uint32_t csum = 0, off_t offset = 0)
        : batch_id(id), size(sz), checksum(csum), file_offset(offset) {}
};

/**
 * @brief Compute CRC32 checksum of data.
 *
 * Uses the standard CRC32 polynomial (0xEDB88320).
 *
 * @param data Pointer to data
 * @param size Size of data in bytes
 * @return CRC32 checksum
 */
inline uint32_t compute_crc32(const void* data, size_t size) {
    const uint8_t* bytes = static_cast<const uint8_t*>(data);
    uint32_t crc = 0xFFFFFFFF;

    for (size_t i = 0; i < size; i++) {
        crc ^= bytes[i];
        for (int j = 0; j < 8; j++) {
            crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
        }
    }

    return ~crc;
}

/**
 * @brief Fill buffer with deterministic test pattern based on batch_id.
 *
 * The pattern allows verification that data was transferred correctly.
 *
 * @param buffer Buffer to fill
 * @param size Size of buffer in bytes
 * @param batch_id Batch ID used as seed for pattern
 */
inline void fill_test_pattern(void* buffer, size_t size, uint64_t batch_id) {
    uint8_t* bytes = static_cast<uint8_t*>(buffer);

    // Use batch_id as seed for deterministic pattern
    uint64_t seed = batch_id;
    for (size_t i = 0; i < size; i++) {
        // Simple linear congruential generator
        seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
        bytes[i] = static_cast<uint8_t>(seed >> 56);
    }
}

/**
 * @brief Verify that buffer contains expected test pattern.
 *
 * @param buffer Buffer to verify
 * @param size Size of buffer in bytes
 * @param batch_id Expected batch ID seed
 * @return true if pattern matches, false otherwise
 */
inline bool verify_test_pattern(const void* buffer, size_t size, uint64_t batch_id) {
    const uint8_t* bytes = static_cast<const uint8_t*>(buffer);

    uint64_t seed = batch_id;
    for (size_t i = 0; i < size; i++) {
        seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
        uint8_t expected = static_cast<uint8_t>(seed >> 56);
        if (bytes[i] != expected) {
            return false;
        }
    }
    return true;
}

} // namespace mochi_pipeline

#endif // MOCHI_PIPELINE_BATCH_HPP
