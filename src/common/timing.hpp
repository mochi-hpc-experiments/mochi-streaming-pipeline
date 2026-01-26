#ifndef MOCHI_PIPELINE_TIMING_HPP
#define MOCHI_PIPELINE_TIMING_HPP

#include <chrono>
#include <cstdint>
#include <string>
#include <sstream>
#include <iomanip>

namespace mochi_pipeline {

/**
 * @brief High-resolution timing utilities for benchmarking.
 */
class Timer {
public:
    using Clock = std::chrono::high_resolution_clock;
    using TimePoint = Clock::time_point;
    using Duration = std::chrono::duration<double>;

    Timer() : start_time_(Clock::now()), running_(true) {}

    /**
     * @brief Reset and start the timer.
     */
    void start() {
        start_time_ = Clock::now();
        running_ = true;
    }

    /**
     * @brief Stop the timer.
     */
    void stop() {
        if (running_) {
            end_time_ = Clock::now();
            running_ = false;
        }
    }

    /**
     * @brief Get elapsed time in seconds.
     */
    double elapsed_seconds() const {
        TimePoint end = running_ ? Clock::now() : end_time_;
        return std::chrono::duration<double>(end - start_time_).count();
    }

    /**
     * @brief Get elapsed time in milliseconds.
     */
    double elapsed_ms() const {
        return elapsed_seconds() * 1000.0;
    }

    /**
     * @brief Get elapsed time in microseconds.
     */
    double elapsed_us() const {
        return elapsed_seconds() * 1000000.0;
    }

    /**
     * @brief Check if timer is currently running.
     */
    bool is_running() const { return running_; }

    /**
     * @brief Get start time point.
     */
    TimePoint start_time() const { return start_time_; }

    /**
     * @brief Get end time point (only valid if stopped).
     */
    TimePoint end_time() const { return end_time_; }

private:
    TimePoint start_time_;
    TimePoint end_time_;
    bool running_;
};

/**
 * @brief Statistics accumulator for latency measurements.
 */
class LatencyStats {
public:
    LatencyStats() : count_(0), sum_(0), sum_sq_(0), min_(0), max_(0) {}

    /**
     * @brief Record a latency sample in seconds.
     */
    void record(double seconds) {
        if (count_ == 0) {
            min_ = max_ = seconds;
        } else {
            if (seconds < min_) min_ = seconds;
            if (seconds > max_) max_ = seconds;
        }
        sum_ += seconds;
        sum_sq_ += seconds * seconds;
        count_++;
    }

    /**
     * @brief Get number of samples.
     */
    uint64_t count() const { return count_; }

    /**
     * @brief Get minimum latency in seconds.
     */
    double min_seconds() const { return min_; }

    /**
     * @brief Get maximum latency in seconds.
     */
    double max_seconds() const { return max_; }

    /**
     * @brief Get average latency in seconds.
     */
    double avg_seconds() const {
        return count_ > 0 ? sum_ / count_ : 0.0;
    }

    /**
     * @brief Get standard deviation in seconds.
     */
    double stddev_seconds() const {
        if (count_ < 2) return 0.0;
        double mean = avg_seconds();
        return std::sqrt(sum_sq_ / count_ - mean * mean);
    }

    /**
     * @brief Get min latency in milliseconds.
     */
    double min_ms() const { return min_ * 1000.0; }

    /**
     * @brief Get max latency in milliseconds.
     */
    double max_ms() const { return max_ * 1000.0; }

    /**
     * @brief Get average latency in milliseconds.
     */
    double avg_ms() const { return avg_seconds() * 1000.0; }

    /**
     * @brief Get standard deviation in milliseconds.
     */
    double stddev_ms() const { return stddev_seconds() * 1000.0; }

    /**
     * @brief Reset all statistics.
     */
    void reset() {
        count_ = 0;
        sum_ = 0;
        sum_sq_ = 0;
        min_ = 0;
        max_ = 0;
    }

private:
    uint64_t count_;
    double sum_;
    double sum_sq_;
    double min_;
    double max_;
};

/**
 * @brief Format bytes as human-readable string (KB, MB, GB).
 */
inline std::string format_bytes(size_t bytes) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);

    if (bytes >= 1024ULL * 1024 * 1024 * 1024) {
        oss << (static_cast<double>(bytes) / (1024ULL * 1024 * 1024 * 1024)) << " TB";
    } else if (bytes >= 1024ULL * 1024 * 1024) {
        oss << (static_cast<double>(bytes) / (1024ULL * 1024 * 1024)) << " GB";
    } else if (bytes >= 1024ULL * 1024) {
        oss << (static_cast<double>(bytes) / (1024ULL * 1024)) << " MB";
    } else if (bytes >= 1024) {
        oss << (static_cast<double>(bytes) / 1024) << " KB";
    } else {
        oss << bytes << " B";
    }

    return oss.str();
}

/**
 * @brief Format throughput as GB/s.
 */
inline std::string format_throughput(size_t bytes, double seconds) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(2);

    if (seconds > 0) {
        double gbps = static_cast<double>(bytes) / seconds / (1024.0 * 1024 * 1024);
        oss << gbps << " GB/s";
    } else {
        oss << "N/A";
    }

    return oss.str();
}

/**
 * @brief Format duration as human-readable string.
 */
inline std::string format_duration(double seconds) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(3);

    if (seconds >= 60.0) {
        int mins = static_cast<int>(seconds / 60.0);
        double secs = seconds - mins * 60.0;
        oss << mins << " min " << secs << " s";
    } else if (seconds >= 1.0) {
        oss << seconds << " s";
    } else if (seconds >= 0.001) {
        oss << (seconds * 1000.0) << " ms";
    } else {
        oss << (seconds * 1000000.0) << " us";
    }

    return oss.str();
}

} // namespace mochi_pipeline

#endif // MOCHI_PIPELINE_TIMING_HPP
