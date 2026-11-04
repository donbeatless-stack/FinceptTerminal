// gateway/include/fmdg/latency_histogram.h
#pragma once

#include <array>
#include <atomic>
#include <cstdint>

namespace fmdg {

/// Log-linear latency histogram (HdrHistogram's bucketing, simplified). Values
/// below 32 get a bucket each; above that every power of two is split into 32
/// sub-buckets, so any value is reported within 1/32 (about 3 %) of its true size,
/// from nanoseconds to centuries, in a fixed array of 1920 counters.
///
/// record() is a few relaxed atomic operations and safe from any number of
/// threads at once; the readers are exact once writers stop and approximate while
/// they run.
class LatencyHistogram {
  public:
    static constexpr int kSubBits = 5;
    static constexpr int kSub = 1 << kSubBits;                     // 32 sub-buckets
    static constexpr int kBuckets = kSub + (64 - kSubBits) * kSub; // 1920

    /// Negative values (clock skew between processes) count as 0.
    void record(std::int64_t value);

    std::uint64_t count() const;
    std::int64_t max() const;
    double mean() const;

    /// Upper edge of the bucket holding the q-quantile, q in [0, 1], capped at
    /// max(): never under-reports. 0 when empty.
    std::int64_t percentile(double q) const;

    /// Adds `other`'s counts to this one (merging per-thread histograms).
    void merge(const LatencyHistogram& other);
    void reset();

    static int bucket_of(std::uint64_t value);
    static std::uint64_t bucket_upper(int index);

  private:
    std::array<std::atomic<std::uint64_t>, kBuckets> counts_{};
    std::atomic<std::uint64_t> total_{0};
    std::atomic<std::uint64_t> sum_{0};
    std::atomic<std::int64_t> max_{0};
};

} // namespace fmdg
