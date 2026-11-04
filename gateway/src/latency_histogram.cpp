// gateway/src/latency_histogram.cpp
#include "fmdg/latency_histogram.h"

#include <algorithm>
#include <bit>
#include <cmath>

namespace fmdg {

int LatencyHistogram::bucket_of(std::uint64_t value) {
    if (value < static_cast<std::uint64_t>(kSub))
        return static_cast<int>(value);
    // value lies in [2^k, 2^(k+1)) with k >= kSubBits; keep its top kSubBits + 1
    // bits: (value >> shift) is in [32, 64), its offset from 32 is the sub-bucket.
    const int k = static_cast<int>(std::bit_width(value)) - 1;
    const int shift = k - kSubBits;
    const auto sub = static_cast<int>(value >> shift) - kSub;
    return kSub + shift * kSub + sub;
}

std::uint64_t LatencyHistogram::bucket_upper(int index) {
    if (index < kSub)
        return static_cast<std::uint64_t>(index);
    const int j = index - kSub;
    const int shift = j / kSub;
    const auto sub = static_cast<std::uint64_t>(j % kSub);
    const std::uint64_t lower = (static_cast<std::uint64_t>(kSub) + sub) << shift;
    return lower + ((std::uint64_t{1} << shift) - 1);
}

void LatencyHistogram::record(std::int64_t value) {
    const auto v = static_cast<std::uint64_t>(value < 0 ? 0 : value);
    counts_[static_cast<std::size_t>(bucket_of(v))].fetch_add(1, std::memory_order_relaxed);
    total_.fetch_add(1, std::memory_order_relaxed);
    sum_.fetch_add(v, std::memory_order_relaxed);
    const auto signed_v = static_cast<std::int64_t>(v);
    std::int64_t seen = max_.load(std::memory_order_relaxed);
    while (signed_v > seen && !max_.compare_exchange_weak(seen, signed_v, std::memory_order_relaxed)) {
    }
}

std::uint64_t LatencyHistogram::count() const {
    return total_.load(std::memory_order_relaxed);
}

std::int64_t LatencyHistogram::max() const {
    return max_.load(std::memory_order_relaxed);
}

double LatencyHistogram::mean() const {
    const std::uint64_t n = count();
    return n == 0 ? 0.0 : static_cast<double>(sum_.load(std::memory_order_relaxed)) / static_cast<double>(n);
}

std::int64_t LatencyHistogram::percentile(double q) const {
    const std::uint64_t n = count();
    if (n == 0)
        return 0;
    q = std::clamp(q, 0.0, 1.0);
    // The rank of the q-quantile, 1-based: q = 0.5 of 10 samples is the 5th.
    const auto rank = std::max<std::uint64_t>(1, static_cast<std::uint64_t>(std::ceil(q * static_cast<double>(n))));
    std::uint64_t seen = 0;
    for (int i = 0; i < kBuckets; ++i) {
        seen += counts_[static_cast<std::size_t>(i)].load(std::memory_order_relaxed);
        if (seen >= rank)
            return std::min(static_cast<std::int64_t>(bucket_upper(i)), max());
    }
    return max(); // writers raced the scan
}

void LatencyHistogram::merge(const LatencyHistogram& other) {
    for (std::size_t i = 0; i < counts_.size(); ++i) {
        const std::uint64_t c = other.counts_[i].load(std::memory_order_relaxed);
        if (c != 0)
            counts_[i].fetch_add(c, std::memory_order_relaxed);
    }
    total_.fetch_add(other.total_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    sum_.fetch_add(other.sum_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    const std::int64_t m = other.max();
    std::int64_t seen = max_.load(std::memory_order_relaxed);
    while (m > seen && !max_.compare_exchange_weak(seen, m, std::memory_order_relaxed)) {
    }
}

void LatencyHistogram::reset() {
    for (auto& c : counts_)
        c.store(0, std::memory_order_relaxed);
    total_.store(0, std::memory_order_relaxed);
    sum_.store(0, std::memory_order_relaxed);
    max_.store(0, std::memory_order_relaxed);
}

} // namespace fmdg
