// gateway/tests/test_latency_histogram.cpp
#include "fmdg/latency_histogram.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <random>
#include <thread>
#include <vector>

using fmdg::LatencyHistogram;

TEST(LatencyHistogram, SmallValuesAreExact) {
    for (std::uint64_t v = 0; v < 32; ++v) {
        EXPECT_EQ(LatencyHistogram::bucket_of(v), static_cast<int>(v));
        EXPECT_EQ(LatencyHistogram::bucket_upper(static_cast<int>(v)), v);
    }
}

// Every value must land in a bucket whose upper edge is >= the value and within
// 1/32 of it — that is the whole accuracy contract.
TEST(LatencyHistogram, BucketsContainTheirValuesWithinOneThirtySecond) {
    std::mt19937_64 rng(7);
    std::vector<std::uint64_t> values = {32, 33, 63, 64, 65, 127, 128, 1000, 1'000'000, 999'999'999};
    for (int shift = 5; shift < 63; ++shift) {
        values.push_back(std::uint64_t{1} << shift);
        values.push_back((std::uint64_t{1} << shift) - 1);
        values.push_back((std::uint64_t{1} << shift) + 1);
    }
    for (int i = 0; i < 100'000; ++i)
        values.push_back(rng() >> (rng() % 63));
    values.push_back(static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max()));

    for (const std::uint64_t v : values) {
        const int b = LatencyHistogram::bucket_of(v);
        ASSERT_GE(b, 0);
        ASSERT_LT(b, LatencyHistogram::kBuckets);
        const std::uint64_t upper = LatencyHistogram::bucket_upper(b);
        ASSERT_GE(upper, v) << v;
        ASSERT_LE(static_cast<double>(upper - v), static_cast<double>(v) / 32.0) << v;
        if (b > 0)
            ASSERT_LT(LatencyHistogram::bucket_upper(b - 1), v) << "buckets must not overlap: " << v;
    }
}

TEST(LatencyHistogram, BucketsAreContiguousAndIncreasing) {
    for (int b = 1; b < LatencyHistogram::kBuckets; ++b) {
        const std::uint64_t prev_upper = LatencyHistogram::bucket_upper(b - 1);
        ASSERT_GT(LatencyHistogram::bucket_upper(b), prev_upper);
        ASSERT_EQ(LatencyHistogram::bucket_of(prev_upper + 1), b) << "gap after bucket " << (b - 1);
    }
}

TEST(LatencyHistogram, EmptyHistogram) {
    LatencyHistogram h;
    EXPECT_EQ(h.count(), 0u);
    EXPECT_EQ(h.percentile(0.5), 0);
    EXPECT_EQ(h.max(), 0);
    EXPECT_EQ(h.mean(), 0.0);
}

// Compare against the exact quantile of the recorded samples: the histogram
// may over-report by at most 1/32 and must never under-report.
TEST(LatencyHistogram, PercentilesMatchExactQuantilesWithinResolution) {
    std::mt19937_64 rng(11);
    std::lognormal_distribution<double> dist(9.0, 1.2); // ~8 us median, long tail
    std::vector<std::int64_t> samples;
    LatencyHistogram h;
    for (int i = 0; i < 200'000; ++i) {
        const auto v = static_cast<std::int64_t>(dist(rng));
        samples.push_back(v);
        h.record(v);
    }
    std::sort(samples.begin(), samples.end());
    for (const double q : {0.0, 0.1, 0.5, 0.9, 0.99, 0.999, 0.9999, 1.0}) {
        const std::size_t rank = std::max<std::size_t>(1, static_cast<std::size_t>(std::ceil(q * samples.size())));
        const std::int64_t exact = samples[rank - 1];
        const std::int64_t got = h.percentile(q);
        EXPECT_GE(got, exact) << "q=" << q;
        EXPECT_LE(static_cast<double>(got - exact), static_cast<double>(exact) / 32.0 + 1) << "q=" << q;
    }
    EXPECT_EQ(h.max(), samples.back());
    EXPECT_EQ(h.percentile(1.0), samples.back());
}

TEST(LatencyHistogram, NegativeValuesCountAsZero) {
    LatencyHistogram h;
    h.record(-5);
    EXPECT_EQ(h.count(), 1u);
    EXPECT_EQ(h.percentile(1.0), 0);
}

TEST(LatencyHistogram, MergeEqualsRecordingEverythingInOne) {
    LatencyHistogram a, b, all;
    for (int i = 0; i < 1000; ++i) {
        (i % 3 ? a : b).record(i * 37);
        all.record(i * 37);
    }
    a.merge(b);
    EXPECT_EQ(a.count(), all.count());
    EXPECT_EQ(a.max(), all.max());
    EXPECT_DOUBLE_EQ(a.mean(), all.mean());
    for (const double q : {0.5, 0.9, 0.99})
        EXPECT_EQ(a.percentile(q), all.percentile(q));
    a.reset();
    EXPECT_EQ(a.count(), 0u);
    EXPECT_EQ(a.max(), 0);
}

TEST(LatencyHistogram, ConcurrentRecordersLoseNothing) {
    LatencyHistogram h;
    constexpr int kThreads = 4, kPer = 250'000;
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t)
        threads.emplace_back([&h, t] {
            for (int i = 0; i < kPer; ++i)
                h.record(t * 1000 + i % 1000);
        });
    for (auto& t : threads)
        t.join();
    EXPECT_EQ(h.count(), static_cast<std::uint64_t>(kThreads) * kPer);
    EXPECT_EQ(h.max(), (kThreads - 1) * 1000 + 999);
}
