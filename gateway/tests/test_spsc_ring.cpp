// gateway/tests/test_spsc_ring.cpp
#include "fmdg/spsc_ring.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <thread>

using fmdg::SpscRing;

TEST(SpscRing, CapacityRoundsUpToPowerOfTwo) {
    EXPECT_EQ(SpscRing<int>(0).capacity(), 2u);
    EXPECT_EQ(SpscRing<int>(2).capacity(), 2u);
    EXPECT_EQ(SpscRing<int>(3).capacity(), 4u);
    EXPECT_EQ(SpscRing<int>(1000).capacity(), 1024u);
    EXPECT_EQ(SpscRing<int>(1024).capacity(), 1024u);
}

TEST(SpscRing, FifoUntilFullThenRefuses) {
    SpscRing<int> ring(4);
    int out = -1;
    EXPECT_FALSE(ring.try_pop(out));
    for (int i = 0; i < 4; ++i)
        EXPECT_TRUE(ring.try_push(i));
    EXPECT_FALSE(ring.try_push(99)) << "a full ring must refuse, not overwrite";
    EXPECT_EQ(ring.size_approx(), 4u);
    for (int i = 0; i < 4; ++i) {
        ASSERT_TRUE(ring.try_pop(out));
        EXPECT_EQ(out, i);
    }
    EXPECT_FALSE(ring.try_pop(out));
    EXPECT_EQ(ring.size_approx(), 0u);
}

TEST(SpscRing, WrapsAroundManyTimes) {
    SpscRing<std::uint64_t> ring(8);
    std::uint64_t next_in = 0, next_out = 0, v = 0;
    for (int round = 0; round < 10'000; ++round) {
        const int burst = 1 + round % 8;
        for (int i = 0; i < burst; ++i)
            ASSERT_TRUE(ring.try_push(next_in++));
        for (int i = 0; i < burst; ++i) {
            ASSERT_TRUE(ring.try_pop(v));
            ASSERT_EQ(v, next_out++);
        }
    }
}

// The property that matters: across two threads, nothing is lost, duplicated
// or reordered, including while the ring keeps hitting full and empty.
TEST(SpscRing, TwoThreadsDeliverEverythingInOrder) {
    constexpr std::uint64_t kCount = 2'000'000;
    SpscRing<std::uint64_t> ring(64); // small, so both full and empty happen constantly
    std::thread producer([&] {
        for (std::uint64_t i = 0; i < kCount; ++i)
            while (!ring.try_push(i))
                std::this_thread::yield();
    });
    std::uint64_t expected = 0, v = 0, mismatches = 0;
    while (expected < kCount) {
        if (ring.try_pop(v)) {
            if (v != expected)
                ++mismatches;
            expected = v + 1;
        } else {
            std::this_thread::yield();
        }
    }
    producer.join();
    EXPECT_EQ(mismatches, 0u);
    EXPECT_FALSE(ring.try_pop(v));
}
