// gateway/tests/test_conflating_queue.cpp
#include "fmdg/conflating_queue.h"

#include <gtest/gtest.h>

#include <atomic>
#include <thread>
#include <vector>

using fmdg::ConflatingQueue;
using fmdg::SymbolId;
using fmdg::Tick;

namespace {

Tick tick(SymbolId sym, std::uint64_t seq, double price = 0) {
    Tick t;
    t.symbol = sym;
    t.sequence = seq;
    t.price = price;
    return t;
}

} // namespace

TEST(ConflatingQueue, KeepUpMeansEveryTickInOrder) {
    ConflatingQueue q;
    EXPECT_TRUE(q.push(tick(1, 1)));  // was empty → writer must be woken
    EXPECT_FALSE(q.push(tick(2, 2))); // already non-empty
    EXPECT_FALSE(q.push(tick(3, 3)));
    std::vector<Tick> out;
    EXPECT_EQ(q.drain(out, 100), 3u);
    ASSERT_EQ(out.size(), 3u);
    EXPECT_EQ(out[0].sequence, 1u);
    EXPECT_EQ(out[1].sequence, 2u);
    EXPECT_EQ(out[2].sequence, 3u);
    EXPECT_EQ(q.conflated(), 0u);
    EXPECT_EQ(q.size(), 0u);
}

TEST(ConflatingQueue, NewerTickReplacesPendingOneInPlace) {
    ConflatingQueue q;
    q.push(tick(1, 1, 10.0));
    q.push(tick(2, 2, 20.0));
    q.push(tick(1, 3, 11.0)); // replaces seq 1, keeps symbol 1's place (first)
    q.push(tick(1, 4, 12.0)); // and again
    EXPECT_EQ(q.size(), 2u);
    EXPECT_EQ(q.conflated(), 2u);
    std::vector<Tick> out;
    q.drain(out, 100);
    ASSERT_EQ(out.size(), 2u);
    EXPECT_EQ(out[0].symbol, 1u);
    EXPECT_EQ(out[0].sequence, 4u) << "the latest value is what gets sent";
    EXPECT_EQ(out[0].price, 12.0);
    EXPECT_EQ(out[1].symbol, 2u);
}

TEST(ConflatingQueue, AfterDrainASymbolQueuesAgain) {
    ConflatingQueue q;
    q.push(tick(1, 1));
    std::vector<Tick> out;
    q.drain(out, 100);
    EXPECT_TRUE(q.push(tick(1, 2))) << "drained slot is no longer pending";
    EXPECT_EQ(q.conflated(), 0u);
    out.clear();
    q.drain(out, 100);
    ASSERT_EQ(out.size(), 1u);
    EXPECT_EQ(out[0].sequence, 2u);
}

TEST(ConflatingQueue, DrainRespectsMaxAndAppends) {
    ConflatingQueue q;
    for (SymbolId s = 0; s < 10; ++s)
        q.push(tick(s, s));
    std::vector<Tick> out{tick(99, 99)};
    EXPECT_EQ(q.drain(out, 4), 4u);
    EXPECT_EQ(out.size(), 5u);
    EXPECT_EQ(out[1].symbol, 0u);
    EXPECT_EQ(q.size(), 6u);
    EXPECT_EQ(q.drain(out, 100), 6u);
    EXPECT_EQ(out.back().symbol, 9u);
}

// The slow-consumer guarantee: however many ticks arrive while nothing is
// drained, the backlog never exceeds the number of distinct symbols.
TEST(ConflatingQueue, BacklogIsBoundedBySymbolCount) {
    ConflatingQueue q;
    constexpr SymbolId kSymbols = 50;
    for (std::uint64_t i = 0; i < 100'000; ++i)
        q.push(tick(static_cast<SymbolId>(i % kSymbols), i));
    EXPECT_EQ(q.size(), kSymbols);
    EXPECT_EQ(q.high_water(), kSymbols);
    EXPECT_EQ(q.conflated(), 100'000u - kSymbols);
    std::vector<Tick> out;
    q.drain(out, 1000);
    for (const Tick& t : out)
        EXPECT_GE(t.sequence, 100'000u - kSymbols) << "each symbol's newest tick survives";
}

// One pusher, one drainer, as in the gateway. Per symbol, sequences must come
// out strictly increasing, and pushed == delivered + conflated.
TEST(ConflatingQueue, ConcurrentPushAndDrainKeepPerSymbolOrder) {
    ConflatingQueue q;
    constexpr std::uint64_t kCount = 500'000;
    constexpr SymbolId kSymbols = 16;
    std::atomic<bool> done{false};
    std::thread pusher([&] {
        for (std::uint64_t i = 1; i <= kCount; ++i)
            q.push(tick(static_cast<SymbolId>(i % kSymbols), i));
        done = true;
    });
    std::vector<std::uint64_t> last(kSymbols, 0);
    std::uint64_t delivered = 0, out_of_order = 0;
    std::vector<Tick> out;
    for (;;) {
        const bool finished = done.load();
        out.clear();
        q.drain(out, 64);
        for (const Tick& t : out) {
            if (t.sequence <= last[t.symbol])
                ++out_of_order;
            last[t.symbol] = t.sequence;
        }
        delivered += out.size();
        if (finished && q.size() == 0)
            break;
    }
    pusher.join();
    EXPECT_EQ(out_of_order, 0u);
    EXPECT_EQ(delivered + q.conflated(), kCount);
    for (SymbolId s = 0; s < kSymbols; ++s)
        EXPECT_GT(last[s], kCount - kSymbols) << "final value of every symbol delivered";
}
