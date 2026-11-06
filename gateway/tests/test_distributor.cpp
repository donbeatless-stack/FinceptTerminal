// gateway/tests/test_distributor.cpp
#include "fmdg/distributor.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

using namespace fmdg;
using namespace std::chrono_literals;

namespace {

class Recorder : public Subscriber {
  public:
    void deliver(const Tick& t) override {
        std::lock_guard lock(mu_);
        got_.push_back(t);
        count_.fetch_add(1, std::memory_order_release);
    }
    std::vector<Tick> got() const {
        std::lock_guard lock(mu_);
        return got_;
    }
    std::uint64_t count() const { return count_.load(std::memory_order_acquire); }

  private:
    mutable std::mutex mu_;
    std::vector<Tick> got_;
    std::atomic<std::uint64_t> count_{0};
};

Tick tick(SymbolId sym, std::uint64_t seq, double price = 1.0) {
    Tick t;
    t.symbol = sym;
    t.sequence = seq;
    t.price = price;
    return t;
}

bool wait_for(const std::function<bool()>& pred, std::chrono::milliseconds limit = 2000ms) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::yield();
    }
    return true;
}

} // namespace

TEST(Distributor, DeliversOnlyToSubscribersOfTheSymbol) {
    Distributor d(64);
    auto a = std::make_shared<Recorder>();
    auto b = std::make_shared<Recorder>();
    d.subscribe(a, {1});
    d.subscribe(b, {1, 2});
    d.publish(tick(1, 1));
    d.publish(tick(2, 2));
    d.publish(tick(3, 3)); // nobody
    EXPECT_EQ(d.drain_now(), 3u);
    EXPECT_EQ(a->count(), 1u);
    EXPECT_EQ(b->count(), 2u);
    EXPECT_EQ(d.ticks_in(), 3u);
    EXPECT_EQ(d.deliveries(), 3u);
}

TEST(Distributor, NewSubscriberStartsFromTheLastValue) {
    Distributor d(64);
    d.publish(tick(5, 1, 10.0));
    d.publish(tick(5, 2, 11.0));
    d.drain_now();
    ASSERT_TRUE(d.last(5));
    EXPECT_EQ(d.last(5)->price, 11.0);
    EXPECT_FALSE(d.last(6));
    EXPECT_FALSE(d.last(1'000'000));

    auto r = std::make_shared<Recorder>();
    d.subscribe(r, {5, 6});
    const auto got = r->got();
    ASSERT_EQ(got.size(), 1u) << "snapshot for 5 only: 6 has no value yet";
    EXPECT_EQ(got[0].sequence, 2u);
    EXPECT_EQ(got[0].flags, kTickSnapshot);
    EXPECT_EQ(d.last(5)->flags, 0) << "the stored value is not marked";
    EXPECT_EQ(d.deliveries(), 1u) << "snapshots count as deliveries";
}

TEST(Distributor, SubscribingTwiceDoesNotDuplicate) {
    Distributor d(64);
    auto r = std::make_shared<Recorder>();
    d.subscribe(r, {1});
    d.subscribe(r, {1});
    d.publish(tick(1, 1));
    d.drain_now();
    // Neither subscribe had a snapshot to send; the tick goes out once.
    EXPECT_EQ(r->count(), 1u);
}

TEST(Distributor, UnsubscribeAndRemoveStopDelivery) {
    Distributor d(64);
    auto r = std::make_shared<Recorder>();
    d.subscribe(r, {1, 2, 3});
    d.unsubscribe(r.get(), {2});
    d.publish(tick(1, 1));
    d.publish(tick(2, 2));
    d.drain_now();
    EXPECT_EQ(r->count(), 1u);
    d.remove(r.get());
    d.publish(tick(1, 3));
    d.publish(tick(3, 4));
    d.drain_now();
    EXPECT_EQ(r->count(), 1u);
    EXPECT_EQ(r.use_count(), 1) << "the distributor must drop its reference";
}

TEST(Distributor, ThreadedDeliveryIsCompleteAndOrdered) {
    Distributor d(1024);
    auto r = std::make_shared<Recorder>();
    std::vector<SymbolId> ids;
    for (SymbolId s = 0; s < 8; ++s)
        ids.push_back(s);
    d.subscribe(r, ids);
    d.start();
    constexpr std::uint64_t kCount = 300'000;
    for (std::uint64_t i = 1; i <= kCount; ++i)
        while (!d.publish(tick(static_cast<SymbolId>(i % 8), i)))
            std::this_thread::yield();
    ASSERT_TRUE(wait_for([&] { return r->count() == kCount; }));
    d.stop();
    const auto got = r->got();
    for (std::size_t i = 0; i < got.size(); ++i)
        ASSERT_EQ(got[i].sequence, i + 1);
}

// The idle distributor sleeps on an atomic wait; publish() must wake it. Pause
// for varying times so publishes land in every phase — busy, spinning, about to
// sleep, asleep. A lost wake-up shows up as a tick that never arrives.
TEST(Distributor, NoLostWakeupsAcrossSleepTransitions) {
    Distributor d(64);
    auto r = std::make_shared<Recorder>();
    d.subscribe(r, {0});
    d.start();
    std::mt19937 rng(5);
    for (std::uint64_t i = 1; i <= 400; ++i) {
        const auto pause = std::chrono::microseconds(rng() % 3000);
        const auto until = std::chrono::steady_clock::now() + pause;
        while (std::chrono::steady_clock::now() < until) {
        }
        ASSERT_TRUE(d.publish(tick(0, i)));
        ASSERT_TRUE(wait_for([&] { return r->count() == i; }, 1000ms)) << "tick " << i << " not delivered";
    }
    d.stop();
}

// Same property, aimed at the exact race. With no spinning, the consumer heads
// for sleep after every empty poll, while the producer publishes the moment it
// sees the previous tick delivered — so publishes keep landing between the
// consumer's last empty poll and its going to sleep. Without the re-check after
// `sleeping_` is set (and the fences around it), a tick is left in the ring with
// the consumer asleep, and the wait below times out.
TEST(Distributor, NoLostWakeupsWhenSleepingAfterEveryPoll) {
    Distributor d(64, /*spins_before_sleep=*/0);
    auto r = std::make_shared<Recorder>();
    d.subscribe(r, {0});
    d.start();
    for (std::uint64_t i = 1; i <= 50'000; ++i) {
        ASSERT_TRUE(d.publish(tick(0, i)));
        ASSERT_TRUE(wait_for([&] { return r->count() == i; }, 1000ms)) << "tick " << i << " not delivered";
    }
    d.stop();
}

// Re-subscribing while ticks flow: the snapshot may repeat a tick, but a
// subscriber must never see a symbol's sequence go backwards.
TEST(Distributor, ResubscribeRacingPublishNeverGoesBackwards) {
    Distributor d(4096);
    auto r = std::make_shared<Recorder>();
    d.start();
    std::atomic<bool> done{false};
    std::thread producer([&] {
        for (std::uint64_t i = 1; i <= 200'000; ++i)
            while (!d.publish(tick(0, i)))
                std::this_thread::yield();
        done = true;
    });
    while (!done.load()) {
        d.subscribe(r, {0});
        std::this_thread::yield();
        d.unsubscribe(r.get(), {0});
    }
    producer.join();
    d.stop();
    const auto got = r->got();
    ASSERT_FALSE(got.empty());
    std::uint64_t backwards = 0;
    for (std::size_t i = 1; i < got.size(); ++i)
        if (got[i].sequence < got[i - 1].sequence)
            ++backwards;
    EXPECT_EQ(backwards, 0u);
}

TEST(Distributor, DemandChangesOnlyWithAFirstOrLastSubscriber) {
    Distributor d(64);
    int calls = 0;
    d.set_demand_listener([&] { ++calls; });
    auto a = std::make_shared<Recorder>();
    auto b = std::make_shared<Recorder>();

    d.subscribe(a, {1, 2});
    EXPECT_EQ(calls, 1);
    EXPECT_EQ(d.demanded(), (std::vector<SymbolId>{1, 2}));
    d.subscribe(b, {2}); // 2 already had a subscriber: no change
    d.subscribe(a, {1}); // a duplicate: no change
    EXPECT_EQ(calls, 1);
    d.unsubscribe(a.get(), {2}); // b still follows 2
    EXPECT_EQ(calls, 1);
    d.unsubscribe(b.get(), {7}); // never subscribed: no change
    EXPECT_EQ(calls, 1);
    d.unsubscribe(a.get(), {1}); // 1 has nobody left
    EXPECT_EQ(calls, 2);
    EXPECT_EQ(d.demanded(), (std::vector<SymbolId>{2}));
    d.remove(b.get()); // a disconnect takes 2's last subscriber
    EXPECT_EQ(calls, 3);
    EXPECT_TRUE(d.demanded().empty());

    d.set_demand_listener(nullptr);
    d.subscribe(a, {3});
    EXPECT_EQ(calls, 3);
    EXPECT_EQ(d.demanded(), (std::vector<SymbolId>{3}));
}
