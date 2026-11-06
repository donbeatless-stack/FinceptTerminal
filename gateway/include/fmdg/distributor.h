// gateway/include/fmdg/distributor.h
#pragma once

#include "fmdg/spsc_ring.h"
#include "fmdg/tick.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fmdg {

/// Something that receives the ticks of the symbols it subscribed to.
class Subscriber {
  public:
    virtual ~Subscriber() = default;
    /// Called on the distributor thread, and on the subscribing thread for the
    /// initial snapshot. Must not block: queue the tick and return.
    virtual void deliver(const Tick& t) = 0;
};

/// The single consumer of the ingest ring. For each tick it records the symbol's
/// last value (served to new subscribers and to the request port) and hands the
/// tick to that symbol's subscribers.
///
/// Threads: one producer (the feed) calls publish(); the distributor's own thread
/// consumes; any thread may (un)subscribe or read last(). When idle the thread
/// spins briefly, then sleeps on an atomic wait that publish() ends — so an idle
/// gateway does not burn a core, and a tick arriving after a pause is not held
/// for the 1–15 ms a timed sleep can take on Windows.
class Distributor {
  public:
    /// spins_before_sleep: empty polls (each a CPU pause) before the idle thread
    /// sleeps. The default spins ~0.1–0.5 ms, so a steady feed never pays the
    /// wake-up cost; 0 sleeps at once (tests use it to stress the sleep path).
    explicit Distributor(std::size_t ring_capacity, int spins_before_sleep = 4000);
    ~Distributor();

    Distributor(const Distributor&) = delete;
    Distributor& operator=(const Distributor&) = delete;

    /// Producer thread only. False when the ring is full (the caller decides
    /// whether to wait or drop).
    bool publish(const Tick& t);

    /// busy_poll: never sleep when idle — lowest latency, one core at 100 %.
    void start(bool busy_poll = false);
    void stop();
    /// Processes everything in the ring on the calling thread; for tests, and
    /// only while start() has not been called.
    std::size_t drain_now();

    /// Adds `sub` to each symbol, then delivers each symbol's last tick, if any,
    /// so a new subscriber starts from the current value. A tick being published
    /// at the same moment can reach `sub` twice; sequence numbers make that visible.
    void subscribe(const std::shared_ptr<Subscriber>& sub, const std::vector<SymbolId>& ids);
    void unsubscribe(const Subscriber* sub, const std::vector<SymbolId>& ids);
    /// Removes `sub` from every symbol (a disconnect).
    void remove(const Subscriber* sub);

    /// `listener` is called whenever the set of symbols with at least one
    /// subscriber changes (a symbol gains its first or loses its last one), on
    /// the thread that changed it, after the subscription lock is released. It
    /// must not block or call back into the distributor: an upstream feed posts
    /// itself a note and reads demanded() later. Any thread; nullptr removes it,
    /// and once this returns the old listener is not running and won't run again.
    void set_demand_listener(std::function<void()> listener);
    /// The symbols that have at least one subscriber now.
    std::vector<SymbolId> demanded() const;

    std::optional<Tick> last(SymbolId id) const;
    std::uint64_t ticks_in() const { return ticks_in_.load(std::memory_order_relaxed); }
    /// Ticks handed to subscribers, live and snapshot: every one is then either
    /// written to its client or conflated (or dropped when the client leaves).
    std::uint64_t deliveries() const { return deliveries_.load(std::memory_order_relaxed); }
    std::size_t backlog() const { return ring_.size_approx(); }

  private:
    void run(bool busy_poll);
    void process(const Tick& t);

    SpscRing<Tick> ring_;
    const int spins_before_sleep_;

    mutable std::shared_mutex subs_mu_;
    std::vector<std::vector<std::shared_ptr<Subscriber>>> subs_;          ///< by SymbolId
    std::unordered_map<const Subscriber*, std::vector<SymbolId>> by_sub_; ///< for remove()

    void notify_demand();
    /// Held while the listener runs, so set_demand_listener() can wait it out.
    std::mutex listener_mu_;
    std::function<void()> listener_;

    mutable std::mutex last_mu_;
    std::vector<Tick> last_;
    std::vector<std::uint8_t> has_last_;

    std::atomic<bool> running_{false};
    std::atomic<bool> sleeping_{false};
    std::atomic<std::uint32_t> wake_{0};
    std::thread thread_;

    std::atomic<std::uint64_t> ticks_in_{0};
    std::atomic<std::uint64_t> deliveries_{0};
};

} // namespace fmdg
