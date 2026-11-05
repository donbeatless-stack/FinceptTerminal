// gateway/include/fmdg/pool.h
#pragma once
// A bounded, thread-safe connection pool, generic over the connection type: the
// rules are unit-tested with fake connections (tests/test_pool.cpp), and the
// gateway uses it with PostgreSQL connections (pg.h).
//
// The rules, in short:
//   - at most `max_size` connections exist at once, lent out or idle, counting
//     ones being opened right now;
//   - acquire() lends an idle connection (most recently returned first, so the
//     surplus ages out), or opens one if there is room, or waits up to
//     `acquire_timeout` for a return, then throws PoolTimeout;
//   - nothing slow runs under the lock: opening, health-checking and closing a
//     connection are network round trips, done with the slot reserved and the
//     lock released, so one slow connect never stalls other borrowers;
//   - a connection idle for `validate_after` is health-checked before it is lent;
//     one older than `max_lifetime` or idle longer than `max_idle` is closed;
//   - a Lease returns its connection when it goes out of scope; discard() closes
//     it instead (the caller saw it break) and frees the slot.

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

namespace fmdg {

struct PoolConfig {
    std::size_t max_size = 8;
    std::chrono::milliseconds acquire_timeout{5'000};
    std::chrono::milliseconds validate_after{30'000}; ///< idle this long → health-checked before lending
    std::chrono::milliseconds max_idle{300'000};      ///< idle longer → closed instead of lent
    std::chrono::milliseconds max_lifetime{3'600'000}; ///< older → closed (server-side bloat, rotated credentials)
};

struct PoolStats {
    std::size_t open = 0;    ///< lent + idle + being opened
    std::size_t idle = 0;
    std::size_t leased = 0;
    std::size_t waiting = 0; ///< threads inside acquire() right now
    std::uint64_t acquired = 0;
    std::uint64_t created = 0;
    std::uint64_t closed = 0;
    std::uint64_t create_failures = 0;
    std::uint64_t timeouts = 0;
    std::uint64_t failed_checks = 0;  ///< idle connections that failed their health check
    std::uint64_t discarded = 0;      ///< leases returned with discard()
    std::int64_t wait_ns_total = 0;   ///< time spent inside successful acquire() calls
    std::int64_t wait_ns_max = 0;
};

class PoolTimeout : public std::runtime_error {
  public:
    using std::runtime_error::runtime_error;
};

template <class Conn>
class Pool {
  public:
    using Clock = std::chrono::steady_clock;
    /// Opens one connection; throws on failure (the exception reaches the caller of acquire()).
    using Factory = std::function<std::unique_ptr<Conn>()>;
    /// True if the connection still works (e.g. "SELECT 1" succeeds).
    using Check = std::function<bool(Conn&)>;

  private:
    struct Slot {
        std::unique_ptr<Conn> conn;
        Clock::time_point created;
        Clock::time_point returned;
    };

    struct State {
        PoolConfig cfg;
        Factory factory;
        Check check;
        std::mutex mu;
        std::condition_variable cv;
        std::vector<Slot> idle; ///< back = most recently returned
        std::size_t open = 0;
        std::size_t leased = 0;
        std::size_t waiting = 0;
        bool closed = false;
        PoolStats st;

        bool expired(const Slot& s, Clock::time_point now) const {
            return now - s.created >= cfg.max_lifetime || now - s.returned >= cfg.max_idle;
        }
    };

  public:
    /// Lends one connection; returns it (or closes it, after discard()) when
    /// destroyed. Move-only. Safe to outlive the Pool: the connection is then closed.
    class Lease {
      public:
        Lease() = default;
        Lease(Lease&& o) noexcept = default;
        Lease& operator=(Lease&& o) noexcept {
            if (this != &o) {
                release();
                state_ = std::move(o.state_);
                slot_ = std::move(o.slot_);
                discard_ = o.discard_;
            }
            return *this;
        }
        Lease(const Lease&) = delete;
        Lease& operator=(const Lease&) = delete;
        ~Lease() { release(); }

        Conn& operator*() const { return *slot_.conn; }
        Conn* operator->() const { return slot_.conn.get(); }
        explicit operator bool() const { return slot_.conn != nullptr; }

        /// The connection is broken (or in an unknown state): close it on return.
        void discard() { discard_ = true; }

      private:
        friend class Pool;
        Lease(std::shared_ptr<State> state, Slot slot) : state_(std::move(state)), slot_(std::move(slot)) {}

        void release() {
            if (!slot_.conn || !state_)
                return;
            std::unique_ptr<Conn> to_close;
            {
                std::lock_guard lock(state_->mu);
                --state_->leased;
                const auto now = Clock::now();
                if (discard_ || state_->closed || now - slot_.created >= state_->cfg.max_lifetime) {
                    if (discard_)
                        ++state_->st.discarded;
                    --state_->open;
                    ++state_->st.closed;
                    to_close = std::move(slot_.conn);
                } else {
                    slot_.returned = now;
                    state_->idle.push_back(std::move(slot_));
                }
            }
            state_->cv.notify_one(); // a connection or a slot is free
            slot_ = {};
            state_.reset();
            // to_close dies here, outside the lock
        }

        std::shared_ptr<State> state_;
        Slot slot_;
        bool discard_ = false;
    };

    Pool(PoolConfig cfg, Factory factory, Check check) : state_(std::make_shared<State>()) {
        state_->cfg = cfg;
        state_->factory = std::move(factory);
        state_->check = std::move(check);
    }

    /// Closes the idle connections now; lent ones close when their Lease ends.
    /// Destroy the pool only once no thread is inside acquire().
    ~Pool() {
        std::vector<Slot> idle;
        {
            std::lock_guard lock(state_->mu);
            state_->closed = true;
            idle.swap(state_->idle);
            state_->open -= idle.size();
            state_->st.closed += idle.size();
        }
        state_->cv.notify_all();
    }

    Pool(const Pool&) = delete;
    Pool& operator=(const Pool&) = delete;

    Lease acquire() { return acquire(state_->cfg.acquire_timeout); }

    Lease acquire(std::chrono::milliseconds timeout) {
        State& s = *state_;
        const auto start = Clock::now();
        const auto deadline = start + timeout;
        std::unique_lock lock(s.mu);
        ++s.waiting;
        struct WaitingGuard { // --waiting on every exit, normal or thrown
            State& s;
            ~WaitingGuard() { --s.waiting; }
        } guard{s};

        for (;;) {
            if (s.closed)
                throw std::runtime_error("pool is closed");

            // 1. An idle connection, newest first.
            while (!s.idle.empty()) {
                Slot slot = std::move(s.idle.back());
                s.idle.pop_back();
                const auto now = Clock::now();
                if (s.expired(slot, now)) {
                    --s.open;
                    ++s.st.closed;
                    lock.unlock();
                    slot.conn.reset(); // close outside the lock
                    s.cv.notify_one(); // a slot is free
                    lock.lock();
                    continue;
                }
                if (now - slot.returned >= s.cfg.validate_after) {
                    // Health check with the slot still counted in `open`, so no one
                    // can open a connection in its place meanwhile.
                    lock.unlock();
                    bool ok = false;
                    try {
                        ok = s.check(*slot.conn);
                    } catch (...) {
                        ok = false;
                    }
                    if (!ok)
                        slot.conn.reset();
                    lock.lock();
                    if (!ok) {
                        --s.open;
                        ++s.st.closed;
                        ++s.st.failed_checks;
                        s.cv.notify_one();
                        continue;
                    }
                }
                return lend(std::move(slot), start);
            }

            // 2. Room for a new connection: reserve the slot, open it unlocked.
            if (s.open < s.cfg.max_size) {
                ++s.open;
                lock.unlock();
                Slot slot;
                try {
                    slot.conn = s.factory();
                    if (!slot.conn)
                        throw std::runtime_error("pool factory returned no connection");
                } catch (...) {
                    lock.lock();
                    --s.open;
                    ++s.st.create_failures;
                    lock.unlock();
                    s.cv.notify_one(); // the reserved slot is free again
                    lock.lock();
                    throw;
                }
                slot.created = slot.returned = Clock::now();
                lock.lock();
                ++s.st.created;
                return lend(std::move(slot), start);
            }

            // 3. Full: wait for a return, a discard, or a failed open.
            if (s.cv.wait_until(lock, deadline) == std::cv_status::timeout && Clock::now() >= deadline) {
                // One last look: a return may have raced the timeout.
                if (s.idle.empty() && s.open >= s.cfg.max_size) {
                    ++s.st.timeouts;
                    throw PoolTimeout("no connection free within " + std::to_string(timeout.count()) + " ms (" +
                                      std::to_string(s.cfg.max_size) + " in use)");
                }
            }
        }
    }

    PoolStats stats() const {
        std::lock_guard lock(state_->mu);
        PoolStats out = state_->st;
        out.open = state_->open;
        out.idle = state_->idle.size();
        out.leased = state_->leased;
        out.waiting = state_->waiting;
        return out;
    }

    const PoolConfig& config() const { return state_->cfg; }

  private:
    // Called with the lock held.
    Lease lend(Slot slot, Clock::time_point start) {
        State& s = *state_;
        ++s.leased;
        ++s.st.acquired;
        const auto waited = std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now() - start).count();
        s.st.wait_ns_total += waited;
        if (waited > s.st.wait_ns_max)
            s.st.wait_ns_max = waited;
        return Lease(state_, std::move(slot));
    }

    std::shared_ptr<State> state_;
};

} // namespace fmdg
