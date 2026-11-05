// gateway/tests/test_pool.cpp — Pool<Conn> (fmdg/pool.h) with fake connections.
// Each test pins one rule of the pool; the fakes count how many connections are
// alive, opening, and in use, so the size cap and "nothing slow under the lock"
// are checked directly rather than inferred.

#include "fmdg/pool.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <optional>
#include <stdexcept>
#include <thread>
#include <vector>

using namespace fmdg;
using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

namespace {

struct Fakes;

struct FakeConn {
    FakeConn(int id, Fakes& f);
    ~FakeConn();
    const int id;
    Fakes& fakes;
    std::atomic<int> users{0}; ///< threads using this connection at once: must stay ≤ 1
};

struct Fakes {
    std::atomic<int> alive{0}, peak{0}, opening{0}, opens{0}, next_id{0}, checks{0};
    std::atomic<int> fail_next{0};     ///< fail this many opens
    std::atomic<int> open_delay_ms{0}; ///< each open takes this long
    std::atomic<bool> healthy{true};   ///< what the health check answers

    Pool<FakeConn>::Factory factory() {
        return [this]() -> std::unique_ptr<FakeConn> {
            ++opening;
            if (const int ms = open_delay_ms.load())
                std::this_thread::sleep_for(std::chrono::milliseconds(ms));
            --opening;
            if (fail_next.load() > 0) {
                --fail_next;
                throw std::runtime_error("connection refused");
            }
            ++opens;
            return std::make_unique<FakeConn>(next_id++, *this);
        };
    }
    Pool<FakeConn>::Check check() {
        return [this](FakeConn&) {
            ++checks;
            return healthy.load();
        };
    }
};

FakeConn::FakeConn(int i, Fakes& f) : id(i), fakes(f) {
    const int n = ++fakes.alive;
    int p = fakes.peak.load();
    while (n > p && !fakes.peak.compare_exchange_weak(p, n)) {
    }
}
FakeConn::~FakeConn() {
    --fakes.alive;
}

PoolConfig config(std::size_t max_size) {
    PoolConfig c;
    c.max_size = max_size;
    c.acquire_timeout = 5s;
    return c;
}

bool eventually(const std::function<bool()>& done, std::chrono::milliseconds limit = 2s) {
    const auto end = Clock::now() + limit;
    while (!done()) {
        if (Clock::now() > end)
            return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

} // namespace

TEST(Pool, ReusesAnIdleConnection) {
    Fakes f;
    Pool<FakeConn> pool(config(4), f.factory(), f.check());
    int first = -1;
    {
        auto lease = pool.acquire();
        first = lease->id;
    }
    for (int i = 0; i < 100; ++i)
        EXPECT_EQ(pool.acquire()->id, first);
    const PoolStats st = pool.stats();
    EXPECT_EQ(st.created, 1u);
    EXPECT_EQ(st.acquired, 101u);
    EXPECT_EQ(st.idle, 1u);
    EXPECT_EQ(st.leased, 0u);
    EXPECT_EQ(f.checks.load(), 0); // returned moments ago: no health check needed
}

TEST(Pool, NeverExceedsMaxSizeUnderContention) {
    Fakes f;
    Pool<FakeConn> pool(config(4), f.factory(), f.check());
    constexpr int kThreads = 16, kRounds = 2000;
    std::atomic<int> shared_use{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t)
        threads.emplace_back([&] {
            for (int i = 0; i < kRounds; ++i) {
                auto lease = pool.acquire();
                if (++lease->users != 1)
                    ++shared_use; // two threads holding one connection
                std::this_thread::yield();
                --lease->users;
            }
        });
    for (auto& t : threads)
        t.join();

    const PoolStats st = pool.stats();
    EXPECT_EQ(shared_use.load(), 0);
    EXPECT_LE(f.peak.load(), 4);
    EXPECT_EQ(st.acquired, static_cast<std::uint64_t>(kThreads * kRounds));
    EXPECT_EQ(st.leased, 0u);
    EXPECT_EQ(st.waiting, 0u);
    EXPECT_EQ(st.timeouts, 0u);
    EXPECT_EQ(st.open, st.idle);
    EXPECT_EQ(f.alive.load(), static_cast<int>(st.open)); // nothing leaked
}

TEST(Pool, TimesOutWhenEveryConnectionIsLent) {
    Fakes f;
    Pool<FakeConn> pool(config(2), f.factory(), f.check());
    auto a = pool.acquire();
    auto b = pool.acquire();
    const auto t0 = Clock::now();
    EXPECT_THROW(pool.acquire(50ms), PoolTimeout);
    EXPECT_GE(Clock::now() - t0, 50ms);
    const PoolStats st = pool.stats();
    EXPECT_EQ(st.timeouts, 1u);
    EXPECT_EQ(st.waiting, 0u);
    EXPECT_EQ(f.opens.load(), 2);
}

TEST(Pool, AReturnedConnectionGoesToAWaiter) {
    Fakes f;
    Pool<FakeConn> pool(config(1), f.factory(), f.check());
    std::optional<Pool<FakeConn>::Lease> held(pool.acquire());
    const int id = (*held)->id;
    std::atomic<int> got{-1};
    std::thread waiter([&] { got = pool.acquire(2s)->id; });
    ASSERT_TRUE(eventually([&] { return pool.stats().waiting == 1; }));
    std::this_thread::sleep_for(20ms);
    held.reset(); // return it
    waiter.join();
    EXPECT_EQ(got.load(), id); // the same connection, handed over
    EXPECT_EQ(f.opens.load(), 1);
}

TEST(Pool, DiscardClosesTheConnectionAndFreesItsSlot) {
    Fakes f;
    Pool<FakeConn> pool(config(1), f.factory(), f.check());
    int first = -1;
    {
        auto lease = pool.acquire();
        first = lease->id;
        lease.discard(); // the caller saw it break
    }
    EXPECT_EQ(f.alive.load(), 0);
    EXPECT_NE(pool.acquire()->id, first);
    const PoolStats st = pool.stats();
    EXPECT_EQ(st.discarded, 1u);
    EXPECT_EQ(st.closed, 1u);
    EXPECT_EQ(st.created, 2u);
}

TEST(Pool, AFailedOpenReachesTheCallerAndFreesTheSlot) {
    Fakes f;
    Pool<FakeConn> pool(config(1), f.factory(), f.check());
    f.fail_next = 1;
    EXPECT_THROW(pool.acquire(), std::runtime_error);
    EXPECT_EQ(pool.stats().open, 0u);
    EXPECT_EQ(pool.stats().create_failures, 1u);
    EXPECT_TRUE(pool.acquire()); // the slot is usable again
}

TEST(Pool, AFailedOpenWakesAWaiter) {
    // The only slot is reserved by an open that will fail; a second borrower waits
    // on it. The failure must wake that borrower, not leave it to its timeout.
    Fakes f;
    Pool<FakeConn> pool(config(1), f.factory(), f.check());
    f.open_delay_ms = 100;
    f.fail_next = 1;
    std::thread failing([&] { EXPECT_THROW(pool.acquire(), std::runtime_error); });
    ASSERT_TRUE(eventually([&] { return f.opening.load() == 1; }));
    const auto t0 = Clock::now();
    auto lease = pool.acquire(3s); // waits behind the failing open, then opens its own
    EXPECT_LT(Clock::now() - t0, 1s);
    EXPECT_TRUE(lease);
    failing.join();
}

TEST(Pool, ASlowOpenDoesNotBlockBorrowersOfIdleConnections) {
    Fakes f;
    Pool<FakeConn> pool(config(2), f.factory(), f.check());
    std::optional<Pool<FakeConn>::Lease> held(pool.acquire()); // opened fast
    f.open_delay_ms = 300;
    std::thread slow([&] { pool.acquire(); }); // must open the second connection: 300 ms
    ASSERT_TRUE(eventually([&] { return f.opening.load() == 1; }));
    // Returning a connection and borrowing it again both take the pool's lock;
    // neither may wait for the open in progress. (Timing only the borrow missed a
    // mutant that held the lock through the open: the return absorbed the wait.)
    const auto t0 = Clock::now();
    held.reset(); // one idle connection now, while the slow open is in progress
    auto quick = pool.acquire();
    EXPECT_LT(Clock::now() - t0, 100ms);
    slow.join();
}

TEST(Pool, ConcurrentOpensRespectMaxSize) {
    // An empty pool, eight borrowers at once, opens that take a while: the slot
    // must be reserved before the lock is released, or every borrower sees room
    // and opens its own connection.
    Fakes f;
    Pool<FakeConn> pool(config(2), f.factory(), f.check());
    f.open_delay_ms = 50;
    std::vector<std::thread> threads;
    for (int t = 0; t < 8; ++t)
        threads.emplace_back([&] {
            auto lease = pool.acquire();
            std::this_thread::sleep_for(10ms);
        });
    for (auto& t : threads)
        t.join();
    EXPECT_LE(f.peak.load(), 2);
    EXPECT_EQ(f.opens.load(), 2);
}

TEST(Pool, AnIdleConnectionIsCheckedAndReplacedIfDead) {
    Fakes f;
    PoolConfig c = config(2);
    c.validate_after = 0ms; // check every idle connection before lending it
    Pool<FakeConn> pool(c, f.factory(), f.check());
    int first = -1;
    {
        auto lease = pool.acquire();
        first = lease->id;
    }
    EXPECT_EQ(f.checks.load(), 0); // a freshly opened connection is not checked
    f.healthy = false;
    {
        auto lease = pool.acquire();
        EXPECT_NE(lease->id, first);
    }
    EXPECT_EQ(f.checks.load(), 1);
    EXPECT_EQ(pool.stats().failed_checks, 1u);
    EXPECT_EQ(f.alive.load(), 1);
}

TEST(Pool, OldAndLongIdleConnectionsAreRetired) {
    Fakes f;
    PoolConfig c = config(2);
    c.max_lifetime = 50ms;
    Pool<FakeConn> pool(c, f.factory(), f.check());
    int first = -1;
    {
        auto lease = pool.acquire();
        first = lease->id;
    }
    std::this_thread::sleep_for(60ms);
    EXPECT_NE(pool.acquire()->id, first);
    EXPECT_EQ(pool.stats().closed, 1u);

    Fakes g;
    PoolConfig d = config(2);
    d.max_idle = 50ms;
    Pool<FakeConn> idle_pool(d, g.factory(), g.check());
    {
        auto lease = idle_pool.acquire();
        first = lease->id;
    }
    std::this_thread::sleep_for(60ms);
    EXPECT_NE(idle_pool.acquire()->id, first);
}

TEST(Pool, ALeaseMayOutliveThePool) {
    Fakes f;
    std::optional<Pool<FakeConn>> pool;
    pool.emplace(config(2), f.factory(), f.check());
    auto lease = pool->acquire();
    {
        auto other = pool->acquire(); // returned: idle
    }
    EXPECT_EQ(f.alive.load(), 2);
    pool.reset(); // closes the idle one
    EXPECT_EQ(f.alive.load(), 1);
    lease = {}; // the lent one closes on return
    EXPECT_EQ(f.alive.load(), 0);
}
