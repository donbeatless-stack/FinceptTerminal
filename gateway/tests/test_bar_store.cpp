// gateway/tests/test_bar_store.cpp — BarStore and pg::Connection against a real
// PostgreSQL. Runs only when FMDG_TEST_PG holds a libpq connection string, e.g.
//   host=localhost dbname=fmdg user=fmdg passfile=C:/fintech_terminal/pgpass.conf
// and is skipped otherwise. Each test writes to its own table and drops it.

#include "fmdg/bar_store.h"
#include "fmdg/pg.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <string>
#include <thread>

using namespace fmdg;
using namespace std::chrono_literals;

namespace {

std::string test_conninfo() {
#ifdef _WIN32
    char* buf = nullptr;
    std::size_t n = 0;
    if (_dupenv_s(&buf, &n, "FMDG_TEST_PG") != 0 || buf == nullptr)
        return {};
    std::string v(buf);
    std::free(buf);
    return v;
#else
    const char* v = std::getenv("FMDG_TEST_PG");
    return v ? v : "";
#endif
}

std::int64_t minute_ns(int minute) { // 2026-11-04 19:00 UTC + minute
    return (1'793'818'800LL + 60LL * minute) * 1'000'000'000LL;
}

alpaca::Bar bar(const std::string& sym, int minute, double close, bool updated = false) {
    alpaca::Bar b;
    b.symbol = sym;
    b.start_ns = minute_ns(minute);
    b.open = close - 1;
    b.high = close + 1;
    b.low = close - 2;
    b.close = close;
    b.volume = 1000 + minute;
    b.updated = updated;
    return b;
}

bool eventually(const std::function<bool()>& done, std::chrono::milliseconds limit = 10s) {
    const auto end = std::chrono::steady_clock::now() + limit;
    while (!done()) {
        if (std::chrono::steady_clock::now() > end)
            return false;
        std::this_thread::sleep_for(5ms);
    }
    return true;
}

class BarStoreTest : public ::testing::Test {
  protected:
    void SetUp() override {
        conninfo_ = test_conninfo();
        if (conninfo_.empty())
            GTEST_SKIP() << "set FMDG_TEST_PG to a libpq connection string to run the PostgreSQL tests";
        static std::atomic<int> counter{0};
        table_ = "test_bars_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() % 1000000) +
                 "_" + std::to_string(counter++);
        admin_ = pg::Connection::open(conninfo_);
    }
    void TearDown() override {
        if (admin_)
            admin_->exec("DROP TABLE IF EXISTS " + table_);
    }

    BarStoreConfig config() {
        BarStoreConfig c;
        c.conninfo = conninfo_;
        c.table = table_;
        c.flush_interval = 50ms;
        c.retry_initial = 50ms;
        c.retry_max = 200ms;
        return c;
    }
    std::int64_t count() { return admin_->exec("SELECT count(*) FROM " + table_).integer(0, 0); }

    std::string conninfo_, table_;
    std::unique_ptr<pg::Connection> admin_;
};

} // namespace

TEST_F(BarStoreTest, ConnectionRunsParameterizedQueries) {
    const auto r = admin_->exec("SELECT $1::int + $2::int, $3::text IS NULL, $4::text", {"40", "2", std::nullopt,
                                                                                           "it's; DROP TABLE x"});
    EXPECT_EQ(r.integer(0, 0), 42);
    EXPECT_EQ(r.text(0, 1), "t");
    EXPECT_EQ(r.text(0, 2), "it's; DROP TABLE x"); // a value, never SQL
    EXPECT_TRUE(admin_->healthy());
    try {
        admin_->exec("SELECT * FROM no_such_table_here");
        FAIL() << "expected an error";
    } catch (const pg::Error& e) {
        EXPECT_EQ(e.sqlstate(), "42P01"); // undefined_table
        EXPECT_FALSE(e.connection_lost());
    }
    EXPECT_TRUE(admin_->healthy()); // still usable after a statement error
}

TEST_F(BarStoreTest, WritesBarsAndLetsCorrectionsWin) {
    BarStore store(config());
    store.start();
    store.add(bar("AAPL", 15, 150.0));
    store.add(bar("AAPL", 15, 151.25, true)); // late correction, same minute, same batch
    store.add(bar("MSFT", 15, 400.5));
    store.stop();

    EXPECT_EQ(count(), 2);
    const auto r = admin_->exec("SELECT close, volume, extract(epoch FROM start_ts)::bigint FROM " + table_ +
                                " WHERE symbol = $1",
                                {"AAPL"});
    ASSERT_EQ(r.rows(), 1);
    EXPECT_DOUBLE_EQ(r.number(0, 0), 151.25);
    EXPECT_DOUBLE_EQ(r.number(0, 1), 1015.0);
    EXPECT_EQ(r.integer(0, 2), minute_ns(15) / 1'000'000'000);
    const BarStoreStats st = store.stats();
    EXPECT_EQ(st.received, 3u);
    EXPECT_EQ(st.written, 2u);
    EXPECT_EQ(st.batches, 1u);
    EXPECT_EQ(st.queued, 0u);

    // A correction arriving in a later batch overwrites the stored bar too.
    BarStore again(config());
    again.start();
    again.add(bar("MSFT", 15, 401.0, true));
    again.stop();
    EXPECT_DOUBLE_EQ(admin_->exec("SELECT close FROM " + table_ + " WHERE symbol = 'MSFT'").number(0, 0), 401.0);
    EXPECT_EQ(count(), 2);
}

TEST_F(BarStoreTest, BatchesABurst) {
    BarStoreConfig c = config();
    c.flush_interval = 10s; // only full batches, until stop() flushes the rest
    BarStore store(c);
    store.start();
    for (int i = 0; i < 2500; ++i)
        store.add(bar("SYM" + std::to_string(i % 50), i / 50, 100.0 + i));
    ASSERT_TRUE(eventually([&] { return store.stats().batches == 2; }));
    store.stop();
    EXPECT_EQ(count(), 2500);
    EXPECT_EQ(store.stats().batches, 3u); // 1000 + 1000 + 500
    EXPECT_EQ(store.stats().written, 2500u);
}

TEST_F(BarStoreTest, FlushesAPartialBatchAfterTheInterval) {
    BarStore store(config()); // flush_interval 50 ms
    store.start();
    store.add(bar("AAPL", 1, 150.0));
    ASSERT_TRUE(eventually([&] { return store.stats().written == 1; }, 3s));
    EXPECT_EQ(count(), 1);
}

TEST_F(BarStoreTest, AKilledConnectionIsDiscardedAndTheBatchRetried) {
    BarStore store(config());
    store.start();
    store.add(bar("AAPL", 1, 150.0));
    ASSERT_TRUE(eventually([&] { return store.stats().written == 1; }));

    // Kill the store's server session behind its back, as a restart or a network
    // drop would. The pooled connection is now dead but still looks idle.
    // The victims are listed first (MATERIALIZED) and only then terminated: SQL
    // does not order AND, and terminating before "pid <> pg_backend_pid()" is
    // checked would kill this very session.
    const auto killed = admin_->exec("WITH victims AS MATERIALIZED (SELECT pid FROM pg_stat_activity "
                                     "WHERE usename = current_user AND pid <> pg_backend_pid()) "
                                     "SELECT count(*) FROM victims WHERE pg_terminate_backend(pid)");
    ASSERT_GE(killed.integer(0, 0), 1);

    store.add(bar("AAPL", 2, 151.0));
    ASSERT_TRUE(eventually([&] { return store.stats().written == 2; })) << store.stats().last_error;
    store.stop();
    EXPECT_EQ(count(), 2);
    const BarStoreStats st = store.stats();
    EXPECT_GE(st.failed_batches, 1u); // the first attempt hit the dead connection
    EXPECT_GE(st.pool.discarded, 1u); // which was closed, not handed out again
}

TEST_F(BarStoreTest, KeepsBarsWhileTheDatabaseIsUnreachable) {
    BarStoreConfig c = config();
    c.conninfo = "host=127.0.0.1 port=1 dbname=fmdg user=fmdg connect_timeout=1"; // nothing listens on port 1
    c.queue_max = 2;
    BarStore store(c);
    store.start();
    store.add(bar("AAPL", 1, 150.0));
    store.add(bar("AAPL", 2, 151.0));
    store.add(bar("AAPL", 3, 152.0)); // over queue_max: the oldest goes
    ASSERT_TRUE(eventually([&] { return store.stats().failed_batches >= 1; }));
    const BarStoreStats st = store.stats();
    EXPECT_EQ(st.queued, 2u);
    EXPECT_EQ(st.dropped, 1u);
    EXPECT_NE(st.last_error.find("cannot connect"), std::string::npos) << st.last_error;
    const auto t0 = std::chrono::steady_clock::now();
    store.stop(); // one more attempt, then gives up: shutdown is not held hostage
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 5s);
}

TEST(BarStoreConfig, RejectsAnUnsafeTableName) {
    BarStoreConfig c;
    c.conninfo = "host=127.0.0.1 port=1";
    c.table = "bars; DROP TABLE bars_1m";
    EXPECT_THROW(BarStore{c}, std::invalid_argument);
}
