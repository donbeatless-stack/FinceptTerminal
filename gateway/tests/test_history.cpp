// gateway/tests/test_history.cpp — HISTORY (fmdg/history.h): parsing always;
// aggregation and the request-port path against a real PostgreSQL when
// FMDG_TEST_PG is set (see test_bar_store.cpp), skipped otherwise.

#include "fmdg/bar_store.h"
#include "fmdg/gateway.h"
#include "fmdg/history.h"

#include <gtest/gtest.h>

#include <asio.hpp>

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <string>
#include <thread>
#include <vector>

using namespace fmdg;
using namespace std::chrono_literals;

namespace {

constexpr std::int64_t kBase = 1'793'818'800; // 2026-11-04 19:00:00 UTC, a 15-minute boundary

std::string conninfo_from_env() {
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

std::string request(std::uint16_t port, const std::string& line) {
    asio::io_context io;
    asio::ip::tcp::socket s(io);
    s.connect({asio::ip::make_address("127.0.0.1"), port});
    asio::write(s, asio::buffer(line + "\n"));
    std::string out;
    asio::error_code ec;
    asio::read(s, asio::dynamic_buffer(out), ec); // until the gateway closes
    return out;
}

} // namespace

// ── parsing ──────────────────────────────────────────────────────────────────

TEST(HistoryParse, ReadsTimeframeRangeAndSymbols) {
    std::string err;
    const auto r = parse_history("history 15m 1793818800 1793820600 AAPL MSFT", err);
    ASSERT_TRUE(r) << err;
    EXPECT_EQ(r->bucket_seconds, 900);
    EXPECT_EQ(r->from, kBase);
    EXPECT_EQ(r->to, kBase + 1800);
    EXPECT_EQ(r->symbols, (std::vector<std::string>{"AAPL", "MSFT"}));
}

TEST(HistoryParse, AlignsTheRangeToWholePeriods) {
    std::string err;
    // from 19:07 → the first whole 15-minute period starts at 19:15; to 19:31 → up to 19:45.
    const auto r = parse_history("HISTORY 15m 1793819220 1793820660 AAPL", err);
    ASSERT_TRUE(r) << err;
    EXPECT_EQ(r->from, kBase + 900);
    EXPECT_EQ(r->to, kBase + 2700);
}

TEST(HistoryParse, RejectsMalformedRequests) {
    std::string err;
    for (const char* bad : {"HISTORY", "HISTORY 15m 1 2", "HISTORY 2m 1 2 AAPL", "HISTORY 1d 1 2 AAPL",
                            "HISTORY 15m 5 5 AAPL", "HISTORY 15m x 9 AAPL", "HISTORY 15m -60 9 AAPL",
                            "HISTORY 15m 0 999999999 AAPL", "SNAPSHOT 15m 1 2 AAPL"}) {
        EXPECT_FALSE(parse_history(bad, err)) << bad;
        EXPECT_FALSE(err.empty()) << bad;
    }
    EXPECT_FALSE(parse_history("HISTORY 15m 1 2 " + std::string(40, 'A'), err)); // symbol too long
}

// ── against PostgreSQL ───────────────────────────────────────────────────────

class HistoryDb : public ::testing::Test {
  protected:
    void SetUp() override {
        conninfo_ = conninfo_from_env();
        if (conninfo_.empty())
            GTEST_SKIP() << "set FMDG_TEST_PG to run the PostgreSQL tests";
        static std::atomic<int> counter{0};
        table_ = "test_hist_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() % 1000000) +
                 "_" + std::to_string(counter++);
        BarStoreConfig c;
        c.conninfo = conninfo_;
        c.table = table_;
        c.pool.max_size = 2;
        c.flush_interval = 20ms;
        store_ = std::make_unique<BarStore>(c);
        store_->start();
        // AAPL: 32 one-minute bars from 19:00, with a spike at 19:05 and a dip at 19:20.
        for (int i = 0; i < 32; ++i) {
            alpaca::Bar b;
            b.symbol = "AAPL";
            b.start_ns = (kBase + 60LL * i) * 1'000'000'000LL;
            b.open = 100 + i;
            b.high = i == 5 ? 200 : 101 + i;
            b.low = i == 20 ? 50 : 99 + i;
            b.close = 100.5 + i;
            b.volume = 10 + i;
            store_->add(b);
        }
        alpaca::Bar m;
        m.symbol = "MSFT";
        m.start_ns = kBase * 1'000'000'000LL;
        m.open = m.high = m.low = m.close = 400.25;
        m.volume = 7;
        store_->add(m);
        const auto deadline = std::chrono::steady_clock::now() + 10s;
        while (store_->stats().written < 33 && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(5ms);
        ASSERT_EQ(store_->stats().written, 33u) << store_->stats().last_error;
    }
    void TearDown() override {
        if (!store_)
            return;
        store_->stop();
        store_->pool().acquire()->exec("DROP TABLE IF EXISTS " + table_);
    }
    std::string history(const std::string& line) {
        std::string err;
        const auto r = parse_history(line, err);
        EXPECT_TRUE(r) << err;
        return r ? run_history(store_->pool(), table_, *r) : "";
    }

    std::string conninfo_, table_;
    std::unique_ptr<BarStore> store_;
};

TEST_F(HistoryDb, AggregatesMinutesIntoWholePeriods) {
    // 19:00-19:15: open of 19:00, the 19:05 spike, low of 19:00, close of 19:14, Σ volume 10..24.
    // 19:15-19:30: open of 19:15, high of 19:29, the 19:20 dip, close of 19:29, Σ volume 25..39.
    // 19:30 starts at `to`: not included.
    EXPECT_EQ(history("HISTORY 15m 1793818800 1793820600 AAPL"), "BARS 2 0\n"
                                                                 "AAPL 1793818800 100 200 99 114.5 255\n"
                                                                 "AAPL 1793819700 115 130 50 129.5 480\n"
                                                                 "END\n");
}

TEST_F(HistoryDb, NeverReturnsPartOfAPeriod) {
    // From 19:07: the 19:00 period would be built from 8 of its 15 minutes, so it is skipped.
    EXPECT_EQ(history("HISTORY 15m 1793819220 1793820600 AAPL"), "BARS 1 0\n"
                                                                 "AAPL 1793819700 115 130 50 129.5 480\n"
                                                                 "END\n");
}

TEST_F(HistoryDb, OneMinuteBarsComeBackAsStored) {
    EXPECT_EQ(history("HISTORY 1m 1793818800 1793818980 AAPL"), "BARS 3 0\n"
                                                                "AAPL 1793818800 100 101 99 100.5 10\n"
                                                                "AAPL 1793818860 101 102 100 101.5 11\n"
                                                                "AAPL 1793818920 102 103 101 102.5 12\n"
                                                                "END\n");
}

TEST_F(HistoryDb, OrdersBySymbolAndSkipsUnknownOnes) {
    EXPECT_EQ(history("HISTORY 1h 1793818800 1793822400 ZZZZ MSFT AAPL"), "BARS 2 0\n"
                                                                          "AAPL 1793818800 100 200 50 131.5 816\n"
                                                                          "MSFT 1793818800 400.25 400.25 400.25 400.25 7\n"
                                                                          "END\n");
}

TEST_F(HistoryDb, ServesConcurrentRequestsThroughTheGatewayPort) {
    GatewayConfig gc;
    gc.push_port = 0;
    gc.request_port = 0;
    gc.command_threads = 4; // more workers than pooled connections (2)
    Gateway gw(gc);
    gw.add_command("HISTORY", history_command(store_->pool(), table_));
    gw.start();
    const std::string expected = history("HISTORY 15m 1793818800 1793820600 AAPL");
    std::atomic<int> wrong{0};
    std::vector<std::thread> clients;
    for (int t = 0; t < 8; ++t)
        clients.emplace_back([&] {
            for (int i = 0; i < 10; ++i)
                if (request(gw.request_port(), "HISTORY 15m 1793818800 1793820600 AAPL") != expected)
                    ++wrong;
        });
    for (auto& c : clients)
        c.join();
    EXPECT_EQ(wrong.load(), 0);
    EXPECT_EQ(request(gw.request_port(), "HISTORY 2m 1 2 AAPL").rfind("ERR timeframe", 0), 0u);
    EXPECT_LE(store_->pool().stats().created, 2u);
    gw.stop();
}

TEST_F(HistoryDb, WithASyncOnlySyncedSymbolsAreAnswered) {
    GatewayConfig gc;
    gc.push_port = 0;
    gc.request_port = 0;
    Gateway gw(gc);
    // MSFT is stored but not synced: complete_to does not vouch for it.
    gw.add_command("HISTORY", history_command(
                                  store_->pool(), table_, [] { return std::int64_t{1'793'822'400}; },
                                  [](std::string_view s) { return s == "AAPL"; }));
    gw.start();
    EXPECT_EQ(request(gw.request_port(), "HISTORY 1h 1793818800 1793822400 MSFT AAPL"),
              "BARS 1 1793822400\nAAPL 1793818800 100 200 50 131.5 816\nEND\n");
    gw.stop();
}
