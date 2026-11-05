// gateway/tests/test_bar_sync.cpp — BarSync against a fake Alpaca REST server
// on loopback (plain HTTP). Each test scripts the server's answers and checks
// what the sync asked for and what it did with the bars.

#include "fmdg/bar_sync.h"

#include <gtest/gtest.h>

#include <asio.hpp>

#include <atomic>
#include <chrono>
#include <deque>
#include <format>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

using namespace fmdg;
using namespace std::chrono_literals;
using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;

namespace {

constexpr std::int64_t kNow = 1'793'820'605; // 2026-11-04 19:30:05 UTC

struct Reply {
    int status = 200;
    std::string body;
    std::string extra_headers;     ///< e.g. "Retry-After: 1\r\n"
    bool close_silently = false;   ///< drop the connection after replying, without "Connection: close"
};

std::string page(const std::string& sym, int minute, const std::string& token) {
    const std::string t = std::format("2026-11-04T19:{:02}:00Z", minute);
    return std::format(R"({{"bars":{{"{}":[{{"t":"{}","o":1,"h":2,"l":0.5,"c":1.5,"v":10,"n":1,"vw":1.2}}]}},)"
                       R"("next_page_token":{}}})",
                       sym, t, token.empty() ? "null" : "\"" + token + "\"");
}

/// Answers GETs from a script; once the script runs out, repeats its last reply.
class FakeRest {
  public:
    explicit FakeRest(std::function<Reply(const std::string& target, int n)> script)
        : script_(std::move(script)), acceptor_(io_, {asio::ip::make_address("127.0.0.1"), 0}) {
        asio::co_spawn(io_, accept_loop(), asio::detached);
        thread_ = std::thread([this] { io_.run(); });
    }
    ~FakeRest() {
        io_.stop();
        thread_.join();
    }
    std::string port() const { return std::to_string(acceptor_.local_endpoint().port()); }
    int connections() const { return connections_.load(); }
    std::vector<std::string> targets() const {
        std::lock_guard lock(mu_);
        return targets_;
    }
    std::vector<std::string> keys() const {
        std::lock_guard lock(mu_);
        return keys_;
    }
    std::vector<std::chrono::steady_clock::time_point> times() const {
        std::lock_guard lock(mu_);
        return times_;
    }

  private:
    awaitable<void> accept_loop() {
        for (;;) {
            tcp::socket s = co_await acceptor_.async_accept(use_awaitable);
            ++connections_;
            asio::co_spawn(io_, serve(std::move(s)), asio::detached);
        }
    }
    awaitable<void> serve(tcp::socket s) {
        try {
            std::string buf;
            for (;;) {
                const std::size_t n = co_await asio::async_read_until(s, asio::dynamic_buffer(buf), "\r\n\r\n",
                                                                      use_awaitable);
                const std::string head = buf.substr(0, n);
                buf.erase(0, n);
                const std::string target = head.substr(4, head.find(' ', 4) - 4);
                const auto k = head.find("APCA-API-KEY-ID: ");
                int index = 0;
                {
                    std::lock_guard lock(mu_);
                    targets_.push_back(target);
                    keys_.push_back(k == std::string::npos ? "" : head.substr(k + 17, head.find("\r\n", k) - k - 17));
                    times_.push_back(std::chrono::steady_clock::now());
                    index = static_cast<int>(targets_.size()) - 1;
                }
                const Reply r = script_(target, index);
                const std::string out = std::format("HTTP/1.1 {} X\r\nContent-Type: application/json\r\n{}"
                                                    "Content-Length: {}\r\n\r\n{}",
                                                    r.status, r.extra_headers, r.body.size(), r.body);
                co_await asio::async_write(s, asio::buffer(out), use_awaitable);
                if (r.close_silently)
                    co_return; // the socket closes; the client's next request on it fails
            }
        } catch (const std::exception&) {
        }
    }

    std::function<Reply(const std::string&, int)> script_;
    asio::io_context io_;
    tcp::acceptor acceptor_;
    std::thread thread_;
    std::atomic<int> connections_{0};
    mutable std::mutex mu_;
    std::vector<std::string> targets_, keys_;
    std::vector<std::chrono::steady_clock::time_point> times_;
};

struct Collected {
    std::mutex mu;
    std::vector<alpaca::Bar> bars;
    std::uint64_t seq = 0;
    std::atomic<std::uint64_t> written{~std::uint64_t{0}}; ///< what written_through reports
};

BarSyncConfig config(const FakeRest& server, Collected& out) {
    BarSyncConfig c;
    c.host = "127.0.0.1";
    c.port = server.port();
    c.tls = false;
    c.key = "test-key";
    c.secret = "test-secret";
    c.symbols = {"AAPL", "MSFT"};
    c.days = 1;
    c.interval = 1h; // no top-up unless a test asks
    c.offset = 0ms;
    c.max_requests_per_minute = 6000;
    c.request_timeout = 2s;
    c.retry_initial = 50ms;
    c.retry_max = 200ms;
    c.now = [] { return kNow; };
    c.on_bars = [&out](const std::vector<alpaca::Bar>& bars) {
        std::lock_guard lock(out.mu);
        out.bars.insert(out.bars.end(), bars.begin(), bars.end());
        out.seq += bars.size();
        return out.seq;
    };
    c.written_through = [&out] { return out.written.load(); };
    return c;
}

bool eventually(const std::function<bool()>& done, std::chrono::milliseconds limit = 5s) {
    const auto end = std::chrono::steady_clock::now() + limit;
    while (!done()) {
        if (std::chrono::steady_clock::now() > end)
            return false;
        std::this_thread::sleep_for(2ms);
    }
    return true;
}

} // namespace

TEST(BarSync, BackfillsEveryPageOverOneConnection) {
    FakeRest server([](const std::string& target, int) {
        if (target.find("page_token=p2") != std::string::npos)
            return Reply{200, page("MSFT", 29, "")};
        return Reply{200, page("AAPL", 28, "p2")};
    });
    Collected out;
    BarSync sync(config(server, out));
    sync.start();
    ASSERT_TRUE(eventually([&] { return sync.complete_to() != 0; })) << sync.stats().last_error;

    const auto targets = server.targets();
    ASSERT_EQ(targets.size(), 2u);
    // A day back from 19:30, through the last finished minute (19:29:59, inclusive).
    EXPECT_EQ(targets[0], "/v2/stocks/bars?symbols=AAPL%2CMSFT&timeframe=1Min&start=2026-11-03T19%3A30%3A00Z"
                          "&end=2026-11-04T19%3A29%3A59Z&limit=10000&feed=iex&adjustment=raw&sort=asc");
    EXPECT_NE(targets[1].find("&page_token=p2"), std::string::npos);
    EXPECT_EQ(server.keys()[0], "test-key");
    EXPECT_EQ(server.connections(), 1); // kept alive across pages
    EXPECT_EQ(sync.complete_to(), 1'793'820'600); // 19:30:00
    std::lock_guard lock(out.mu);
    ASSERT_EQ(out.bars.size(), 2u);
    EXPECT_EQ(out.bars[1].symbol, "MSFT");
    EXPECT_EQ(sync.stats().bars, 2u);
}

TEST(BarSync, ResumesWhereTheStoreEnds) {
    FakeRest server([](const std::string&, int) { return Reply{200, page("AAPL", 29, "")}; });
    Collected out;
    BarSyncConfig c = config(server, out);
    c.resume_from = [](const std::vector<std::string>&) { return std::int64_t{1'793'820'000}; }; // 19:20
    BarSync sync(c);
    sync.start();
    ASSERT_TRUE(eventually([&] { return sync.complete_to() != 0; }));
    EXPECT_NE(server.targets()[0].find("&start=2026-11-04T19%3A20%3A00Z"), std::string::npos);
}

TEST(BarSync, TopUpsRereadTheOverlap) {
    FakeRest server([](const std::string&, int) { return Reply{200, page("AAPL", 29, "")}; });
    Collected out;
    BarSyncConfig c = config(server, out);
    c.interval = 50ms;
    std::atomic<int> calls{0};
    c.now = [&calls] { return kNow + 60 * calls++; }; // a new minute on every look
    BarSync sync(c);
    sync.start();
    ASSERT_TRUE(eventually([&] { return server.targets().size() >= 3; }));
    const auto targets = server.targets();
    // Backfill ended before 19:30:00; the first top-up re-reads from 19:28:00
    // (two minutes of overlap) to the end of 19:30.
    EXPECT_NE(targets[1].find("&start=2026-11-04T19%3A28%3A00Z&end=2026-11-04T19%3A30%3A59Z"), std::string::npos)
        << targets[1];
    EXPECT_GE(sync.complete_to(), 1'793'820'660);
}

TEST(BarSync, ARateLimitWaitsForRetryAfter) {
    FakeRest server([](const std::string&, int n) {
        if (n == 0)
            return Reply{429, R"({"message":"too many requests"})", "Retry-After: 1\r\n"};
        return Reply{200, page("AAPL", 29, "")};
    });
    Collected out;
    BarSync sync(config(server, out));
    sync.start();
    ASSERT_TRUE(eventually([&] { return sync.complete_to() != 0; }));
    const auto t = server.times();
    ASSERT_EQ(t.size(), 2u);
    EXPECT_GE(t[1] - t[0], 950ms);
    EXPECT_EQ(sync.stats().rate_limited, 1u);
}

TEST(BarSync, ServerErrorsAreRetried) {
    FakeRest server([](const std::string&, int n) {
        if (n < 2)
            return Reply{503, "upstream unavailable"};
        return Reply{200, page("AAPL", 29, "")};
    });
    Collected out;
    BarSync sync(config(server, out));
    sync.start();
    ASSERT_TRUE(eventually([&] { return sync.complete_to() != 0; }));
    EXPECT_EQ(sync.stats().retries, 2u);
    EXPECT_EQ(server.targets().size(), 3u);
}

TEST(BarSync, ABadKeyStopsForGood) {
    FakeRest server([](const std::string&, int) { return Reply{401, R"({"message":"forbidden."})"}; });
    Collected out;
    BarSync sync(config(server, out));
    sync.start();
    ASSERT_TRUE(eventually([&] { return sync.stats().state == "stopped"; }));
    std::this_thread::sleep_for(300ms);
    EXPECT_EQ(server.targets().size(), 1u); // no retries
    EXPECT_NE(sync.stats().last_error.find("HTTP 401"), std::string::npos);
    EXPECT_EQ(sync.complete_to(), 0);
    EXPECT_EQ(sync.stats().last_error.find("test-secret"), std::string::npos);
}

TEST(BarSync, CompleteToWaitsUntilTheStoreHasWrittenTheBars) {
    FakeRest server([](const std::string&, int) { return Reply{200, page("AAPL", 29, "")}; });
    Collected out;
    out.written = 0; // the store has written nothing yet
    BarSync sync(config(server, out));
    sync.start();
    ASSERT_TRUE(eventually([&] { return sync.stats().bars == 1; }));
    std::this_thread::sleep_for(50ms);
    EXPECT_EQ(sync.complete_to(), 0); // fetched, queued, not yet in the database
    out.written = 1;
    EXPECT_EQ(sync.complete_to(), 1'793'820'600);
}

TEST(BarSync, AKeepAliveConnectionTheServerDroppedIsReopened) {
    FakeRest server([](const std::string& target, int) {
        // Each reply ends the connection without saying so.
        if (target.find("page_token=p2") != std::string::npos)
            return Reply{200, page("MSFT", 29, ""), "", true};
        return Reply{200, page("AAPL", 28, "p2"), "", true};
    });
    Collected out;
    BarSync sync(config(server, out));
    sync.start();
    ASSERT_TRUE(eventually([&] { return sync.complete_to() != 0; })) << sync.stats().last_error;
    EXPECT_EQ(server.connections(), 2);
    EXPECT_EQ(sync.stats().retries, 0u); // the transparent reconnect is not a failure
}

TEST(BarSync, StopsFetchingWhileTheStoreIsBehind) {
    FakeRest server([](const std::string& target, int) {
        if (target.find("page_token=p3") != std::string::npos)
            return Reply{200, page("AAPL", 29, "")};
        if (target.find("page_token=p2") != std::string::npos)
            return Reply{200, page("AAPL", 28, "p3")};
        return Reply{200, page("AAPL", 27, "p2")};
    });
    Collected out;
    BarSyncConfig c = config(server, out);
    std::atomic<std::size_t> backlog{1'000'000}; // the store is far behind
    c.backlog = [&backlog] { return backlog.load(); };
    c.max_backlog = 100;
    BarSync sync(c);
    sync.start();
    ASSERT_TRUE(eventually([&] { return sync.stats().bars == 1; }));
    std::this_thread::sleep_for(200ms);
    EXPECT_EQ(server.targets().size(), 1u); // no second page while the backlog is high
    backlog = 60;                            // above half the limit: still waiting
    std::this_thread::sleep_for(150ms);
    EXPECT_EQ(server.targets().size(), 1u);
    backlog = 10; // caught up
    ASSERT_TRUE(eventually([&] { return sync.complete_to() != 0; }));
    EXPECT_EQ(server.targets().size(), 3u);
    EXPECT_GE(sync.stats().backpressure_seconds, 0.3);
}
