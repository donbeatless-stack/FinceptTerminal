// gateway/tests/test_alpaca_feed.cpp — AlpacaFeed end to end against a fake
// Alpaca server on loopback (plain TCP; TLS is the one layer only the live test
// covers). Each connection the server accepts follows a script, so every test
// pins one failure mode: a dropped link, bad credentials, the one-connection
// limit (406), a server that never answers the handshake, a dead link that only
// pings can detect, a quiet link pings keep alive, and a server-initiated close.

#include "fmdg/alpaca_feed.h"
#include "fmdg/gateway.h"
#include "fmdg/protocol.h"
#include "fmdg/ws_codec.h"

#include <gtest/gtest.h>

#include <asio.hpp>

#include <atomic>
#include <chrono>
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

/// What the fake server does on one connection.
struct Script {
    bool answer_handshake = true;
    int auth_error = 0;                       ///< reply to auth with this error code instead of "authenticated"
    std::vector<std::string> after_subscribe; ///< text messages sent after the subscription reply
    bool answer_pings = true;
    bool close_after = false; ///< then send a Close frame (1001 "going away")
    bool drop_after = false;  ///< then drop TCP without a Close
    int drop_after_messages = 0; ///< drop TCP once the client has sent this many text messages
};

std::string server_frame(std::uint8_t first_byte, std::string_view payload) {
    std::string f(1, static_cast<char>(first_byte));
    const std::size_t n = payload.size();
    if (n < 126) {
        f.push_back(static_cast<char>(n));
    } else {
        f.push_back(static_cast<char>(126));
        f.push_back(static_cast<char>(n >> 8));
        f.push_back(static_cast<char>(n & 0xFF));
    }
    f.append(payload);
    return f;
}

std::string trade(std::string_view sym, double price, int size, std::string_view ts) {
    return std::format(R"([{{"T":"t","i":1,"S":"{}","x":"V","p":{},"s":{},"t":"{}","c":["@"],"z":"C"}}])", sym, price,
                       size, ts);
}

class FakeAlpaca {
  public:
    explicit FakeAlpaca(std::vector<Script> scripts)
        : scripts_(std::move(scripts)), acceptor_(io_, {asio::ip::make_address("127.0.0.1"), 0}) {
        asio::co_spawn(io_, accept_loop(), asio::detached);
        thread_ = std::thread([this] { io_.run(); });
    }
    ~FakeAlpaca() {
        io_.stop();
        thread_.join();
    }

    std::uint16_t port() const { return acceptor_.local_endpoint().port(); }
    int connections() const { return connections_.load(); }
    int pings() const { return pings_.load(); }
    int closes() const { return closes_.load(); }
    bool all_client_frames_masked() const { return !unmasked_.load(); }
    std::vector<std::string> received() const {
        std::lock_guard lock(mu_);
        return received_;
    }
    std::vector<std::chrono::steady_clock::time_point> accept_times() const {
        std::lock_guard lock(mu_);
        return accept_times_;
    }

  private:
    struct ClientFrame {
        std::uint8_t op = 0;
        std::string payload;
    };

    awaitable<void> accept_loop() {
        for (int i = 0;; ++i) {
            tcp::socket s = co_await acceptor_.async_accept(use_awaitable);
            {
                std::lock_guard lock(mu_);
                accept_times_.push_back(std::chrono::steady_clock::now());
            }
            ++connections_;
            const Script& sc = scripts_[std::min<std::size_t>(i, scripts_.size() - 1)];
            asio::co_spawn(io_, serve(std::move(s), sc), asio::detached);
        }
    }

    awaitable<ClientFrame> read_frame(tcp::socket& s) {
        std::array<std::uint8_t, 2> h{};
        co_await asio::async_read(s, asio::buffer(h), use_awaitable);
        ClientFrame f;
        f.op = h[0] & 0x0F;
        if (!(h[1] & 0x80))
            unmasked_ = true;
        std::uint64_t len = h[1] & 0x7F;
        if (len == 126) {
            std::array<std::uint8_t, 2> e{};
            co_await asio::async_read(s, asio::buffer(e), use_awaitable);
            len = (std::uint64_t{e[0]} << 8) | e[1];
        } else if (len == 127) {
            std::array<std::uint8_t, 8> e{};
            co_await asio::async_read(s, asio::buffer(e), use_awaitable);
            len = 0;
            for (auto b : e)
                len = (len << 8) | b;
        }
        std::array<std::uint8_t, 4> mask{};
        if (h[1] & 0x80)
            co_await asio::async_read(s, asio::buffer(mask), use_awaitable);
        f.payload.resize(static_cast<std::size_t>(len));
        co_await asio::async_read(s, asio::buffer(f.payload), use_awaitable);
        for (std::size_t i = 0; i < f.payload.size(); ++i)
            f.payload[i] = static_cast<char>(static_cast<std::uint8_t>(f.payload[i]) ^ mask[i % 4]);
        co_return f;
    }

    awaitable<void> serve(tcp::socket s, Script sc) {
        try {
            std::string head;
            co_await asio::async_read_until(s, asio::dynamic_buffer(head), "\r\n\r\n", use_awaitable);
            if (!sc.answer_handshake) {
                char c;
                co_await asio::async_read(s, asio::buffer(&c, 1), use_awaitable); // until the client gives up
                co_return;
            }
            const std::string marker = "Sec-WebSocket-Key: ";
            const std::size_t at = head.find(marker) + marker.size();
            const std::string key = head.substr(at, head.find("\r\n", at) - at);
            const std::string reply = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                                      "Connection: Upgrade\r\nSec-WebSocket-Accept: " +
                                      ws::accept_key(key) + "\r\n\r\n";
            co_await asio::async_write(s, asio::buffer(reply), use_awaitable);
            co_await send_text(s, R"([{"T":"success","msg":"connected"}])");
            int texts = 0;

            for (;;) {
                const ClientFrame f = co_await read_frame(s);
                if (f.op == 0x9) {
                    ++pings_;
                    if (sc.answer_pings)
                        co_await asio::async_write(s, asio::buffer(server_frame(0x8A, f.payload)), use_awaitable);
                    continue;
                }
                if (f.op == 0x8) {
                    ++closes_;
                    co_await asio::async_write(s, asio::buffer(server_frame(0x88, f.payload)), use_awaitable);
                    co_return; // the socket closes with this frame
                }
                {
                    std::lock_guard lock(mu_);
                    received_.push_back(f.payload);
                }
                ++texts;
                if (f.payload.rfind(R"({"action":"auth")", 0) == 0) {
                    if (sc.auth_error)
                        co_await send_text(s, std::format(R"([{{"T":"error","code":{},"msg":"scripted"}}])",
                                                          sc.auth_error));
                    else
                        co_await send_text(s, R"([{"T":"success","msg":"authenticated"}])");
                } else if (f.payload.rfind(R"({"action":"subscribe")", 0) == 0) {
                    co_await send_text(s, R"([{"T":"subscription","trades":["AAPL","MSFT"],"bars":["AAPL"]}])");
                    for (const auto& m : sc.after_subscribe)
                        co_await send_text(s, m);
                    if (sc.close_after)
                        co_await asio::async_write(
                            s, asio::buffer(server_frame(0x88, ws::close_payload(1001, "going away"))), use_awaitable);
                    if (sc.drop_after)
                        co_return;
                }
                if (sc.drop_after_messages && texts >= sc.drop_after_messages)
                    co_return;
            }
        } catch (const std::exception&) {
            // the client hung up
        }
    }

    awaitable<void> send_text(tcp::socket& s, std::string_view text) {
        co_await asio::async_write(s, asio::buffer(server_frame(0x81, text)), use_awaitable);
    }

    std::vector<Script> scripts_;
    asio::io_context io_;
    tcp::acceptor acceptor_;
    std::thread thread_;
    std::atomic<int> connections_{0}, pings_{0}, closes_{0};
    std::atomic<bool> unmasked_{false};
    mutable std::mutex mu_;
    std::vector<std::string> received_;
    std::vector<std::chrono::steady_clock::time_point> accept_times_;
};

/// Gateway + feed pointed at the fake server, with timings scaled down.
struct Harness {
    explicit Harness(FakeAlpaca& server, std::function<void(AlpacaFeedConfig&)> tweak = {}) : gateway(gateway_config()) {
        gateway.start();
        AlpacaFeedConfig c;
        c.host = "127.0.0.1";
        c.port = std::to_string(server.port());
        c.path = "/v2/test";
        c.tls = false;
        c.key = "test-key";
        c.secret = "test-secret";
        c.symbols = {"AAPL", "MSFT"};
        c.connect_timeout = 1s;
        c.ping_interval = 200ms;
        c.backoff_initial = 50ms;
        c.backoff_max = 200ms;
        c.backoff_connection_limit = 800ms;
        c.log = [this](const std::string& line) {
            std::lock_guard lock(mu);
            logs.push_back(line);
        };
        c.on_bar = [this](const alpaca::Bar& b) {
            std::lock_guard lock(mu);
            bars.push_back(b);
        };
        if (tweak)
            tweak(c);
        feed = std::make_unique<AlpacaFeed>(gateway, c);
        feed->start();
    }
    ~Harness() {
        feed->stop();
        gateway.stop();
    }

    static GatewayConfig gateway_config() {
        GatewayConfig g;
        g.push_port = 0;
        g.request_port = 0;
        g.io_threads = 1;
        return g;
    }

    bool logged(std::string_view needle) {
        std::lock_guard lock(mu);
        for (const auto& l : logs)
            if (l.find(needle) != std::string::npos)
                return true;
        return false;
    }
    std::string all_logs() {
        std::lock_guard lock(mu);
        std::string s;
        for (const auto& l : logs)
            s += "  " + l + "\n";
        return s;
    }

    Gateway gateway;
    std::unique_ptr<AlpacaFeed> feed;
    std::mutex mu;
    std::vector<std::string> logs;
    std::vector<alpaca::Bar> bars;
};

/// A push-port client that only sends: subscriptions are all these tests need.
struct PushClient {
    explicit PushClient(std::uint16_t port) : socket(io) {
        socket.connect({asio::ip::make_address("127.0.0.1"), port});
    }
    void send(proto::MsgType type, const std::vector<std::string>& symbols) {
        proto::Bytes b;
        proto::append_symbols(b, type, symbols);
        asio::write(socket, asio::buffer(b));
    }
    void close() {
        asio::error_code ignored;
        socket.shutdown(tcp::socket::shutdown_both, ignored);
        socket.close(ignored);
    }
    asio::io_context io;
    tcp::socket socket;
};

std::string sub_msg(std::string_view action, std::string_view list) {
    return std::format(R"({{"action":"{}","trades":[{}],"bars":[{}],"updatedBars":[{}]}})", action, list, list, list);
}

bool received(const FakeAlpaca& server, const std::string& message) {
    for (const auto& m : server.received())
        if (m == message)
            return true;
    return false;
}

bool wait_for(const std::function<bool()>& done, std::chrono::milliseconds limit = 5s) {
    const auto end = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < end) {
        if (done())
            return true;
        std::this_thread::sleep_for(5ms);
    }
    return done();
}

} // namespace

TEST(AlpacaFeed, StreamsTradesAndBarsIntoTheGateway) {
    FakeAlpaca server({Script{.after_subscribe = {
                                  trade("AAPL", 150.25, 10, "2026-11-04T19:15:02.123456789Z"),
                                  trade("MSFT", 400.5, 3, "2026-11-04T19:15:03Z"),
                                  R"([{"T":"b","S":"AAPL","o":150,"h":151,"l":149.5,"c":150.25,"v":1200,)"
                                  R"("t":"2026-11-04T19:14:00Z","n":12,"vw":150.1},{"T":"q","S":"AAPL"}])",
                              }}});
    Harness h(server);
    ASSERT_TRUE(wait_for([&] { return h.gateway.handle_command("SNAPSHOT MSFT") != "MSFT NA\n"; })) << h.all_logs();

    // Prices, sizes and exchange timestamps arrive intact.
    EXPECT_EQ(h.gateway.handle_command("SNAPSHOT AAPL").substr(0, 5), "AAPL ");
    const std::string aapl = h.gateway.handle_command("SNAPSHOT AAPL");
    EXPECT_NE(aapl.find(" 150.25 10 1793819702123456789"), std::string::npos) << aapl;
    EXPECT_NE(h.gateway.handle_command("SNAPSHOT MSFT").find(" 400.5 3 "), std::string::npos);

    ASSERT_TRUE(wait_for([&] { return h.feed->stats().bars == 1; }));
    const AlpacaFeedStats st = h.feed->stats();
    EXPECT_EQ(st.state, "streaming");
    EXPECT_EQ(st.connects, 1u);
    EXPECT_EQ(st.authenticated, 1u);
    EXPECT_EQ(st.trades, 2u);
    EXPECT_EQ(st.subscription, "trades=[AAPL,MSFT] bars=[AAPL]");
    {
        std::lock_guard lock(h.mu);
        ASSERT_EQ(h.bars.size(), 1u);
        EXPECT_EQ(h.bars[0].symbol, "AAPL");
        EXPECT_DOUBLE_EQ(h.bars[0].close, 150.25);
    }

    // What the client sent: auth, then the subscription, every frame masked.
    const auto sent = server.received();
    ASSERT_GE(sent.size(), 2u);
    EXPECT_EQ(sent[0], R"({"action":"auth","key":"test-key","secret":"test-secret"})");
    EXPECT_EQ(sent[1], R"({"action":"subscribe","trades":["AAPL","MSFT"],"bars":["AAPL","MSFT"],)"
                       R"("updatedBars":["AAPL","MSFT"]})");
    EXPECT_TRUE(server.all_client_frames_masked());
    // The secret never reaches the log.
    EXPECT_EQ(h.all_logs().find("test-secret"), std::string::npos);
}

TEST(AlpacaFeed, ReconnectsAndResubscribesAfterADroppedLink) {
    FakeAlpaca server({
        Script{.after_subscribe = {trade("AAPL", 100.0, 1, "2026-11-04T19:15:00Z")}, .drop_after = true},
        Script{.after_subscribe = {trade("AAPL", 101.0, 1, "2026-11-04T19:15:05Z")}},
    });
    Harness h(server);
    ASSERT_TRUE(wait_for([&] { return h.gateway.handle_command("SNAPSHOT AAPL").find(" 101 ") != std::string::npos; }))
        << h.all_logs();
    const AlpacaFeedStats st = h.feed->stats();
    EXPECT_EQ(server.connections(), 2);
    EXPECT_EQ(st.connects, 2u);
    EXPECT_EQ(st.authenticated, 2u);
    EXPECT_GE(st.disconnects, 1u);
    // Auth and subscribe again on the new connection.
    const auto sent = server.received();
    ASSERT_EQ(sent.size(), 4u);
    EXPECT_EQ(sent[2].rfind(R"({"action":"auth")", 0), 0u);
    EXPECT_EQ(sent[3].rfind(R"({"action":"subscribe")", 0), 0u);
}

TEST(AlpacaFeed, BadCredentialsStopForGood) {
    FakeAlpaca server({Script{.auth_error = 402}});
    Harness h(server);
    ASSERT_TRUE(wait_for([&] { return h.feed->stats().state == "stopped"; })) << h.all_logs();
    std::this_thread::sleep_for(800ms); // four times backoff_max: no retry may happen
    EXPECT_EQ(server.connections(), 1);
    EXPECT_NE(h.feed->stats().last_error.find("server error 402"), std::string::npos);
    EXPECT_TRUE(h.logged("stopped, not retrying"));
}

TEST(AlpacaFeed, ConnectionLimitWaitsLongerBeforeRetrying) {
    FakeAlpaca server({Script{.auth_error = 406}, Script{}});
    Harness h(server);
    ASSERT_TRUE(wait_for([&] { return h.feed->stats().state == "streaming"; })) << h.all_logs();
    const auto t = server.accept_times();
    ASSERT_EQ(t.size(), 2u);
    // backoff_connection_limit is 800 ms, jittered to 50-100 %; a normal retry would be ≤ 50 ms.
    EXPECT_GE(t[1] - t[0], 400ms);
    EXPECT_TRUE(h.logged("another session is using this account's stream"));
}

TEST(AlpacaFeed, AServerThatNeverAnswersTheHandshakeTimesOut) {
    FakeAlpaca server({Script{.answer_handshake = false}, Script{}});
    Harness h(server, [](AlpacaFeedConfig& c) { c.connect_timeout = 300ms; });
    ASSERT_TRUE(wait_for([&] { return h.feed->stats().state == "streaming"; })) << h.all_logs();
    EXPECT_EQ(server.connections(), 2);
    EXPECT_TRUE(h.logged("connect timed out"));
}

TEST(AlpacaFeed, ADeadLinkIsDetectedByPings) {
    // The first server stays connected but answers nothing after subscribing:
    // only the missing pongs reveal it.
    FakeAlpaca server({Script{.answer_pings = false}, Script{}});
    Harness h(server, [](AlpacaFeedConfig& c) { c.ping_interval = 100ms; });
    ASSERT_TRUE(wait_for([&] { return server.connections() == 2 && h.feed->stats().state == "streaming"; }))
        << h.all_logs();
    EXPECT_TRUE(h.logged("not even a pong"));
}

TEST(AlpacaFeed, AnsweredPingsKeepAQuietLinkUp) {
    FakeAlpaca server({Script{}});
    Harness h(server, [](AlpacaFeedConfig& c) { c.ping_interval = 50ms; });
    ASSERT_TRUE(wait_for([&] { return h.feed->stats().state == "streaming"; })) << h.all_logs();
    std::this_thread::sleep_for(600ms); // twelve ping intervals without a single data message
    EXPECT_EQ(server.connections(), 1) << h.all_logs();
    EXPECT_GE(server.pings(), 5);
    EXPECT_EQ(h.feed->stats().state, "streaming");
}

TEST(AlpacaFeed, AServerCloseIsEchoedAndFollowedByAReconnect) {
    FakeAlpaca server({Script{.close_after = true}, Script{}});
    Harness h(server);
    ASSERT_TRUE(wait_for([&] { return server.connections() == 2 && h.feed->stats().state == "streaming"; }))
        << h.all_logs();
    EXPECT_GE(server.closes(), 1); // the client answered the Close
    EXPECT_TRUE(h.logged("server closed the stream (1001: going away)"));
}

// ── following what push clients subscribe to ────────────────────────────────

TEST(UpstreamPlan, FixedSymbolsThenDemandedUpToTheCap) {
    // AAPL is fixed; clients want MSFT, TSLA, NVDA; cap 3 leaves room for two.
    const UpstreamPlan p = plan_upstream({"AAPL"}, {}, {"TSLA", "MSFT", "NVDA", "AAPL"}, 3);
    EXPECT_EQ(p.want, (std::vector<std::string>{"AAPL", "MSFT", "NVDA"})); // newcomers in name order
    EXPECT_EQ(p.add, p.want);
    EXPECT_TRUE(p.drop.empty());
    EXPECT_EQ(p.skipped, (std::vector<std::string>{"TSLA"}));
}

TEST(UpstreamPlan, SymbolsAlreadyStreamingKeepTheirPlace) {
    // TSLA streams already; AMD, a newcomer that sorts first, must not push it out.
    const UpstreamPlan p = plan_upstream({}, {"TSLA"}, {"AMD", "TSLA"}, 1);
    EXPECT_EQ(p.want, (std::vector<std::string>{"TSLA"}));
    EXPECT_TRUE(p.add.empty());
    EXPECT_TRUE(p.drop.empty());
    EXPECT_EQ(p.skipped, (std::vector<std::string>{"AMD"}));
}

TEST(UpstreamPlan, DropsWhatNobodyWantsButNeverAFixedSymbol) {
    const UpstreamPlan p = plan_upstream({"SPY"}, {"SPY", "TSLA", "AMD"}, {"AMD"}, 0);
    EXPECT_EQ(p.want, (std::vector<std::string>{"AMD", "SPY"}));
    EXPECT_TRUE(p.add.empty());
    EXPECT_EQ(p.drop, (std::vector<std::string>{"TSLA"}));
}

TEST(UpstreamPlan, OnlyStockSymbolsGoUpstreamAndFixedOnesIgnoreTheCap) {
    const UpstreamPlan p = plan_upstream({"A", "B", "C"}, {}, {"*", "D", "msft"}, 2);
    EXPECT_EQ(p.want, (std::vector<std::string>{"A", "B", "C"}));
    EXPECT_EQ(p.skipped, (std::vector<std::string>{"*", "D", "msft"}));
}

TEST(AlpacaFeed, FollowsWhatPushClientsSubscribeTo) {
    FakeAlpaca server({Script{}});
    Harness h(server); // AAPL and MSFT always
    ASSERT_TRUE(wait_for([&] { return h.feed->stats().state == "streaming"; })) << h.all_logs();

    PushClient c(h.gateway.push_port());
    c.send(proto::MsgType::Subscribe, {"TSLA", "MSFT", "*"});
    ASSERT_TRUE(wait_for([&] { return received(server, sub_msg("subscribe", R"("TSLA")")); })) << h.all_logs();
    EXPECT_EQ(h.feed->stats().upstream, 3u);
    EXPECT_EQ(h.feed->stats().skipped, 1u);
    EXPECT_TRUE(h.logged("not streaming [*]"));
    EXPECT_EQ(h.feed->status_line(), "FEED alpaca stream=test state=streaming on_demand=1 upstream=3 cap=0\n");

    c.send(proto::MsgType::Unsubscribe, {"TSLA"});
    ASSERT_TRUE(wait_for([&] { return received(server, sub_msg("unsubscribe", R"("TSLA")")); })) << h.all_logs();
    EXPECT_EQ(h.feed->stats().upstream, 2u);

    // A second client's symbol, then its disconnect: that symbol goes, the fixed MSFT stays.
    PushClient d(h.gateway.push_port());
    d.send(proto::MsgType::Subscribe, {"NVDA", "MSFT"});
    ASSERT_TRUE(wait_for([&] { return received(server, sub_msg("subscribe", R"("NVDA")")); })) << h.all_logs();
    d.close();
    ASSERT_TRUE(wait_for([&] { return received(server, sub_msg("unsubscribe", R"("NVDA")")); })) << h.all_logs();
    c.close();
    std::this_thread::sleep_for(100ms);
    for (const auto& m : server.received()) {
        if (m.rfind(R"({"action":"unsubscribe")", 0) == 0) {
            EXPECT_EQ(m.find("MSFT"), std::string::npos) << m;
            EXPECT_EQ(m.find("AAPL"), std::string::npos) << m;
        }
        EXPECT_EQ(m.find(R"("*")"), std::string::npos) << m; // never passed upstream
    }
}

TEST(AlpacaFeed, AReconnectSubscribesToWhatClientsStillWant) {
    // The first connection drops right after the client's TSLA subscription went upstream.
    FakeAlpaca server({Script{.drop_after_messages = 3}, Script{}});
    Harness h(server);
    ASSERT_TRUE(wait_for([&] { return h.feed->stats().state == "streaming"; })) << h.all_logs();
    PushClient c(h.gateway.push_port());
    c.send(proto::MsgType::Subscribe, {"TSLA"});
    ASSERT_TRUE(wait_for([&] { return server.connections() == 2 && h.feed->stats().authenticated == 2; }))
        << h.all_logs();
    // The new connection asks for everything at once, the client's symbol included.
    ASSERT_TRUE(wait_for([&] { return received(server, sub_msg("subscribe", R"("AAPL","MSFT","TSLA")")); }))
        << h.all_logs();
}

TEST(AlpacaFeed, WithoutOnDemandClientsDoNotChangeTheSubscription) {
    FakeAlpaca server({Script{}});
    Harness h(server, [](AlpacaFeedConfig& c) { c.on_demand = false; });
    ASSERT_TRUE(wait_for([&] { return h.feed->stats().state == "streaming"; })) << h.all_logs();
    PushClient c(h.gateway.push_port());
    c.send(proto::MsgType::Subscribe, {"TSLA"});
    std::this_thread::sleep_for(300ms);
    EXPECT_EQ(server.received().size(), 2u); // auth and the fixed subscription only
    EXPECT_EQ(h.feed->status_line(), "FEED alpaca stream=test state=streaming on_demand=0 upstream=2 cap=0\n");
}

TEST(AlpacaFeed, WithNoFixedSymbolsItWaitsForClients) {
    FakeAlpaca server({Script{}});
    Harness h(server, [](AlpacaFeedConfig& c) { c.symbols.clear(); });
    ASSERT_TRUE(wait_for([&] { return h.feed->stats().authenticated == 1; })) << h.all_logs();
    EXPECT_EQ(h.feed->stats().state, "streaming");
    EXPECT_EQ(server.received().size(), 1u); // auth; nothing to subscribe to yet
    PushClient c(h.gateway.push_port());
    c.send(proto::MsgType::Subscribe, {"SPY"});
    ASSERT_TRUE(wait_for([&] { return received(server, sub_msg("subscribe", R"("SPY")")); })) << h.all_logs();
}
