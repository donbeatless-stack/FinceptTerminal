// gateway/tests/test_gateway.cpp — end-to-end over real loopback sockets.
#include "fmdg/gateway.h"
#include "fmdg/protocol.h"

#include <gtest/gtest.h>

#include <asio.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <thread>
#include <vector>

using namespace fmdg;
using namespace fmdg::proto;
using namespace std::chrono_literals;
using asio::ip::tcp;

namespace {

GatewayConfig test_config() {
    GatewayConfig c;
    c.push_port = 0; // any free port: tests run in parallel with anything else
    c.request_port = 0;
    c.io_threads = 2;
    return c;
}

struct Frame {
    MsgType type;
    Bytes body;
    std::uint16_t flags = 0;
};

/// A minimal push-port client. Reads run on a private io_context with a
/// deadline, so a test that expects a frame and gets none fails instead of hanging.
class Client {
  public:
    /// receive_buffer > 0 shrinks the socket's receive buffer, so a client that
    /// stops reading back-pressures the gateway after a few KB instead of a few MB.
    explicit Client(std::uint16_t port, int receive_buffer = 0) : socket_(io_) {
        socket_.open(tcp::v4());
        if (receive_buffer > 0)
            socket_.set_option(asio::socket_base::receive_buffer_size(receive_buffer));
        socket_.connect({asio::ip::make_address("127.0.0.1"), port});
        socket_.set_option(tcp::no_delay(true));
    }

    void send(const Bytes& b) { asio::write(socket_, asio::buffer(b)); }

    void subscribe(const std::vector<std::string>& syms) {
        Bytes b;
        append_symbols(b, MsgType::Subscribe, syms);
        send(b);
    }

    void unsubscribe(const std::vector<std::string>& syms) {
        Bytes b;
        append_symbols(b, MsgType::Unsubscribe, syms);
        send(b);
    }

    void heartbeat() {
        Bytes b;
        append_heartbeat(b);
        send(b);
    }

    /// The next frame, or nullopt on timeout or when the gateway closed the connection.
    std::optional<Frame> next(std::chrono::milliseconds timeout = 3000ms) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        for (;;) {
            Header h;
            std::span<const std::uint8_t> body;
            const auto r = reader_.next(h, body);
            if (r == FrameReader::Result::Frame)
                return Frame{h.type, Bytes(body.begin(), body.end()), h.flags};
            if (r == FrameReader::Result::Bad || closed_)
                return std::nullopt;
            bool done = false;
            asio::error_code ec;
            std::size_t n = 0;
            socket_.async_read_some(asio::buffer(buf_), [&](const asio::error_code& e, std::size_t k) {
                ec = e;
                n = k;
                done = true;
            });
            io_.restart();
            io_.run_until(deadline);
            if (!done) {
                socket_.cancel();
                io_.restart();
                io_.run();
                return std::nullopt; // timed out
            }
            if (ec) {
                closed_ = true;
                return std::nullopt;
            }
            reader_.feed(buf_.data(), n);
        }
    }

    TickMsg next_tick() {
        auto f = next();
        EXPECT_TRUE(f);
        if (!f)
            return {};
        EXPECT_EQ(f->type, MsgType::Tick);
        auto m = parse_tick(f->body);
        EXPECT_TRUE(m);
        if (m && (f->flags & kFlagSnapshot))
            m->tick.flags |= kTickSnapshot;
        return m.value_or(TickMsg{});
    }

    /// True once the gateway has closed the connection (draining what is left).
    bool closed_by_gateway(std::chrono::milliseconds timeout = 3000ms) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!closed_ && std::chrono::steady_clock::now() < deadline)
            if (!next(std::chrono::duration_cast<std::chrono::milliseconds>(deadline - std::chrono::steady_clock::now())))
                break;
        return closed_;
    }

  private:
    asio::io_context io_;
    tcp::socket socket_;
    std::array<std::uint8_t, 64 * 1024> buf_{};
    FrameReader reader_;
    bool closed_ = false;
};

/// One request-port round trip: send a line, read the reply until the gateway
/// closes. half_close: also end our side of the stream after the line, which is
/// how a client marks the end of a last line that has no '\n'.
std::string request(std::uint16_t port, std::string_view line, bool half_close = false) {
    asio::io_context io;
    tcp::socket s(io);
    s.connect({asio::ip::make_address("127.0.0.1"), port});
    asio::write(s, asio::buffer(line));
    if (half_close)
        s.shutdown(tcp::socket::shutdown_send);
    std::string out;
    bool done = false;
    asio::async_read(s, asio::dynamic_buffer(out), [&](const asio::error_code&, std::size_t) { done = true; });
    io.run_for(3s);
    return done ? out : "<timed out>";
}

bool eventually(const std::function<bool()>& pred, std::chrono::milliseconds limit = 3000ms) {
    const auto deadline = std::chrono::steady_clock::now() + limit;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline)
            return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

Tick tick_for(SymbolId id, double price) {
    Tick t;
    t.symbol = id;
    t.price = price;
    t.size = 100;
    t.exchange_ts_ns = 1'700'000'000'000'000'000;
    return t;
}

std::map<std::string, SymbolId> expect_subscribed(Client& c, std::size_t n) {
    auto f = c.next();
    EXPECT_TRUE(f);
    std::map<std::string, SymbolId> ids;
    if (!f)
        return ids;
    EXPECT_EQ(f->type, MsgType::Subscribed);
    const auto acks = parse_subscribed(f->body);
    EXPECT_TRUE(acks);
    if (acks)
        for (const auto& [id, name] : *acks)
            ids[name] = id;
    EXPECT_EQ(ids.size(), n);
    return ids;
}

ErrorCode expect_error_then_close(Client& c) {
    auto f = c.next();
    EXPECT_TRUE(f);
    if (!f)
        return ErrorCode{};
    EXPECT_EQ(f->type, MsgType::Error);
    const auto e = parse_error(f->body);
    EXPECT_TRUE(e);
    EXPECT_TRUE(c.closed_by_gateway()) << "the gateway closes after an error";
    return e ? e->code : ErrorCode{};
}

} // namespace

TEST(Gateway, RequestPortCommands) {
    Gateway gw(test_config());
    gw.start();
    const auto aapl = *gw.symbols().intern("AAPL");
    gw.ingest(tick_for(aapl, 189.25));
    ASSERT_TRUE(eventually([&] { return gw.stats().ticks_in == 1; }));

    EXPECT_EQ(request(gw.request_port(), "PING\n"), "PONG\n");
    EXPECT_EQ(request(gw.request_port(), "ping\r\n"), "PONG\n") << "case-insensitive, CRLF tolerated";
    EXPECT_EQ(request(gw.request_port(), "SNAPSHOT AAPL MSFT\n"),
              "AAPL 1 189.25 100 1700000000000000000\nMSFT NA\n");
    const std::string stats = request(gw.request_port(), "STATS\n");
    EXPECT_NE(stats.find("ticks_in=1\n"), std::string::npos) << stats;
    EXPECT_NE(stats.find("symbols=1\n"), std::string::npos) << stats;
    EXPECT_EQ(request(gw.request_port(), "PING", true), "PONG\n") << "a last line without newline still counts";
    EXPECT_EQ(request(gw.request_port(), std::string(5000, 'x')), "ERR request line longer than 4096 bytes\n");
}

TEST(Gateway, CommandParsingErrors) {
    Gateway gw(test_config());
    EXPECT_EQ(gw.handle_command(""), "ERR empty request\n");
    EXPECT_EQ(gw.handle_command("   "), "ERR empty request\n");
    EXPECT_EQ(gw.handle_command("DELETE *"), "ERR unknown command\n");
    EXPECT_EQ(gw.handle_command("SNAPSHOT"), "ERR usage: SNAPSHOT <symbol>...\n");
    EXPECT_EQ(gw.handle_command("SNAPSHOT " + std::string(40, 'A')), "ERR invalid symbol\n");
    EXPECT_EQ(gw.handle_command("SNAPSHOT   AAPL\t MSFT "), "AAPL NA\nMSFT NA\n");
    EXPECT_EQ(gw.symbols().size(), 0u) << "a snapshot request must not create symbols";
}

TEST(Gateway, SubscribeAckThenSnapshotThenLiveTicks) {
    Gateway gw(test_config());
    gw.start();
    const auto aapl = *gw.symbols().intern("AAPL");
    gw.ingest(tick_for(aapl, 100.0));
    ASSERT_TRUE(eventually([&] { return gw.stats().ticks_in == 1; }));

    Client c(gw.push_port());
    c.subscribe({"AAPL", "MSFT"});
    auto ids = expect_subscribed(c, 2);
    EXPECT_EQ(ids["AAPL"], aapl);

    const TickMsg snap = c.next_tick();
    EXPECT_EQ(snap.tick.symbol, aapl);
    EXPECT_EQ(snap.tick.price, 100.0);
    EXPECT_EQ(snap.tick.sequence, 1u);
    EXPECT_EQ(snap.tick.flags, kTickSnapshot) << "marked as a snapshot, not a live update";

    gw.ingest(tick_for(ids["MSFT"], 400.5));
    gw.ingest(tick_for(aapl, 101.0));
    const TickMsg m1 = c.next_tick();
    const TickMsg m2 = c.next_tick();
    EXPECT_EQ(m1.tick.symbol, ids["MSFT"]);
    EXPECT_EQ(m1.tick.price, 400.5);
    EXPECT_EQ(m1.tick.sequence, 2u);
    EXPECT_EQ(m2.tick.price, 101.0);
    EXPECT_EQ(m2.tick.sequence, 3u);
    EXPECT_GE(m2.send_ts_ns, m2.tick.ingest_ts_ns);
    EXPECT_EQ(m2.tick.exchange_ts_ns, 1'700'000'000'000'000'000);
    EXPECT_EQ(m1.tick.flags, 0);
    EXPECT_EQ(m2.tick.flags, 0);

    EXPECT_TRUE(eventually([&] { return gw.stats().ticks_out == 3; }));
    const auto s = gw.stats();
    EXPECT_EQ(s.clients, 1u);
    EXPECT_EQ(s.deliveries, 3u) << "1 snapshot + 2 live";
    EXPECT_EQ(s.conflated, 0u);
    EXPECT_EQ(s.latency_samples, 2u) << "the snapshot's age is not latency";
}

TEST(Gateway, HeartbeatIsEchoed) {
    Gateway gw(test_config());
    gw.start();
    Client c(gw.push_port());
    c.heartbeat();
    auto f = c.next();
    ASSERT_TRUE(f);
    EXPECT_EQ(f->type, MsgType::Heartbeat);
}

TEST(Gateway, UnsubscribeStopsTicks) {
    Gateway gw(test_config());
    gw.start();
    Client c(gw.push_port());
    c.subscribe({"A", "B"});
    auto ids = expect_subscribed(c, 2);
    c.unsubscribe({"A", "NEVER_SEEN"}); // unknown names are ignored
    c.heartbeat();                      // processed after the unsubscribe
    auto hb = c.next();
    ASSERT_TRUE(hb);
    ASSERT_EQ(hb->type, MsgType::Heartbeat);
    // The distributor handles ticks in order, and a client's queue keeps that
    // order, so had A's tick been delivered it would arrive before B's.
    gw.ingest(tick_for(ids["A"], 1.0));
    gw.ingest(tick_for(ids["B"], 2.0));
    const TickMsg m = c.next_tick();
    EXPECT_EQ(m.tick.symbol, ids["B"]);
    EXPECT_FALSE(gw.symbols().find("NEVER_SEEN")) << "unsubscribe must not create symbols";
}

TEST(Gateway, MalformedHeaderGetsErrorAndClose) {
    Gateway gw(test_config());
    gw.start();
    Client c(gw.push_port());
    c.send(Bytes{0xff, 0xff, 0xff, 0x7f, 1, 0, 0, 0}); // 2 GB body
    EXPECT_EQ(expect_error_then_close(c), ErrorCode::BadFrame);
    EXPECT_TRUE(eventually([&] { return gw.stats().clients == 0; }));
    EXPECT_EQ(gw.stats().protocol_errors, 1u);
}

TEST(Gateway, ServerOnlyMessageTypeIsRejected) {
    Gateway gw(test_config());
    gw.start();
    Client c(gw.push_port());
    Bytes b;
    Tick t;
    append_tick(b, t, 0); // clients may not send ticks
    c.send(b);
    EXPECT_EQ(expect_error_then_close(c), ErrorCode::BadFrame);
}

TEST(Gateway, InvalidSymbolIsRejected) {
    Gateway gw(test_config());
    gw.start();
    Client c(gw.push_port());
    c.send(Bytes{5, 0, 0, 0, 1, 0, 0, 0, 1, 0, 2, 'A', ' '}); // "A " has a space
    EXPECT_EQ(expect_error_then_close(c), ErrorCode::BadSymbol);
}

TEST(Gateway, TooManySymbolsIsRejected) {
    Gateway gw(test_config());
    gw.start();
    Client c(gw.push_port());
    c.send(Bytes{2, 0, 0, 0, 1, 0, 0, 0, 0x01, 0x04}); // n = 1025
    EXPECT_EQ(expect_error_then_close(c), ErrorCode::TooManySymbols);
}

TEST(Gateway, SymbolTableLimitIsEnforced) {
    auto config = test_config();
    config.max_symbols = 2;
    Gateway gw(config);
    gw.start();
    Client c(gw.push_port());
    c.subscribe({"A", "B", "C"});
    EXPECT_EQ(expect_error_then_close(c), ErrorCode::SymbolLimit);
    EXPECT_EQ(gw.symbols().size(), 2u);
}

TEST(Gateway, ClientsBeyondMaxAreTurnedAway) {
    auto config = test_config();
    config.max_clients = 1;
    Gateway gw(config);
    gw.start();
    Client first(gw.push_port());
    first.heartbeat();
    ASSERT_TRUE(first.next()); // first is fully set up
    Client second(gw.push_port());
    EXPECT_TRUE(second.closed_by_gateway());
    EXPECT_EQ(gw.stats().rejected_clients, 1u);
    first.heartbeat();
    EXPECT_TRUE(first.next()) << "the existing client is unaffected";
}

TEST(Gateway, StopClosesClients) {
    Gateway gw(test_config());
    gw.start();
    Client c(gw.push_port());
    c.subscribe({"A"});
    expect_subscribed(c, 1);
    gw.stop();
    EXPECT_TRUE(c.closed_by_gateway());
    gw.stop(); // idempotent
}

// The slow-consumer property, end to end. One client stops reading while a
// burst of ticks arrives; the other must still get every symbol's final value
// promptly, the stalled one is conflated (bounded memory), and once it reads
// again it catches up to the same final values.
TEST(Gateway, SlowClientIsConflatedAndDoesNotHoldBackOthers) {
    Gateway gw(test_config());
    gw.start();
    constexpr int kSymbols = 10;
    constexpr std::uint64_t kTicks = 200'000;
    std::vector<std::string> names;
    for (int i = 0; i < kSymbols; ++i)
        names.push_back("S" + std::to_string(i));

    Client slow(gw.push_port(), 4096);
    slow.subscribe(names);
    auto ids = expect_subscribed(slow, kSymbols);
    Client fast(gw.push_port());
    fast.subscribe(names);
    expect_subscribed(fast, kSymbols);

    // Sequence numbers start at 1, so the i-th ingest gets sequence i.
    std::map<SymbolId, std::uint64_t> final_seq;
    for (std::uint64_t i = 1; i <= kTicks; ++i)
        final_seq[ids[names[i % kSymbols]]] = i;

    std::atomic<bool> fast_done{false};
    std::thread fast_reader([&] {
        std::map<SymbolId, std::uint64_t> last;
        std::uint64_t backwards = 0;
        while (!fast_done) {
            auto f = fast.next(5000ms);
            if (!f)
                break;
            const auto m = parse_tick(f->body);
            if (!m)
                continue;
            if (m->tick.sequence < last[m->tick.symbol])
                ++backwards;
            last[m->tick.symbol] = m->tick.sequence;
            if (last == final_seq)
                fast_done = true;
        }
        EXPECT_EQ(backwards, 0u);
    });

    for (std::uint64_t i = 1; i <= kTicks; ++i)
        gw.ingest(tick_for(ids[names[i % kSymbols]], static_cast<double>(i)));

    fast_reader.join();
    EXPECT_TRUE(fast_done) << "fast client did not reach the final values while the slow one was stalled";
    EXPECT_GT(gw.stats().conflated, 0u) << "the stalled client should have been conflated";

    // Now the slow client reads: it must converge to the same final values.
    std::map<SymbolId, std::uint64_t> last;
    while (last != final_seq) {
        auto f = slow.next(5000ms);
        ASSERT_TRUE(f) << "slow client never caught up";
        if (const auto m = parse_tick(f->body)) {
            ASSERT_GE(m->tick.sequence, last[m->tick.symbol]);
            last[m->tick.symbol] = m->tick.sequence;
        }
    }
    // Conservation: every tick handed to a client queue was either written or
    // conflated (both clients are still connected, so none were dropped).
    ASSERT_TRUE(eventually([&] {
        const auto s = gw.stats();
        return s.deliveries == s.ticks_out + s.conflated;
    })) << "deliveries=" << gw.stats().deliveries << " out=" << gw.stats().ticks_out
        << " conflated=" << gw.stats().conflated;
    EXPECT_EQ(gw.stats().deliveries, 2 * kTicks);
}

// Ping-pong: ingest one tick, wait for it, repeat. Each time the client has the
// tick, the session's writer is just finishing that write and about to go idle,
// so the next tick keeps landing in the window between "queue looks empty" and
// "writer marked idle". If that hand-off were wrong, a tick would be stranded in
// the queue and this loop would time out.
TEST(Gateway, PingPongNeverStrandsATick) {
    Gateway gw(test_config());
    gw.start();
    Client c(gw.push_port());
    c.subscribe({"PP"});
    const SymbolId id = expect_subscribed(c, 1)["PP"];
    for (int i = 1; i <= 50'000; ++i) {
        gw.ingest(tick_for(id, i));
        const auto f = c.next(1000ms);
        ASSERT_TRUE(f) << "tick " << i << " stranded";
        const auto m = parse_tick(f->body);
        ASSERT_TRUE(m);
        ASSERT_EQ(m->tick.price, static_cast<double>(i));
    }
}

TEST(Gateway, FanOutReachesEveryClient) {
    Gateway gw(test_config());
    gw.start();
    constexpr int kClients = 16;
    std::vector<std::unique_ptr<Client>> clients;
    SymbolId id = 0;
    for (int i = 0; i < kClients; ++i) {
        clients.push_back(std::make_unique<Client>(gw.push_port()));
        clients.back()->subscribe({"X"});
        id = expect_subscribed(*clients.back(), 1)["X"];
    }
    for (int i = 1; i <= 100; ++i)
        gw.ingest(tick_for(id, i));
    for (auto& c : clients) {
        std::uint64_t seq = 0;
        while (seq < 100) {
            const TickMsg m = c->next_tick();
            ASSERT_GT(m.tick.sequence, seq);
            seq = m.tick.sequence;
        }
        EXPECT_EQ(seq, 100u);
    }
    EXPECT_EQ(gw.stats().clients, static_cast<std::uint64_t>(kClients));
}

TEST(Gateway, PortInUseThrows) {
    Gateway a(test_config());
    a.start();
    auto config = test_config();
    config.push_port = a.push_port();
    Gateway b(config);
    EXPECT_THROW(b.start(), std::system_error);
}
