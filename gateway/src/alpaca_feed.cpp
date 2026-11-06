// gateway/src/alpaca_feed.cpp
#include "fmdg/alpaca_feed.h"

#include "fmdg/gateway.h"
#include "fmdg/tick.h"
#include "fmdg/ws_codec.h"
#include "net.h"

#include <asio.hpp>
#include <asio/experimental/awaitable_operators.hpp>
#include <asio/ssl.hpp>
#include <openssl/rand.h>

#include <algorithm>
#include <array>
#include <deque>
#include <format>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <stdexcept>
#include <thread>

namespace fmdg {

using asio::awaitable;
using asio::use_awaitable;
using asio::ip::tcp;
using namespace asio::experimental::awaitable_operators;
using Clock = std::chrono::steady_clock;

namespace {

/// Retrying cannot help: bad credentials, a feed the plan does not include.
struct FatalError : std::runtime_error {
    using std::runtime_error::runtime_error;
};
/// Retry, but only after backoff_connection_limit.
struct SlowRetryError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

template <std::size_t N>
std::array<std::uint8_t, N> random_bytes() {
    std::array<std::uint8_t, N> b{};
    if (RAND_bytes(b.data(), static_cast<int>(N)) != 1) {
        std::random_device rd;
        for (auto& x : b)
            x = static_cast<std::uint8_t>(rd());
    }
    return b;
}

std::string seconds_text(std::chrono::nanoseconds d) {
    return std::format("{:.1f} s", std::chrono::duration<double>(d).count());
}

using net::Connection;
using net::note_first_error;

std::string join(const std::vector<std::string>& v) {
    std::string out;
    for (const auto& s : v) {
        if (!out.empty())
            out += ',';
        out += s;
    }
    return out;
}

} // namespace

UpstreamPlan plan_upstream(const std::vector<std::string>& fixed, const std::vector<std::string>& current,
                           const std::vector<std::string>& demanded, std::size_t cap) {
    UpstreamPlan p;
    std::set<std::string, std::less<>> want(fixed.begin(), fixed.end());
    std::set<std::string, std::less<>> extra;
    for (const auto& s : demanded)
        if (!want.contains(s))
            extra.insert(s);
    // Candidates in order of precedence: those streaming already, then newcomers.
    const std::set<std::string, std::less<>> cur(current.begin(), current.end());
    std::vector<std::string> order;
    for (const auto& s : current)
        if (extra.contains(s))
            order.push_back(s);
    for (const auto& s : extra)
        if (!cur.contains(s))
            order.push_back(s);
    for (const auto& s : order) {
        if (!alpaca::streamable_symbol(s) || (cap != 0 && want.size() >= cap)) {
            p.skipped.push_back(s);
            continue;
        }
        want.insert(s);
    }
    p.want.assign(want.begin(), want.end());
    for (const auto& s : p.want)
        if (!cur.contains(s))
            p.add.push_back(s);
    for (const auto& s : cur)
        if (!want.contains(s))
            p.drop.push_back(s);
    std::sort(p.skipped.begin(), p.skipped.end());
    return p;
}

struct AlpacaFeed::Impl {
    Impl(Gateway& g, AlpacaFeedConfig c) : gateway(g), cfg(std::move(c)) {
        net::configure_tls(ssl, cfg.ca_file);
    }

    Gateway& gateway;
    const AlpacaFeedConfig cfg;
    mutable std::mutex mu;
    AlpacaFeedStats st;
    std::thread thread;
    bool started = false;

    // io thread only.
    /// What the last session subscribed to: newcomers queue behind these.
    std::vector<std::string> upstream;
    std::vector<std::string> last_skipped;
    /// Set while a session is authenticated: brings the upstream subscription in
    /// line with what clients want now. The demand listener posts calls to it.
    std::function<void()> resync;

    asio::ssl::context ssl{asio::ssl::context::tls_client};
    /// Declared last, so destroyed first: tearing it down destroys the suspended
    /// coroutines and, with them, their sockets — before anything they reference.
    asio::io_context io{1};

    void log(const std::string& line) const {
        if (cfg.log)
            cfg.log(line);
    }
    template <class F>
    void update(F&& f) {
        std::lock_guard lock(mu);
        f(st);
    }
    void set_state(const char* state) {
        update([&](AlpacaFeedStats& s) { s.state = state; });
    }
    std::vector<std::string> demanded() const {
        return cfg.on_demand ? gateway.demanded_symbols() : std::vector<std::string>{};
    }
    void note_skipped(const std::vector<std::string>& skipped) {
        update([&](AlpacaFeedStats& s) { s.skipped = skipped.size(); });
        if (skipped == last_skipped)
            return;
        last_skipped = skipped;
        if (!skipped.empty())
            log(std::format("not streaming [{}]: over the {}-symbol cap, or not a stock symbol", join(skipped),
                            cfg.max_symbols));
    }

    awaitable<void> supervise();
    awaitable<void> session(bool& connected, bool& authenticated);
};

// ── one connection, start to end ─────────────────────────────────────────────

awaitable<void> AlpacaFeed::Impl::session(bool& connected, bool& authenticated) {
    const auto ex = co_await asio::this_coro::executor;
    Connection conn(ex, ssl, cfg.tls);
    std::string leftover; // bytes that arrived behind the upgrade response

    // Connect: resolve → TCP → TLS → WebSocket upgrade, under one deadline.
    // `a && b` fails as soon as either fails and cancels the other; the deadline
    // branch finishes quietly when cancelled, so success needs both to finish.
    set_state("connecting");
    asio::steady_timer deadline(ex, cfg.connect_timeout);
    bool upgraded = false;
    auto expire = [&]() -> awaitable<void> {
        asio::error_code ec;
        if (!upgraded)
            co_await deadline.async_wait(asio::redirect_error(use_awaitable, ec));
        if (!ec && !upgraded)
            throw std::runtime_error("connect timed out after " + seconds_text(cfg.connect_timeout));
    };
    auto connect = [&]() -> awaitable<void> {
        co_await conn.open(cfg.host, cfg.port); // resolve, TCP, TLS with SNI and hostname check
        const std::string key = ws::client_key(random_bytes<16>());
        const bool default_port = cfg.port == (cfg.tls ? "443" : "80");
        co_await conn.write(ws::upgrade_request(default_port ? cfg.host : cfg.host + ":" + cfg.port, cfg.path, key));

        std::string head;
        std::array<char, 4096> buf;
        std::size_t end;
        while ((end = head.find("\r\n\r\n")) == std::string::npos) {
            if (head.size() > 16 * 1024)
                throw std::runtime_error("handshake: response head over 16 KiB");
            asio::error_code ec;
            const std::size_t n = co_await conn.read_some(asio::buffer(buf), ec);
            if (ec)
                throw std::system_error(ec, "handshake");
            head.append(buf.data(), n);
        }
        leftover = head.substr(end + 4);
        head.resize(end + 4);
        if (const std::string err = ws::check_upgrade_response(head, key); !err.empty())
            throw std::runtime_error(err);
        upgraded = true;
        deadline.cancel();
    };
    std::exception_ptr first_error;
    try {
        co_await (note_first_error(connect(), first_error) && note_first_error(expire(), first_error));
    } catch (const asio::multiple_exceptions&) {
        std::rethrow_exception(first_error);
    }

    connected = true;
    update([](AlpacaFeedStats& s) {
        ++s.connects;
        s.state = "authenticating";
    });
    log(std::format("connected to {}{} ({}), authenticating", cfg.host, cfg.path,
                    conn.tls() ? SSL_get_version(conn.tls()->native_handle()) : "no TLS"));

    // Stream: a reader and a writer side by side, `&&` again — whichever fails
    // first ends the session and cancels the other. Only the writer writes, so
    // there is never more than one write in flight on the stream.
    ws::FrameParser frames;
    alpaca::MessageParser json;
    std::deque<std::string> outbox;
    asio::steady_timer wake(ex);
    auto last_rx = Clock::now();
    const auto auth_deadline = Clock::now() + cfg.connect_timeout;
    bool closing = false;
    bool first_trade = true;
    std::string close_reason;

    auto send = [&](ws::Opcode op, std::string_view payload) {
        outbox.push_back(ws::encode_frame(op, payload, random_bytes<4>()));
        wake.cancel(); // wakes the writer if it is waiting
    };

    // Follow the clients: drop what nobody wants first, so the account's symbol
    // cap has room, then add. Alpaca answers each with the whole subscription.
    auto sync_upstream = [&] {
        const UpstreamPlan plan = plan_upstream(cfg.symbols, upstream, demanded(), cfg.max_symbols);
        note_skipped(plan.skipped);
        if (plan.add.empty() && plan.drop.empty())
            return;
        if (!plan.drop.empty())
            send(ws::Opcode::Text, alpaca::unsubscribe_message(plan.drop, cfg.bars));
        if (!plan.add.empty())
            send(ws::Opcode::Text, alpaca::subscribe_message(plan.add, cfg.bars));
        upstream = plan.want;
        update([&](AlpacaFeedStats& s) { s.upstream = upstream.size(); });
        log(std::format("following clients: +[{}] -[{}], {} symbol(s) upstream", join(plan.add), join(plan.drop),
                        upstream.size()));
    };
    // Declared after everything sync_upstream refers to, so it is destroyed
    // first: once this session is gone, a posted resync finds nothing to call.
    struct ResyncGuard {
        std::function<void()>& slot;
        ~ResyncGuard() { slot = nullptr; }
    } resync_guard{resync};

    auto handle_text = [&](const std::string& text) {
        alpaca::Batch batch;
        std::string err;
        if (!json.parse(text, batch, &err)) {
            update([](AlpacaFeedStats& s) {
                ++s.messages;
                ++s.malformed;
            });
            log("unparsable message: " + err);
            return;
        }
        for (const auto& c : batch.control) {
            switch (c.kind) {
                case alpaca::Control::Kind::Connected:
                    send(ws::Opcode::Text, alpaca::auth_message(cfg.key, cfg.secret));
                    break;
                case alpaca::Control::Kind::Authenticated: {
                    authenticated = true;
                    // A new connection subscribes to everything wanted now; the
                    // last session's list only decides who goes first under the cap.
                    const UpstreamPlan plan = plan_upstream(cfg.symbols, upstream, demanded(), cfg.max_symbols);
                    upstream = plan.want;
                    note_skipped(plan.skipped);
                    update([&](AlpacaFeedStats& s) {
                        ++s.authenticated;
                        s.upstream = upstream.size();
                        s.state = upstream.empty() ? "streaming" : "subscribing";
                    });
                    log(std::format("authenticated; subscribing to {} symbol(s)", upstream.size()));
                    if (!upstream.empty())
                        send(ws::Opcode::Text, alpaca::subscribe_message(upstream, cfg.bars));
                    resync = sync_upstream;
                    break;
                }
                case alpaca::Control::Kind::Subscription:
                    update([&](AlpacaFeedStats& s) {
                        s.state = "streaming";
                        s.subscription = c.message;
                    });
                    log("subscribed: " + c.message);
                    break;
                case alpaca::Control::Kind::Error: {
                    const std::string what = std::format("server error {}: {}", c.code, c.message);
                    update([&](AlpacaFeedStats& s) {
                        ++s.server_errors;
                        s.last_error = what;
                    });
                    switch (alpaca::classify_error(c.code)) {
                        case alpaca::ErrorAction::Ignore:
                            log(what);
                            break;
                        case alpaca::ErrorAction::Reconnect:
                            throw std::runtime_error(what);
                        case alpaca::ErrorAction::ReconnectSlowly:
                            throw SlowRetryError(what + " — another session is using this account's stream");
                        case alpaca::ErrorAction::Stop:
                            throw FatalError(what);
                    }
                    break;
                }
            }
        }
        std::uint64_t dropped = 0;
        for (const auto& t : batch.trades) {
            const auto id = gateway.symbols().intern(t.symbol);
            if (!id) {
                ++dropped;
                continue;
            }
            Tick tick;
            tick.symbol = *id;
            tick.price = t.price;
            tick.size = t.size;
            tick.exchange_ts_ns = t.ts_ns;
            gateway.ingest(tick);
            if (first_trade) {
                first_trade = false;
                log(std::format("first trade: {} {} x {}", t.symbol, t.price, t.size));
            }
        }
        update([&](AlpacaFeedStats& s) {
            ++s.messages;
            s.trades += batch.trades.size() - dropped;
            s.dropped += dropped;
            s.bars += batch.bars.size();
            s.malformed += batch.malformed;
        });
        if (cfg.on_bar)
            for (const auto& b : batch.bars)
                cfg.on_bar(b);
    };

    auto reader = [&]() -> awaitable<void> {
        std::vector<ws::Event> events;
        std::array<char, 16 * 1024> buf;
        std::string_view chunk = leftover;
        for (;;) {
            if (!frames.feed(chunk, events))
                throw std::runtime_error("websocket protocol error: " + frames.error());
            for (auto& e : events) {
                switch (e.kind) {
                    case ws::Event::Kind::Text:
                        handle_text(e.payload);
                        break;
                    case ws::Event::Kind::Binary: // msgpack, never requested
                        update([](AlpacaFeedStats& s) { ++s.malformed; });
                        break;
                    case ws::Event::Kind::Ping:
                        send(ws::Opcode::Pong, e.payload);
                        break;
                    case ws::Event::Kind::Pong:
                        break;
                    case ws::Event::Kind::Close:
                        if (!closing) {
                            // Echo the close, then read on until the server drops TCP.
                            closing = true;
                            close_reason = std::format("server closed the stream ({}{}{})", e.close_code,
                                                       e.payload.empty() ? "" : ": ", e.payload);
                            const bool sendable = e.close_code != 1005 && e.close_code != 1006;
                            send(ws::Opcode::Close, ws::close_payload(sendable ? e.close_code : 1000));
                        }
                        break;
                }
            }
            events.clear();
            asio::error_code ec;
            const std::size_t n = co_await conn.read_some(asio::buffer(buf), ec);
            if (ec) {
                if (closing)
                    throw std::runtime_error(close_reason);
                throw std::system_error(ec, "read");
            }
            last_rx = Clock::now();
            chunk = std::string_view(buf.data(), n);
        }
    };

    auto writer = [&]() -> awaitable<void> {
        auto next_ping = Clock::now() + cfg.ping_interval;
        for (;;) {
            while (!outbox.empty()) {
                const std::string frame = std::move(outbox.front());
                outbox.pop_front();
                co_await conn.write(frame);
            }
            const auto now = Clock::now();
            if (!authenticated && !closing && now >= auth_deadline)
                throw std::runtime_error("not authenticated within " + seconds_text(cfg.connect_timeout));
            if (now >= next_ping && !closing) {
                if (now - last_rx >= 2 * cfg.ping_interval)
                    throw std::runtime_error("no data for " + seconds_text(now - last_rx) + ", not even a pong");
                send(ws::Opcode::Ping, {});
                next_ping = now + cfg.ping_interval;
                continue;
            }
            wake.expires_at(authenticated ? next_ping : std::min(next_ping, auth_deadline));
            asio::error_code ec;
            co_await wake.async_wait(asio::redirect_error(use_awaitable, ec));
            // Woken by send(), by the deadline, or because the reader failed and
            // `&&` is cancelling us: in that last case, stop.
            const auto cs = co_await asio::this_coro::cancellation_state;
            if (cs.cancelled() != asio::cancellation_type::none)
                co_return;
        }
    };

    first_error = nullptr;
    try {
        co_await (note_first_error(reader(), first_error) && note_first_error(writer(), first_error));
    } catch (const asio::multiple_exceptions&) {
        std::rethrow_exception(first_error);
    }
}

// ── reconnect loop ───────────────────────────────────────────────────────────

awaitable<void> AlpacaFeed::Impl::supervise() {
    const auto ex = co_await asio::this_coro::executor;
    const auto seed = random_bytes<8>();
    std::uint64_t s = 0;
    for (auto b : seed)
        s = (s << 8) | b;
    std::mt19937_64 rng(s);
    auto backoff = cfg.backoff_initial;

    for (;;) {
        bool connected = false, authenticated = false, fatal = false, slow = false;
        std::string why;
        try {
            co_await session(connected, authenticated);
        } catch (const FatalError& e) {
            why = e.what();
            fatal = true;
        } catch (const SlowRetryError& e) {
            why = e.what();
            slow = true;
        } catch (const std::exception& e) {
            why = e.what();
        }
        update([&](AlpacaFeedStats& x) {
            x.last_error = why;
            if (connected)
                ++x.disconnects;
        });
        if (fatal) {
            set_state("stopped");
            log("stopped, not retrying: " + why);
            co_return;
        }

        // A session that authenticated proves the setup works: start again from
        // the shortest wait. Otherwise each failure doubles it, up to the cap.
        if (authenticated)
            backoff = cfg.backoff_initial;
        auto wait = slow ? cfg.backoff_connection_limit : backoff;
        if (!slow)
            backoff = std::min(backoff * 2, cfg.backoff_max);
        // 50-100 % of the wait, so many clients dropped together do not return together.
        wait = std::chrono::milliseconds(
            static_cast<long long>(static_cast<double>(wait.count()) * std::uniform_real_distribution(0.5, 1.0)(rng)));

        set_state("backoff");
        log(std::format("disconnected: {}; reconnecting in {}", why, seconds_text(wait)));
        asio::steady_timer timer(ex, wait);
        co_await timer.async_wait(use_awaitable);
    }
}

// ── public API ───────────────────────────────────────────────────────────────

AlpacaFeed::AlpacaFeed(Gateway& gateway, AlpacaFeedConfig config)
    : impl_(std::make_unique<Impl>(gateway, std::move(config))) {}

AlpacaFeed::~AlpacaFeed() {
    stop();
}

void AlpacaFeed::start() {
    if (impl_->started)
        return;
    impl_->started = true;
    if (impl_->cfg.on_demand)
        impl_->gateway.set_demand_listener([im = impl_.get()] {
            asio::post(im->io, [im] {
                if (im->resync)
                    im->resync();
            });
        });
    asio::co_spawn(impl_->io, impl_->supervise(), asio::detached);
    impl_->thread = std::thread([this] { impl_->io.run(); });
}

void AlpacaFeed::stop() {
    // First, so no I/O thread posts to the io_context while it goes away.
    if (impl_->started && impl_->cfg.on_demand)
        impl_->gateway.set_demand_listener(nullptr);
    impl_->io.stop();
    if (impl_->thread.joinable())
        impl_->thread.join();
    if (impl_->started)
        impl_->set_state("stopped");
}

AlpacaFeedStats AlpacaFeed::stats() const {
    std::lock_guard lock(impl_->mu);
    return impl_->st;
}

std::string AlpacaFeed::status_line() const {
    const AlpacaFeedStats s = stats();
    const std::string& path = impl_->cfg.path;
    const std::string stream = path.substr(path.find_last_of('/') + 1);
    return std::format("FEED alpaca stream={} state={} on_demand={} upstream={} cap={}\n", stream, s.state,
                       impl_->cfg.on_demand ? 1 : 0, s.upstream, impl_->cfg.max_symbols);
}

} // namespace fmdg
