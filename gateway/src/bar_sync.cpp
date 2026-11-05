// gateway/src/bar_sync.cpp
#include "fmdg/bar_sync.h"

#include "http_client.h"
#include "net.h"

#include <algorithm>
#include <charconv>
#include <deque>
#include <format>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <thread>

namespace fmdg {

using asio::awaitable;
using asio::use_awaitable;
using Clock = std::chrono::steady_clock;

namespace {

/// Retrying cannot help: a bad key, or a request Alpaca refuses as malformed.
struct FatalError : std::runtime_error {
    using std::runtime_error::runtime_error;
};

std::int64_t system_now() {
    return std::chrono::duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::string body_head(const std::string& body) {
    std::string s = body.substr(0, 200);
    for (char& c : s)
        if (c == '\n' || c == '\r')
            c = ' ';
    return s;
}

} // namespace

struct BarSync::Impl {
    explicit Impl(BarSyncConfig c) : cfg(std::move(c)) {
        if (!cfg.now)
            cfg.now = system_now;
        if (cfg.tls)
            net::configure_tls(ssl, cfg.ca_file);
    }

    BarSyncConfig cfg;
    asio::ssl::context ssl{asio::ssl::context::tls_client};

    mutable std::mutex mu;
    BarSyncStats st;
    struct Pass {
        std::uint64_t last_seq; ///< last bar of the pass, as queued in the store
        std::int64_t until;     ///< the pass covered every minute before this
    };
    mutable std::deque<Pass> pending; ///< finished passes not yet confirmed written
    mutable std::int64_t complete = 0;

    std::thread thread;
    bool started = false;
    Clock::time_point next_slot = Clock::now();
    std::uint64_t last_seq = 0; ///< sync thread only

    asio::io_context io{1}; // last: destroyed first, with the suspended coroutines

    void log(const std::string& line) const {
        if (cfg.log)
            cfg.log(line);
    }
    template <class F>
    void update(F&& f) {
        std::lock_guard lock(mu);
        f(st);
    }
    void set_state(const char* s) {
        update([&](BarSyncStats& x) { x.state = s; });
    }

    awaitable<void> sleep(std::chrono::milliseconds d) {
        asio::steady_timer t(co_await asio::this_coro::executor, d);
        co_await t.async_wait(use_awaitable);
    }

    // Paces requests evenly: at most max_requests_per_minute.
    awaitable<void> pace() {
        const auto gap = std::chrono::milliseconds(60'000 / std::max(1, cfg.max_requests_per_minute));
        const auto now = Clock::now();
        if (next_slot > now)
            co_await sleep(std::chrono::duration_cast<std::chrono::milliseconds>(next_slot - now));
        next_slot = std::max(next_slot, now) + gap;
    }

    awaitable<http::Response> fetch(net::HttpClient& http, const std::string& target) {
        const http::Headers headers{{"APCA-API-KEY-ID", cfg.key},
                                    {"APCA-API-SECRET-KEY", cfg.secret},
                                    {"Accept", "application/json"},
                                    {"User-Agent", "fmdg/0.1"}};
        auto retry = cfg.retry_initial;
        for (;;) {
            co_await pace();
            std::string problem;
            std::chrono::milliseconds wait = retry;
            try {
                http::Response r = co_await http.get(target, headers, cfg.request_timeout);
                update([](BarSyncStats& x) { ++x.requests; });
                if (r.status == 200)
                    co_return r;
                if (r.status == 401 || r.status == 403)
                    throw FatalError(std::format("HTTP {}: {} (check the API key)", r.status, body_head(r.body)));
                if (r.status == 429) {
                    int seconds = 60;
                    const std::string ra = r.header("Retry-After");
                    std::from_chars(ra.data(), ra.data() + ra.size(), seconds);
                    wait = std::chrono::seconds(std::clamp(seconds, 1, 300));
                    problem = "HTTP 429 rate limited";
                    update([](BarSyncStats& x) { ++x.rate_limited; });
                } else if (r.status >= 400 && r.status < 500) {
                    throw FatalError(std::format("HTTP {}: {}", r.status, body_head(r.body)));
                } else {
                    problem = std::format("HTTP {}: {}", r.status, body_head(r.body));
                }
            } catch (const FatalError&) {
                throw;
            } catch (const std::exception& e) {
                problem = e.what(); // network error or timeout
            }
            if (problem.rfind("HTTP 429", 0) != 0) {
                update([](BarSyncStats& x) { ++x.retries; });
                retry = std::min(retry * 2, cfg.retry_max);
            }
            update([&](BarSyncStats& x) {
                x.last_error = problem;
                x.state = "retrying";
            });
            log(std::format("{}; retrying in {} ms", problem, wait.count()));
            co_await sleep(wait);
        }
    }

    // Every page of one-minute bars of `symbols` starting in [start, end].
    awaitable<std::uint64_t> fetch_range(net::HttpClient& http, const std::vector<std::string>& symbols,
                                         std::int64_t start, std::int64_t end) {
        std::uint64_t n = 0;
        if (start > end)
            co_return n;
        alpaca::MessageParser parser;
        std::string token;
        do {
            const http::Response r = co_await fetch(http, alpaca::bars_target(symbols, start, end, cfg.feed, token));
            std::vector<alpaca::Bar> bars;
            std::size_t malformed = 0;
            std::string err;
            if (!parser.parse_bars_page(r.body, bars, token, malformed, &err))
                throw FatalError("unparsable bars page: " + err);
            if (!bars.empty() && cfg.on_bars)
                last_seq = cfg.on_bars(bars);
            n += bars.size();
            update([&](BarSyncStats& x) {
                x.bars += bars.size();
                x.malformed += malformed;
            });
            // Backpressure: no next page while the store is this far behind.
            if (cfg.backlog && cfg.backlog() > cfg.max_backlog) {
                const auto t0 = Clock::now();
                while (cfg.backlog() > cfg.max_backlog / 2) // resume at half, not at the edge
                    co_await sleep(std::chrono::milliseconds(50));
                const double waited = std::chrono::duration<double>(Clock::now() - t0).count();
                update([&](BarSyncStats& x) { x.backpressure_seconds += waited; });
            }
        } while (!token.empty());
        co_return n;
    }

    // One pass over all symbols for [start or resume point, end]; then the pass
    // waits in `pending` until the store has written its last bar.
    awaitable<std::uint64_t> pass(net::HttpClient& http, std::int64_t start, std::int64_t end, bool resume) {
        std::uint64_t n = 0;
        last_seq = 0;
        for (std::size_t i = 0; i < cfg.symbols.size(); i += cfg.symbols_per_request) {
            const std::vector<std::string> chunk(
                cfg.symbols.begin() + static_cast<std::ptrdiff_t>(i),
                cfg.symbols.begin() + static_cast<std::ptrdiff_t>(std::min(cfg.symbols.size(), i + cfg.symbols_per_request)));
            std::int64_t from = start;
            if (resume && cfg.resume_from)
                if (const std::int64_t r = cfg.resume_from(chunk); r > 0)
                    from = std::max(from, r);
            n += co_await fetch_range(http, chunk, from, end);
        }
        std::lock_guard lock(mu);
        pending.push_back({last_seq, end + 1});
        return_pending_locked();
        co_return n;
    }

    // Moves complete_to past every pass the store has finished writing.
    void return_pending_locked() const {
        const std::uint64_t written =
            cfg.written_through ? cfg.written_through() : std::numeric_limits<std::uint64_t>::max();
        while (!pending.empty() && pending.front().last_seq <= written) {
            complete = pending.front().until;
            pending.pop_front();
        }
    }

    awaitable<void> run() {
        net::HttpClient http(ssl, cfg.host, cfg.port, cfg.tls);
        try {
            // 1. Backfill: `days` back, or from where the store's history ends.
            set_state("backfilling");
            const auto t0 = Clock::now();
            std::int64_t until = cfg.now() / 60 * 60; // minutes before this are over
            const std::uint64_t n = co_await pass(http, until - std::int64_t{cfg.days} * 86'400, until - 1, true);
            const double secs = std::chrono::duration<double>(Clock::now() - t0).count();
            update([&](BarSyncStats& x) { x.backfill_seconds = secs; });
            log(std::format("backfill: {} bars for {} symbols in {:.1f} s", n, cfg.symbols.size(), secs));

            // 2. Top-ups: each new minute, re-reading `overlap` for late corrections.
            for (;;) {
                set_state("waiting");
                const auto period = std::max<std::int64_t>(cfg.interval.count(), 1);
                const auto now_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                        std::chrono::system_clock::now().time_since_epoch())
                                        .count();
                const std::int64_t next = (now_ms / period + 1) * period + cfg.offset.count();
                co_await sleep(std::chrono::milliseconds(next - now_ms));
                const std::int64_t end = cfg.now() / 60 * 60;
                if (end <= until)
                    continue; // no minute has ended since the last pass
                set_state("topping up");
                co_await pass(http, until - cfg.overlap.count(), end - 1, false);
                until = end;
            }
        } catch (const FatalError& e) {
            update([&](BarSyncStats& x) {
                x.state = "stopped";
                x.last_error = e.what();
            });
            log(std::string("stopped, not retrying: ") + e.what());
        }
    }
};

BarSync::BarSync(BarSyncConfig config) : impl_(std::make_unique<Impl>(std::move(config))) {
    if (impl_->cfg.symbols_per_request == 0)
        throw std::invalid_argument("BarSync: symbols_per_request must be positive");
}

BarSync::~BarSync() {
    stop();
}

void BarSync::start() {
    if (impl_->started)
        return;
    impl_->started = true;
    asio::co_spawn(impl_->io, impl_->run(), asio::detached);
    impl_->thread = std::thread([this] { impl_->io.run(); });
}

void BarSync::stop() {
    impl_->io.stop();
    if (impl_->thread.joinable())
        impl_->thread.join();
    if (impl_->started)
        impl_->set_state("stopped");
}

std::int64_t BarSync::complete_to() const {
    std::lock_guard lock(impl_->mu);
    impl_->return_pending_locked();
    return impl_->complete;
}

BarSyncStats BarSync::stats() const {
    std::lock_guard lock(impl_->mu);
    impl_->return_pending_locked();
    BarSyncStats s = impl_->st;
    s.complete_to = impl_->complete;
    return s;
}

} // namespace fmdg
