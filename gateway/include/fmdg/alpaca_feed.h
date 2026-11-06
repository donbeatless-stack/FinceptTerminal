// gateway/include/fmdg/alpaca_feed.h
#pragma once

#include "fmdg/alpaca_protocol.h"

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace fmdg {

class Gateway;

struct AlpacaFeedConfig {
    std::string host = "stream.data.alpaca.markets";
    std::string port = "443";
    /// "/v2/iex" (free plan), "/v2/sip" (paid), or "/v2/test", which streams the
    /// made-up symbol FAKEPACA around the clock.
    std::string path = "/v2/iex";
    bool tls = true;     ///< false only for tests against a local plain-TCP server
    std::string ca_file; ///< PEM bundle to trust instead of the system roots (optional)

    std::string key;    ///< API key id
    std::string secret; ///< API secret; never logged
    std::vector<std::string> symbols; ///< always streamed
    bool bars = true; ///< also subscribe to minute bars and their late corrections
    /// Also stream what push clients subscribe to: a symbol is added upstream
    /// when its first client subscribes and dropped when its last one leaves
    /// (`symbols` stay). This is what lets a client — the terminal — use the
    /// gateway instead of the account's one stream connection.
    bool on_demand = true;
    /// Upstream symbols at most, `symbols` included (the free plan streams 30);
    /// 0 = no cap. Demanded symbols over it are left out and counted.
    std::size_t max_symbols = 0;

    /// Budget for resolve + TCP connect + TLS + WebSocket upgrade, and again for
    /// the server's "connected" through "authenticated".
    std::chrono::milliseconds connect_timeout{10'000};
    /// A WebSocket ping goes out at this interval; a link that delivers nothing,
    /// not even a pong, for two intervals is treated as dead.
    std::chrono::milliseconds ping_interval{15'000};
    std::chrono::milliseconds backoff_initial{1'000}; ///< doubles per failed attempt, with jitter
    std::chrono::milliseconds backoff_max{30'000};
    /// After error 406: another session holds the account's one stream connection
    /// (the terminal, say). Retrying fast cannot help.
    std::chrono::milliseconds backoff_connection_limit{60'000};

    std::function<void(const alpaca::Bar&)> on_bar; ///< called on the feed thread
    std::function<void(const std::string&)> log;    ///< one line per event, feed thread
};

struct AlpacaFeedStats {
    std::string state = "idle"; ///< idle, connecting, authenticating, streaming, backoff, stopped
    std::string last_error;
    std::string subscription;    ///< what the server confirmed, e.g. "trades=[AAPL,MSFT] bars=[…]"
    std::size_t upstream = 0;    ///< symbols subscribed upstream now
    std::size_t skipped = 0;     ///< demanded symbols left out (over max_symbols, or not a stock symbol)
    std::uint64_t connects = 0;      ///< WebSocket upgrades completed
    std::uint64_t authenticated = 0; ///< sessions that authenticated
    std::uint64_t disconnects = 0;   ///< sessions that ended after connecting
    std::uint64_t messages = 0;      ///< WebSocket text messages received
    std::uint64_t trades = 0;        ///< trades handed to Gateway::ingest()
    std::uint64_t bars = 0;
    std::uint64_t dropped = 0;   ///< trades for a symbol the symbol table had no room for
    std::uint64_t malformed = 0; ///< unparsable messages or items
    std::uint64_t server_errors = 0;
};

/// Which symbols to stream upstream: all of `fixed`, then what clients
/// `demanded` — only symbols Alpaca would take (alpaca::streamable_symbol) and
/// only up to `cap` in total (0 = no cap). Symbols already streaming
/// (`current`) keep their place ahead of newcomers, so a new request never
/// pushes out a symbol someone is already watching.
struct UpstreamPlan {
    std::vector<std::string> want;    ///< sorted
    std::vector<std::string> add;     ///< in `want`, not in `current`
    std::vector<std::string> drop;    ///< in `current`, not in `want`
    std::vector<std::string> skipped; ///< demanded but left out; sorted
};
UpstreamPlan plan_upstream(const std::vector<std::string>& fixed, const std::vector<std::string>& current,
                           const std::vector<std::string>& demanded, std::size_t cap);

/// Alpaca's real-time stock stream as the gateway's feed: one WebSocket over
/// TLS, its trades turned into Ticks and handed to Gateway::ingest() — so it is
/// the gateway's single producer, like SyntheticFeed, and must not run beside
/// another feed. Runs on its own thread with its own io_context.
///
/// One session: resolve → TCP → TLS (system roots, hostname checked) → WebSocket
/// upgrade → "connected" → auth → "authenticated" → subscribe → stream. Any
/// failure ends the session and the next starts after a backoff (doubling,
/// jittered, reset once a session has authenticated). Server errors are acted
/// on by code (alpaca::classify_error): a bad key stops for good instead of
/// hammering the server; 406 waits a minute.
class AlpacaFeed {
  public:
    AlpacaFeed(Gateway& gateway, AlpacaFeedConfig config);
    ~AlpacaFeed(); ///< stops

    AlpacaFeed(const AlpacaFeed&) = delete;
    AlpacaFeed& operator=(const AlpacaFeed&) = delete;

    void start();
    /// Drops the connection and joins the thread. Idempotent. Stop the feed
    /// before the gateway: ingest() must not race Gateway::stop().
    void stop();

    AlpacaFeedStats stats() const;
    /// The request port's FEED reply: "FEED alpaca stream=<iex|sip|…> state=<state>
    /// on_demand=<0|1> upstream=<n> cap=<n>" and a newline. A client uses it to
    /// tell a gateway that streams live trades from one that only serves history.
    std::string status_line() const;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace fmdg
