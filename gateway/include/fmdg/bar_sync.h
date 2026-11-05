// gateway/include/fmdg/bar_sync.h
#pragma once

#include "fmdg/alpaca_protocol.h"

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <vector>

namespace fmdg {

struct BarSyncConfig {
    std::string host = "data.alpaca.markets";
    std::string port = "443";
    bool tls = true;     ///< false only for tests against a local plain-HTTP server
    std::string ca_file; ///< PEM bundle to trust instead of the system roots (optional)
    std::string key;     ///< API key id
    std::string secret;  ///< never logged
    std::string feed = "iex";
    std::vector<std::string> symbols;
    int days = 60;                        ///< how far back the backfill reaches
    std::size_t symbols_per_request = 100;
    /// Top-ups run at each multiple of `interval` plus `offset`: Alpaca publishes
    /// a minute's bar a few seconds after the minute ends.
    std::chrono::milliseconds interval{60'000};
    std::chrono::milliseconds offset{5'000};
    /// Each top-up re-reads this much: late trades revise recent bars.
    std::chrono::seconds overlap{120};
    int max_requests_per_minute = 150; ///< Alpaca's free plan allows 200
    std::chrono::milliseconds request_timeout{30'000};
    std::chrono::milliseconds retry_initial{2'000};
    std::chrono::milliseconds retry_max{60'000};

    /// Where the stored history of `symbols` ends: the earliest of their last
    /// stored bars plus one minute (Unix s), or 0 if any of them has none. Lets
    /// a restart resume instead of downloading `days` again. Optional.
    std::function<std::int64_t(const std::vector<std::string>&)> resume_from;
    /// Takes fetched bars (BarStore::add); returns the last sequence number queued.
    std::function<std::uint64_t(const std::vector<alpaca::Bar>&)> on_bars;
    /// BarStore::written_through(): complete_to() only moves once the bars of a
    /// pass are in the database, not merely queued. Optional (then: at once).
    std::function<std::uint64_t()> written_through;
    /// Bars queued but not yet written (BarStore stats().queued). The REST API
    /// delivers far faster than the database takes rows: a 500-symbol backfill
    /// would overflow the store's queue, which drops its oldest bars. So the sync
    /// stops fetching while the backlog is above `max_backlog`. Optional.
    std::function<std::size_t()> backlog;
    std::size_t max_backlog = 100'000;
    /// Unix seconds now; tests replace it.
    std::function<std::int64_t()> now;
    std::function<void(const std::string&)> log;
};

struct BarSyncStats {
    std::string state = "idle"; ///< idle, backfilling, waiting, topping up, retrying, stopped
    std::string last_error;
    std::int64_t complete_to = 0;  ///< see BarSync::complete_to()
    std::uint64_t requests = 0;     ///< HTTP responses received
    std::uint64_t bars = 0;
    std::uint64_t retries = 0;      ///< failed requests retried (network, 5xx, timeouts)
    std::uint64_t rate_limited = 0; ///< 429 responses
    std::uint64_t malformed = 0;
    double backfill_seconds = 0;
    double backpressure_seconds = 0; ///< time spent waiting for the store to catch up
};

/// Keeps the store's one-minute bars of `symbols` complete from Alpaca's REST
/// API: a backfill of `days` at start (resuming where the store ends), then a
/// top-up every minute that fetches every symbol's newest bars, many symbols per
/// request. Paced to `max_requests_per_minute`; 429 waits for Retry-After;
/// network errors, timeouts and 5xx retry with backoff; 401/403 and other 4xx
/// stop it (the key or the symbol list needs fixing). Own thread and io_context.
class BarSync {
  public:
    explicit BarSync(BarSyncConfig config);
    ~BarSync(); ///< stop()

    BarSync(const BarSync&) = delete;
    BarSync& operator=(const BarSync&) = delete;

    void start();
    void stop(); ///< idempotent

    /// Every symbol's one-minute bars starting before this time (Unix s) are in
    /// the store; 0 until the backfill has been written. HISTORY reports it so a
    /// client can tell complete history from a gap.
    std::int64_t complete_to() const;

    BarSyncStats stats() const;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace fmdg
