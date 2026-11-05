// gateway/include/fmdg/bar_store.h
#pragma once

#include "fmdg/alpaca_protocol.h"
#include "fmdg/pg.h"
#include "fmdg/pool.h"

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace fmdg {

struct BarStoreConfig {
    /// libpq connection string. Keep the password out of it: use `passfile=`
    /// (or PGPASSFILE), e.g. "host=localhost dbname=fmdg user=fmdg passfile=...".
    std::string conninfo;
    std::string table = "bars_1m"; ///< [a-z0-9_] only; tests use their own
    PoolConfig pool{4};            ///< max_size 4: one writer plus readers
    std::size_t batch_max = 1000;  ///< bars per INSERT
    /// A partial batch is written once its oldest bar has waited this long.
    std::chrono::milliseconds flush_interval{1'000};
    /// Bars held while the database is unreachable; past this the oldest are
    /// dropped (and counted). A day of 30 symbols is about 12k bars.
    std::size_t queue_max = 500'000;
    std::chrono::milliseconds retry_initial{500};
    std::chrono::milliseconds retry_max{30'000};
    std::function<void(const std::string&)> log;
};

struct BarStoreStats {
    std::uint64_t received = 0;       ///< bars passed to add()
    std::uint64_t written = 0;        ///< rows inserted or updated
    std::uint64_t batches = 0;        ///< successful INSERTs
    std::uint64_t failed_batches = 0; ///< attempts that failed (each is retried)
    std::uint64_t dropped = 0;        ///< oldest bars dropped because the queue was full
    std::size_t queued = 0;           ///< waiting to be written now
    std::int64_t last_batch_us = 0;   ///< duration of the last successful INSERT
    std::string last_error;
    PoolStats pool;
};

/// Minute bars into PostgreSQL. add() only queues (never touches the network),
/// so the feed thread is never held up by the database; a writer thread drains
/// the queue in batches through a connection pool, one multi-row
/// INSERT ... ON CONFLICT DO UPDATE each, so Alpaca's late corrections
/// ("updatedBars") overwrite the bar they correct. A failed batch stays at the
/// head of the queue and is retried with backoff; a connection that broke is
/// discarded, not returned to the pool.
///
/// Table (created if missing):
///   <table>(symbol text, start_ts timestamptz, open/high/low/close/volume
///           double precision, received_at timestamptz, PRIMARY KEY (symbol, start_ts))
class BarStore {
  public:
    explicit BarStore(BarStoreConfig config);
    ~BarStore(); ///< stop()

    BarStore(const BarStore&) = delete;
    BarStore& operator=(const BarStore&) = delete;

    void start();
    /// Writes what is queued (one attempt per batch, so a dead database cannot
    /// hold shutdown hostage), then stops the writer. Idempotent.
    void stop();

    /// Any thread. Never blocks on the database.
    void add(const alpaca::Bar& bar);

    BarStoreStats stats() const;

    /// The store's pool, for readers of the same table.
    Pool<pg::Connection>& pool();

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace fmdg
