// gateway/src/bar_store.cpp
#include "fmdg/bar_store.h"

#include <algorithm>
#include <cctype>
#include <charconv>
#include <condition_variable>
#include <deque>
#include <format>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <vector>

namespace fmdg {

namespace {

using Clock = std::chrono::steady_clock;

bool valid_identifier(const std::string& s) {
    if (s.empty() || s.size() > 63 || !(std::islower(static_cast<unsigned char>(s[0])) || s[0] == '_'))
        return false;
    return std::all_of(s.begin(), s.end(), [](char c) {
        return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '_';
    });
}

std::string number(double v) {
    char buf[32];
    const auto r = std::to_chars(buf, buf + sizeof buf, v); // shortest round-trip form
    return std::string(buf, r.ptr);
}

// Epoch nanoseconds → "seconds.nanoseconds" for to_timestamp().
std::string epoch_seconds(std::int64_t ns) {
    return std::format("{}.{:09}", ns / 1'000'000'000, ns % 1'000'000'000);
}

struct Queued {
    std::uint64_t seq;
    Clock::time_point arrived;
    alpaca::Bar bar;
};

} // namespace

struct BarStore::Impl {
    explicit Impl(BarStoreConfig c)
        : cfg(std::move(c)),
          pool(
              cfg.pool, [this] { return pg::Connection::open(cfg.conninfo); },
              [](pg::Connection& conn) { return conn.healthy(); }) {}

    BarStoreConfig cfg;
    Pool<pg::Connection> pool;

    mutable std::mutex mu;
    std::condition_variable cv;
    std::deque<Queued> queue;
    std::uint64_t next_seq = 0;
    bool started = false;
    bool stopping = false;
    BarStoreStats st;
    std::thread writer;
    bool schema_ready = false; // writer thread only

    void log(const std::string& line) const {
        if (cfg.log)
            cfg.log(line);
    }

    void ensure_schema(pg::Connection& conn) {
        conn.exec(std::format("CREATE TABLE IF NOT EXISTS {} ("
                              "symbol text NOT NULL, "
                              "start_ts timestamptz NOT NULL, "
                              "open double precision NOT NULL, "
                              "high double precision NOT NULL, "
                              "low double precision NOT NULL, "
                              "close double precision NOT NULL, "
                              "volume double precision NOT NULL, "
                              "received_at timestamptz NOT NULL DEFAULT now(), "
                              "PRIMARY KEY (symbol, start_ts))",
                              cfg.table));
    }

    /// One INSERT for the whole batch. Returns the rows it inserted or updated.
    std::uint64_t write(const std::vector<Queued>& batch) {
        // One row per (symbol, minute), the last one winning: ON CONFLICT DO UPDATE
        // refuses to touch a row twice in one statement, and a bar and its
        // correction can land in the same batch.
        std::vector<const alpaca::Bar*> rows;
        std::unordered_map<std::string, std::size_t> index;
        for (const auto& q : batch) {
            const std::string key = q.bar.symbol + '\x1f' + std::to_string(q.bar.start_ns);
            const auto [it, fresh] = index.try_emplace(key, rows.size());
            if (fresh)
                rows.push_back(&q.bar);
            else
                rows[it->second] = &q.bar;
        }

        std::string sql = std::format("INSERT INTO {} (symbol, start_ts, open, high, low, close, volume) VALUES ",
                                      cfg.table);
        pg::Params params;
        params.reserve(rows.size() * 7);
        for (std::size_t i = 0; i < rows.size(); ++i) {
            const std::size_t p = i * 7;
            sql += std::format("{}(${}, to_timestamp(${}::double precision), ${}, ${}, ${}, ${}, ${})", i ? ", " : "",
                               p + 1, p + 2, p + 3, p + 4, p + 5, p + 6, p + 7);
            const alpaca::Bar& b = *rows[i];
            params.emplace_back(b.symbol);
            params.emplace_back(epoch_seconds(b.start_ns));
            params.emplace_back(number(b.open));
            params.emplace_back(number(b.high));
            params.emplace_back(number(b.low));
            params.emplace_back(number(b.close));
            params.emplace_back(number(b.volume));
        }
        sql += " ON CONFLICT (symbol, start_ts) DO UPDATE SET open = EXCLUDED.open, high = EXCLUDED.high, "
               "low = EXCLUDED.low, close = EXCLUDED.close, volume = EXCLUDED.volume, received_at = now()";

        auto lease = pool.acquire();
        try {
            if (!schema_ready) {
                ensure_schema(*lease);
                schema_ready = true;
            }
            return lease->exec(sql, params).affected();
        } catch (const pg::Error& e) {
            if (e.connection_lost())
                lease.discard(); // never hand a dead connection to the next borrower
            throw;
        }
    }

    void run() {
        auto retry = cfg.retry_initial;
        for (;;) {
            std::vector<Queued> batch;
            bool final_pass = false;
            {
                std::unique_lock lock(mu);
                cv.wait(lock, [&] { return stopping || !queue.empty(); });
                // Wait for a full batch, or until the oldest bar has waited flush_interval.
                if (!stopping && !queue.empty() && queue.size() < cfg.batch_max)
                    cv.wait_until(lock, queue.front().arrived + cfg.flush_interval,
                                  [&] { return stopping || queue.size() >= cfg.batch_max; });
                if (queue.empty()) {
                    if (stopping)
                        return;
                    continue;
                }
                const std::size_t n = std::min(queue.size(), cfg.batch_max);
                batch.assign(queue.begin(), queue.begin() + static_cast<std::ptrdiff_t>(n));
                final_pass = stopping;
            }

            try {
                const auto t0 = Clock::now();
                const std::uint64_t affected = write(batch);
                const auto us = std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0).count();
                std::lock_guard lock(mu);
                // Remove what was written. Not "the first n": while we wrote, an
                // overflow may have dropped some of them from the front.
                const std::uint64_t last = batch.back().seq;
                while (!queue.empty() && queue.front().seq <= last)
                    queue.pop_front();
                st.written += affected;
                ++st.batches;
                st.last_batch_us = us;
                retry = cfg.retry_initial;
            } catch (const std::exception& e) {
                std::size_t left = 0;
                {
                    std::lock_guard lock(mu);
                    ++st.failed_batches;
                    st.last_error = e.what();
                    left = queue.size();
                }
                if (final_pass) {
                    log(std::format("stopping with {} bar(s) unwritten: {}", left, e.what()));
                    return;
                }
                log(std::format("write of {} bar(s) failed, retrying in {} ms: {}", batch.size(), retry.count(),
                                e.what()));
                std::unique_lock lock(mu);
                cv.wait_for(lock, retry, [&] { return stopping; });
                retry = std::min(retry * 2, cfg.retry_max);
            }
        }
    }
};

BarStore::BarStore(BarStoreConfig config) {
    if (!valid_identifier(config.table))
        throw std::invalid_argument("bar store table name must match [a-z_][a-z0-9_]*: " + config.table);
    if (config.batch_max == 0 || config.queue_max == 0)
        throw std::invalid_argument("bar store batch_max and queue_max must be positive");
    impl_ = std::make_unique<Impl>(std::move(config));
}

BarStore::~BarStore() {
    stop();
}

void BarStore::start() {
    std::lock_guard lock(impl_->mu);
    if (impl_->started)
        return;
    impl_->started = true;
    impl_->writer = std::thread([this] { impl_->run(); });
}

void BarStore::stop() {
    {
        std::lock_guard lock(impl_->mu);
        impl_->stopping = true;
    }
    impl_->cv.notify_all();
    if (impl_->writer.joinable())
        impl_->writer.join();
}

std::uint64_t BarStore::add(const alpaca::Bar& bar) {
    std::uint64_t seq = 0;
    {
        std::lock_guard lock(impl_->mu);
        ++impl_->st.received;
        if (impl_->queue.size() >= impl_->cfg.queue_max) {
            impl_->queue.pop_front(); // the oldest goes: the newest bars matter most live
            ++impl_->st.dropped;
        }
        seq = ++impl_->next_seq; // starts at 1, so 0 means "nothing yet"
        impl_->queue.push_back({seq, Clock::now(), bar});
    }
    impl_->cv.notify_one();
    return seq;
}

std::uint64_t BarStore::written_through() const {
    std::lock_guard lock(impl_->mu);
    // The queue leaves in order: everything before its head is gone.
    return impl_->queue.empty() ? impl_->next_seq : impl_->queue.front().seq - 1;
}

BarStoreStats BarStore::stats() const {
    BarStoreStats out;
    {
        std::lock_guard lock(impl_->mu);
        out = impl_->st;
        out.queued = impl_->queue.size();
    }
    out.pool = impl_->pool.stats();
    return out;
}

Pool<pg::Connection>& BarStore::pool() {
    return impl_->pool;
}

} // namespace fmdg
