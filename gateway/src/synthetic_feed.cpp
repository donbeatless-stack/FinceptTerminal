// gateway/src/synthetic_feed.cpp
#include "fmdg/synthetic_feed.h"

#include "fmdg/gateway.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <format>
#include <random>
#include <vector>

namespace fmdg {

SyntheticFeed::SyntheticFeed(Gateway& gateway, SyntheticFeedConfig config)
    : gateway_(gateway), config_(std::move(config)) {}

SyntheticFeed::~SyntheticFeed() {
    stop();
}

void SyntheticFeed::start() {
    if (running_.exchange(true))
        return;
    thread_ = std::thread([this] { run(); });
}

void SyntheticFeed::stop() {
    running_.store(false);
    if (thread_.joinable())
        thread_.join();
}

void SyntheticFeed::run() {
    std::vector<SymbolId> ids;
    std::vector<double> prices;
    for (std::size_t i = 0; i < config_.symbols; ++i) {
        const auto id = gateway_.symbols().intern(std::format("{}{:04}", config_.prefix, i));
        if (!id)
            break; // symbol table full: run with what fit
        ids.push_back(*id);
        prices.push_back(20.0 + static_cast<double>(i % 480));
    }
    if (ids.empty())
        return;

    std::mt19937_64 rng(config_.seed);
    std::uniform_int_distribution<std::size_t> pick(0, ids.size() - 1);
    std::normal_distribution<double> step(0.0, 2e-4); // ≈ 2 bp per tick
    std::uniform_int_distribution<int> lots(1, 20);

    using clock = std::chrono::steady_clock;
    const auto t0 = clock::now();
    const bool paced = config_.rate > 0;
    std::uint64_t n = 0;

    while (running_.load(std::memory_order_relaxed)) {
        if (paced) {
            // Emit whatever is due by now, so the average rate holds even when
            // the OS wakes us late; then wait for the next tick's due time.
            const double elapsed = std::chrono::duration<double>(clock::now() - t0).count();
            const auto due = static_cast<std::uint64_t>(elapsed * config_.rate);
            if (n >= due) {
                const double wait_s = static_cast<double>(n + 1) / config_.rate - elapsed;
                if (wait_s > 0.002)
                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                else
                    std::this_thread::yield();
                continue;
            }
        }
        const std::size_t k = pick(rng);
        prices[k] = std::max(0.01, prices[k] * std::exp(step(rng)));
        Tick t;
        t.symbol = ids[k];
        t.price = std::round(prices[k] * 100.0) / 100.0; // cents
        t.size = 100.0 * lots(rng);
        t.exchange_ts_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
                               std::chrono::system_clock::now().time_since_epoch())
                               .count();
        gateway_.ingest(t);
        ++n;
        produced_.store(n, std::memory_order_relaxed);
    }
}

} // namespace fmdg
