// gateway/include/fmdg/synthetic_feed.h
#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>

namespace fmdg {

class Gateway;

struct SyntheticFeedConfig {
    std::size_t symbols = 100;   ///< named <prefix>0000, <prefix>0001, …
    double rate = 10'000;        ///< ticks per second across all symbols
    std::uint64_t seed = 42;
    std::string prefix = "SYM";
};

/// Deterministic random-walk feed, paced to `rate`, for demos, tests and load
/// tests. It is the gateway's single producer: run one per gateway, and nothing
/// else may call Gateway::ingest() meanwhile.
class SyntheticFeed {
  public:
    SyntheticFeed(Gateway& gateway, SyntheticFeedConfig config);
    ~SyntheticFeed();

    SyntheticFeed(const SyntheticFeed&) = delete;
    SyntheticFeed& operator=(const SyntheticFeed&) = delete;

    void start();
    void stop();
    std::uint64_t produced() const { return produced_.load(std::memory_order_relaxed); }

  private:
    void run();

    Gateway& gateway_;
    const SyntheticFeedConfig config_;
    std::atomic<bool> running_{false};
    std::atomic<std::uint64_t> produced_{0};
    std::thread thread_;
};

} // namespace fmdg
