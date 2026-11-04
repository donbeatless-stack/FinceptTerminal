// gateway/include/fmdg/tick.h
#pragma once

#include <chrono>
#include <cstdint>

namespace fmdg {

using SymbolId = std::uint32_t;

/// One normalised market-data update. Trivially copyable: it moves through the
/// ingest ring by value and is serialised field by field (protocol.h).
struct Tick {
    SymbolId symbol = 0;
    std::uint8_t flags = 0;          ///< kTickSnapshot; sits in padding, size unchanged
    std::uint64_t sequence = 0;      ///< gateway-wide, strictly increasing
    double price = 0.0;
    double size = 0.0;
    std::int64_t exchange_ts_ns = 0; ///< the source's own timestamp
    std::int64_t ingest_ts_ns = 0;   ///< now_ns() when the gateway took it in
};

/// Tick::flags: the symbol's last value, sent when a client subscribes — not a
/// new update, and possibly old, so it is kept out of latency statistics.
inline constexpr std::uint8_t kTickSnapshot = 1;

/// Monotonic nanoseconds. steady_clock is QueryPerformanceCounter on Windows and
/// CLOCK_MONOTONIC on Linux; both are machine-wide, so a timestamp taken in the
/// gateway and one taken in a client process on the same host can be subtracted.
inline std::int64_t now_ns() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

} // namespace fmdg
