// Benchmark: per-bar indicator cost, batch recompute vs streaming update.
//
// BacktestEngine evaluates conditions on every bar by copying the last
// kEvalWindow (500) candles and calling IndicatorEngine::compute() on that copy,
// so each indicator costs O(window) per bar plus the copy. The streaming classes
// fold in one bar in O(1). This program replays the same random walk through
// both and reports, per indicator, nanoseconds per bar, the speedup and the
// largest difference between the two outputs.
//
// The difference column is a finding, not noise. Finite-memory indicators (SMA,
// Bollinger, Donchian, Stochastic, Williams %R) see the same bars either way and
// agree. Recursive ones (EMA, RSI, ATR, MACD) recomputed over a 500-bar window are
// re-seeded at the window start on every bar, so the batch value only
// approximates the full-history one; the seed's weight decays like
// (1 - alpha)^(500 - period) — nothing for EMA(20), about 5% for EMA(200).
// The streaming value is the full-history value.
//
// Not registered with ctest: timings are machine-dependent. Usage:
//   bench_streaming_indicators [bars]        (default 20000)

#include "algo_engine/AlgoEngineTypes.h"
#include "algo_engine/IndicatorEngine.h"
#include "algo_engine/StreamingIndicators.h"

#include <QJsonObject>
#include <QString>
#include <QVector>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <vector>

using fincept::algo::IndicatorEngine;
using fincept::algo::IndicatorResult;
using fincept::algo::OhlcvCandle;
namespace st = fincept::algo::streaming;

namespace {

constexpr int kEvalWindow = 500;   // BacktestEngine.cpp
constexpr int kStreamRepeats = 25; // the O(1) loop is too fast to time in one pass

QVector<OhlcvCandle> make_walk(int n) {
    std::mt19937_64 rng(42);
    std::normal_distribution<double> ret(0.0, 0.01);
    std::uniform_real_distribution<double> wick(0.0, 0.004);
    std::uniform_real_distribution<double> vol(1'000.0, 5'000.0);
    QVector<OhlcvCandle> out;
    out.reserve(n);
    double close = 100.0;
    for (int i = 0; i < n; ++i) {
        OhlcvCandle c;
        c.open_time = c.close_time = i;
        c.open = close;
        close *= std::exp(ret(rng));
        c.close = close;
        c.high = std::max(c.open, c.close) * (1.0 + wick(rng));
        c.low = std::min(c.open, c.close) * (1.0 - wick(rng));
        c.volume = vol(rng);
        c.is_closed = true;
        out.append(c);
    }
    return out;
}

double ns_per_bar(std::chrono::steady_clock::duration d, int bars) {
    return std::chrono::duration<double, std::nano>(d).count() / bars;
}

// `make` builds a fresh streaming indicator; `step` feeds it one bar and returns
// its current value (NaN while warming up).
template <class Make, class Step>
void run_case(const char* label, const QVector<OhlcvCandle>& bars, const QString& name, const QJsonObject& params,
              const QString& field, Make make, Step step) {
    using clock = std::chrono::steady_clock;
    const int n = static_cast<int>(bars.size());
    std::vector<double> batch(static_cast<std::size_t>(n), st::kNaN);
    std::vector<double> stream(static_cast<std::size_t>(n), st::kNaN);

    // Batch path, exactly as BacktestEngine runs it: window copy + full recompute.
    const auto b0 = clock::now();
    for (int i = 0; i < n; ++i) {
        const int start = std::max(0, i - kEvalWindow + 1);
        const QVector<OhlcvCandle> window = bars.mid(start, i - start + 1);
        const IndicatorResult r = IndicatorEngine::compute(name, window, params, field);
        batch[static_cast<std::size_t>(i)] = r.valid ? r.current.value(field, st::kNaN) : st::kNaN;
    }
    const auto batch_time = clock::now() - b0;

    // Streaming path: best of kStreamRepeats fresh passes.
    auto stream_time = clock::duration::max();
    for (int rep = 0; rep < kStreamRepeats; ++rep) {
        auto ind = make();
        const auto s0 = clock::now();
        for (int i = 0; i < n; ++i)
            stream[static_cast<std::size_t>(i)] = step(ind, bars[i]);
        stream_time = std::min(stream_time, clock::now() - s0);
    }

    double max_diff = 0.0;
    int compared = 0;
    for (std::size_t i = 1; i < static_cast<std::size_t>(n); ++i) { // batch refuses one candle
        if (std::isfinite(batch[i]) && std::isfinite(stream[i])) {
            max_diff = std::max(max_diff, std::abs(batch[i] - stream[i]));
            ++compared;
        }
    }

    const double bns = ns_per_bar(batch_time, n);
    const double sns = ns_per_bar(stream_time, n);
    std::printf("%-20s %14.1f %14.2f %11.0fx %18.3g %9d\n", label, bns, sns, bns / sns, max_diff, compared);
}

} // namespace

int main(int argc, char** argv) {
    const int bars = argc > 1 ? std::max(kEvalWindow * 2, std::atoi(argv[1])) : 20'000;
    const QVector<OhlcvCandle> walk = make_walk(bars);

    std::printf("Streaming vs batch indicators: %d bars, batch window %d (BacktestEngine::kEvalWindow), "
                "price level ~100\n\n",
                bars, kEvalWindow);
    std::printf("%-20s %14s %14s %12s %18s %9s\n", "indicator", "batch ns/bar", "stream ns/bar", "speedup",
                "max |batch-stream|", "bars");

    run_case(
        "SMA(20)", walk, "SMA", {{"period", 20}}, "value", [] { return st::Sma(20); },
        [](st::Sma& s, const OhlcvCandle& c) {
            s.update(c.close);
            return s.value();
        });
    run_case(
        "EMA(20)", walk, "EMA", {{"period", 20}}, "value", [] { return st::Ema(20); },
        [](st::Ema& s, const OhlcvCandle& c) {
            s.update(c.close);
            return s.value();
        });
    run_case(
        "EMA(200)", walk, "EMA", {{"period", 200}}, "value", [] { return st::Ema(200); },
        [](st::Ema& s, const OhlcvCandle& c) {
            s.update(c.close);
            return s.value();
        });
    run_case(
        "RSI(14)", walk, "RSI", {{"period", 14}}, "value", [] { return st::Rsi(14); },
        [](st::Rsi& s, const OhlcvCandle& c) {
            s.update(c.close);
            return s.value();
        });
    run_case(
        "ATR(14)", walk, "ATR", {{"period", 14}}, "value", [] { return st::Atr(14); },
        [](st::Atr& s, const OhlcvCandle& c) {
            s.update(c.high, c.low, c.close);
            return s.value();
        });
    run_case(
        "MACD(12,26,9) line", walk, "MACD", {{"fast", 12}, {"slow", 26}, {"signal", 9}}, "line",
        [] { return st::Macd(12, 26, 9); },
        [](st::Macd& s, const OhlcvCandle& c) {
            s.update(c.close);
            return s.line();
        });
    run_case(
        "BOLLINGER(20,2) up", walk, "BOLLINGER", {{"period", 20}, {"std_dev", 2.0}}, "upper",
        [] { return st::Bollinger(20, 2.0); },
        [](st::Bollinger& s, const OhlcvCandle& c) {
            s.update(c.close);
            return s.bands().upper;
        });
    run_case(
        "DONCHIAN(20) upper", walk, "DONCHIAN", {{"period", 20}}, "upper", [] { return st::Donchian(20); },
        [](st::Donchian& s, const OhlcvCandle& c) {
            s.update(c.high, c.low);
            return s.upper();
        });
    run_case(
        "STOCHASTIC(14,3) %K", walk, "STOCHASTIC", {{"k_period", 14}, {"d_period", 3}}, "k",
        [] { return st::Stochastic(14, 3); },
        [](st::Stochastic& s, const OhlcvCandle& c) {
            s.update(c.high, c.low, c.close);
            return s.k();
        });
    run_case(
        "WILLIAMS_R(14)", walk, "WILLIAMS_R", {{"period", 14}}, "value", [] { return st::WilliamsR(14); },
        [](st::WilliamsR& s, const OhlcvCandle& c) {
            s.update(c.high, c.low, c.close);
            return s.value();
        });
    return 0;
}
