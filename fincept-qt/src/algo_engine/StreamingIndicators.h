// src/algo_engine/StreamingIndicators.h
#pragma once
// Streaming (incremental) indicators: update() folds in ONE new bar in O(1)
// (amortised O(1) for the rolling extrema), where IndicatorEngine::compute()
// rebuilds the whole series on every evaluation — O(window) per bar per
// indicator, plus a candle-window copy, in BacktestEngine and the realtime scans.
//
// Contract with the batch engine — pinned bar by bar in
// tests/tst_streaming_indicators.cpp: fed the same bars in order, an indicator
// here reports the same value as IndicatorEngine::compute(...).current over that
// full history, with the same warm-up length and seeding (SMA-seeded EMA, Wilder
// RSI / ATR) and the same floating-point operation order. SMA, EMA, RSI, ATR,
// MACD, OBV and the rolling extrema therefore match to the last bit on IEEE
// doubles without FMA contraction; Bollinger (sliding Welford instead of a
// two-pass sum) agrees to ~1e-12 relative.
//
// Deliberate differences from the batch engine:
//   * A non-finite input (NaN / ±inf tick) is ignored, not folded in. The batch
//     engine rebuilds from scratch, so a bad bar only hurts while it is inside the
//     window; a recursive streaming state would carry it forever.
//   * Indicators are ready after their own warm-up even at one bar; the batch
//     dispatcher refuses fewer than two candles for every indicator.
//   * Periods below 1 are clamped to 1 (the batch dispatcher does the same).
//
// Plain C++20, no Qt. Not thread-safe: one instance per symbol per thread.

#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <utility>
#include <vector>

namespace fincept::algo::streaming {

inline constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();

// ── Building blocks ─────────────────────────────────────────────────────────

/// Fixed-capacity FIFO of the most recent `capacity` samples.
class RingBuffer {
  public:
    explicit RingBuffer(int capacity);

    /// Appends `x`; once full, the oldest sample is overwritten.
    void push(double x);

    int size() const { return size_; }
    int capacity() const { return static_cast<int>(buf_.size()); }
    bool full() const { return size_ == capacity(); }

    /// Oldest sample. Requires size() > 0.
    double oldest() const { return buf_[static_cast<std::size_t>(head_)]; }
    /// i = 0 is the oldest sample, size() - 1 the newest.
    double at(int i) const;

  private:
    std::vector<double> buf_;
    int head_ = 0; ///< index of the oldest sample
    int size_ = 0;
};

/// Mean and variance of the last `window` samples in O(1) per push
/// (sliding-window Welford update).
///
/// Sliding Welford accumulates rounding error proportional to the variance it has
/// seen, so two guards keep it honest: a window of identical samples (an illiquid
/// symbol printing no trades) is detected exactly and reports variance 0, and an
/// exact two-pass recompute runs every max(1024, window) pushes (amortised O(1)).
class RollingMoments {
  public:
    explicit RollingMoments(int window);

    void push(double x);

    int count() const { return values_.size(); }
    bool full() const { return values_.full(); }
    double mean() const { return count() > 0 ? mean_ : kNaN; }
    /// Σ(x - mean)² / n — population variance (Bollinger convention).
    double variance_population() const;
    /// Σ(x - mean)² / (n - 1) — sample variance.
    double variance_sample() const;

  private:
    void reanchor();

    RingBuffer values_;
    double mean_ = 0.0;
    double m2_ = 0.0;
    double last_ = 0.0;
    int run_ = 0; ///< length of the current run of identical samples
    int since_anchor_ = 0;
    int reanchor_every_;
};

/// Highest high and lowest low over the last `window` bars, amortised O(1) via
/// monotonic deques.
class RollingExtrema {
  public:
    explicit RollingExtrema(int window);

    void push(double high, double low);

    bool full() const { return pushed_ >= window_; }
    double max() const { return maxq_.empty() ? kNaN : maxq_.front().second; }
    double min() const { return minq_.empty() ? kNaN : minq_.front().second; }

  private:
    int window_;
    std::int64_t pushed_ = 0;
    std::deque<std::pair<std::int64_t, double>> maxq_; ///< (bar index, high), highs descending
    std::deque<std::pair<std::int64_t, double>> minq_; ///< (bar index, low), lows ascending
};

// ── Moving averages ─────────────────────────────────────────────────────────

class Sma {
  public:
    explicit Sma(int period);
    void update(double x);
    bool ready() const { return window_.full(); }
    double value() const;

  private:
    RingBuffer window_;
    double sum_ = 0.0;
};

/// EMA seeded with the SMA of the first `period` samples, multiplier 2/(period+1).
class Ema {
  public:
    explicit Ema(int period);
    void update(double x);
    bool ready() const { return count_ >= period_; }
    double value() const { return ready() ? value_ : kNaN; }

  private:
    int period_;
    double mult_;
    int count_ = 0;
    double value_ = 0.0; ///< seed sum during warm-up, then the EMA
};

// ── Momentum ────────────────────────────────────────────────────────────────

/// Wilder RSI: simple average of the first `period` gains/losses, then Wilder smoothing.
class Rsi {
  public:
    explicit Rsi(int period);
    void update(double close);
    bool ready() const { return diffs_ >= period_; }
    double value() const { return ready() ? value_ : kNaN; }

  private:
    int period_;
    bool has_prev_ = false;
    double prev_close_ = 0.0;
    int diffs_ = 0;
    double avg_gain_ = 0.0;
    double avg_loss_ = 0.0;
    double value_ = kNaN;
};

class Macd {
  public:
    Macd(int fast, int slow, int signal);
    void update(double close);
    bool ready() const { return signal_.ready(); }
    double line() const { return ready() ? line_ : kNaN; }
    double signal() const { return signal_.value(); }
    double histogram() const { return ready() ? line_ - signal_.value() : kNaN; }

  private:
    Ema fast_;
    Ema slow_;
    Ema signal_;
    double line_ = kNaN;
};

/// Stochastic oscillator: %K over `k_period` bars, %D = SMA(%K, d_period).
/// Until %D has warmed up it reports %K, as the batch engine does.
class Stochastic {
  public:
    Stochastic(int k_period, int d_period);
    void update(double high, double low, double close);
    bool ready() const { return extrema_.full(); }
    double k() const { return ready() ? k_ : kNaN; }
    double d() const;

  private:
    RollingExtrema extrema_;
    Sma d_sma_;
    double k_ = kNaN;
};

class WilliamsR {
  public:
    explicit WilliamsR(int period);
    void update(double high, double low, double close);
    bool ready() const { return extrema_.full(); }
    double value() const { return ready() ? value_ : kNaN; }

  private:
    RollingExtrema extrema_;
    double value_ = kNaN;
};

// ── Volatility ──────────────────────────────────────────────────────────────

/// Wilder ATR: simple average of the first `period` true ranges (the first bar has
/// no previous close and contributes none), then Wilder smoothing.
class Atr {
  public:
    explicit Atr(int period);
    void update(double high, double low, double close);
    bool ready() const { return ranges_ >= period_; }
    double value() const { return ready() ? atr_ : kNaN; }

  private:
    int period_;
    bool has_prev_ = false;
    double prev_close_ = 0.0;
    int ranges_ = 0;
    double atr_ = 0.0; ///< seed sum during warm-up, then the ATR
};

class Bollinger {
  public:
    struct Bands {
        double upper = kNaN;
        double middle = kNaN;
        double lower = kNaN;
        double width = kNaN; ///< (upper - lower) / middle
        double pct_b = kNaN; ///< (close - lower) / (upper - lower)
    };

    Bollinger(int period, double std_dev_mult);
    void update(double close);
    bool ready() const { return moments_.full(); }
    Bands bands() const;

  private:
    RollingMoments moments_;
    double mult_;
    double last_close_ = kNaN;
};

class Donchian {
  public:
    explicit Donchian(int period);
    void update(double high, double low);
    bool ready() const { return extrema_.full(); }
    double upper() const { return ready() ? extrema_.max() : kNaN; }
    double lower() const { return ready() ? extrema_.min() : kNaN; }

  private:
    RollingExtrema extrema_;
};

/// Rolling z-score: (x - mean) / sd over the last `period` samples, population sd
/// (the Bollinger convention, so z = ±k sits exactly on a k-sigma band). A window
/// whose sd is below 1e-9 of its price scale is treated as flat and reports 0.
class ZScore {
  public:
    explicit ZScore(int period);
    void update(double x);
    bool ready() const { return moments_.full(); }
    double value() const;

  private:
    RollingMoments moments_;
    double last_ = kNaN;
};

/// Annualised realised volatility: sample sd of the last `period` log returns
/// times sqrt(periods_per_year) — 252 for daily bars; pass the bar count per
/// trading year for intraday bars (e.g. 252 * 375 for NSE one-minute bars).
/// Needs period + 1 closes. Non-positive prices are ignored (no log return).
class RealizedVolatility {
  public:
    explicit RealizedVolatility(int period, double periods_per_year = 252.0);
    void update(double close);
    bool ready() const { return returns_.full(); }
    double value() const;

  private:
    RollingMoments returns_;
    double annualizer_;
    bool has_prev_ = false;
    double prev_close_ = 0.0;
};

// ── Volume ──────────────────────────────────────────────────────────────────

class Obv {
  public:
    void update(double close, double volume);
    bool ready() const { return has_prev_; }
    double value() const { return ready() ? obv_ : kNaN; }

  private:
    bool has_prev_ = false;
    double prev_close_ = 0.0;
    double obv_ = 0.0;
};

} // namespace fincept::algo::streaming
