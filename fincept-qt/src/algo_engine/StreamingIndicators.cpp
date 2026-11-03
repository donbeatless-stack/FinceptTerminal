// src/algo_engine/StreamingIndicators.cpp
//
// Where a class mirrors an IndicatorEngine routine, the arithmetic below is kept
// in the same order as the batch code it shadows (named per class) so the two
// agree bit for bit. Reordering "equivalent" expressions breaks that guarantee —
// tests/tst_streaming_indicators.cpp will catch it.
#include "algo_engine/StreamingIndicators.h"

#include <algorithm>
#include <cmath>

namespace fincept::algo::streaming {

namespace {

int at_least(int v, int lo) {
    return v >= lo ? v : lo;
}

bool all_finite(double x) {
    return std::isfinite(x);
}

bool all_finite(double a, double b, double c) {
    return std::isfinite(a) && std::isfinite(b) && std::isfinite(c);
}

} // namespace

// ── RingBuffer ──────────────────────────────────────────────────────────────

RingBuffer::RingBuffer(int capacity) : buf_(static_cast<std::size_t>(at_least(capacity, 1)), 0.0) {}

void RingBuffer::push(double x) {
    const int cap = capacity();
    if (size_ < cap) {
        buf_[static_cast<std::size_t>((head_ + size_) % cap)] = x;
        ++size_;
    } else {
        buf_[static_cast<std::size_t>(head_)] = x;
        head_ = (head_ + 1) % cap;
    }
}

double RingBuffer::at(int i) const {
    return buf_[static_cast<std::size_t>((head_ + i) % capacity())];
}

// ── RollingMoments ──────────────────────────────────────────────────────────

RollingMoments::RollingMoments(int window)
    : values_(at_least(window, 1)), reanchor_every_(std::max(1024, at_least(window, 1))) {}

void RollingMoments::push(double x) {
    run_ = (values_.size() > 0 && x == last_) ? run_ + 1 : 1;
    last_ = x;

    if (!values_.full()) {
        values_.push(x);
        const double delta = x - mean_;
        mean_ += delta / values_.size();
        m2_ += delta * (x - mean_);
    } else {
        const double old = values_.oldest();
        values_.push(x);
        const double old_mean = mean_;
        mean_ += (x - old) / values_.size();
        m2_ += (x - old) * (x - mean_ + old - old_mean);
    }

    if (run_ >= values_.capacity()) {
        // Every sample in the window is x: the answer is exact, and so is the
        // state we restart from — no error carried out of the flat stretch.
        mean_ = x;
        m2_ = 0.0;
        since_anchor_ = 0;
        return;
    }
    if (++since_anchor_ >= reanchor_every_)
        reanchor();
}

void RollingMoments::reanchor() {
    const int n = values_.size();
    double sum = 0.0;
    for (int i = 0; i < n; ++i)
        sum += values_.at(i);
    mean_ = sum / n;
    double m2 = 0.0;
    for (int i = 0; i < n; ++i) {
        const double d = values_.at(i) - mean_;
        m2 += d * d;
    }
    m2_ = m2;
    since_anchor_ = 0;
}

double RollingMoments::variance_population() const {
    const int n = count();
    if (n < 1)
        return kNaN;
    return std::max(0.0, m2_ / n);
}

double RollingMoments::variance_sample() const {
    const int n = count();
    if (n < 2)
        return kNaN;
    return std::max(0.0, m2_ / (n - 1));
}

// ── RollingExtrema ──────────────────────────────────────────────────────────

RollingExtrema::RollingExtrema(int window) : window_(at_least(window, 1)) {}

void RollingExtrema::push(double high, double low) {
    const std::int64_t idx = pushed_++;
    while (!maxq_.empty() && maxq_.back().second <= high)
        maxq_.pop_back();
    maxq_.emplace_back(idx, high);
    while (!minq_.empty() && minq_.back().second >= low)
        minq_.pop_back();
    minq_.emplace_back(idx, low);

    const std::int64_t expired = idx - window_;
    while (maxq_.front().first <= expired)
        maxq_.pop_front();
    while (minq_.front().first <= expired)
        minq_.pop_front();
}

// ── Sma — mirrors IndicatorEngine::sma_series ───────────────────────────────

Sma::Sma(int period) : window_(at_least(period, 1)) {}

void Sma::update(double x) {
    if (!all_finite(x))
        return;
    if (window_.full())
        sum_ += x - window_.oldest();
    else
        sum_ += x;
    window_.push(x);
}

double Sma::value() const {
    return ready() ? sum_ / window_.capacity() : kNaN;
}

// ── Ema — mirrors IndicatorEngine::ema_series ───────────────────────────────

Ema::Ema(int period) : period_(at_least(period, 1)), mult_(2.0 / (at_least(period, 1) + 1)) {}

void Ema::update(double x) {
    if (!all_finite(x))
        return;
    if (count_ < period_) {
        value_ += x;
        if (++count_ == period_)
            value_ = value_ / period_;
        return;
    }
    value_ = (x - value_) * mult_ + value_;
}

// ── Rsi — mirrors IndicatorEngine::compute_rsi ──────────────────────────────

Rsi::Rsi(int period) : period_(at_least(period, 1)) {}

void Rsi::update(double close) {
    if (!all_finite(close))
        return;
    if (!has_prev_) {
        has_prev_ = true;
        prev_close_ = close;
        return;
    }
    const double diff = close - prev_close_;
    prev_close_ = close;
    const double gain = diff > 0 ? diff : 0.0;
    const double loss = diff > 0 ? 0.0 : -diff;

    if (diffs_ < period_) {
        avg_gain_ += gain;
        avg_loss_ += loss;
        if (++diffs_ < period_)
            return;
        avg_gain_ /= period_;
        avg_loss_ /= period_;
    } else {
        avg_gain_ = (avg_gain_ * (period_ - 1) + gain) / period_;
        avg_loss_ = (avg_loss_ * (period_ - 1) + loss) / period_;
    }
    value_ = avg_loss_ < 1e-10 ? 100.0 : 100.0 - 100.0 / (1.0 + avg_gain_ / avg_loss_);
}

// ── Macd — mirrors IndicatorEngine::compute_macd ────────────────────────────

Macd::Macd(int fast, int slow, int signal) : fast_(fast), slow_(slow), signal_(signal) {}

void Macd::update(double close) {
    // Guard here, not only in the EMAs: on a rejected tick the EMAs would be
    // unchanged and the signal line would be fed the same MACD value twice.
    if (!all_finite(close))
        return;
    fast_.update(close);
    slow_.update(close);
    if (fast_.ready() && slow_.ready()) {
        line_ = fast_.value() - slow_.value();
        signal_.update(line_);
    }
}

// ── Stochastic — mirrors IndicatorEngine::compute_stochastic ────────────────

Stochastic::Stochastic(int k_period, int d_period) : extrema_(k_period), d_sma_(d_period) {}

void Stochastic::update(double high, double low, double close) {
    if (!all_finite(high, low, close))
        return;
    extrema_.push(high, low);
    if (!extrema_.full())
        return;
    const double hh = extrema_.max();
    const double ll = extrema_.min();
    const double range = hh - ll;
    k_ = range > 1e-10 ? (close - ll) / range * 100.0 : 50.0;
    d_sma_.update(k_);
}

double Stochastic::d() const {
    if (!ready())
        return kNaN;
    return d_sma_.ready() ? d_sma_.value() : k_;
}

// ── WilliamsR — mirrors IndicatorEngine::compute_williams_r ─────────────────

WilliamsR::WilliamsR(int period) : extrema_(period) {}

void WilliamsR::update(double high, double low, double close) {
    if (!all_finite(high, low, close))
        return;
    extrema_.push(high, low);
    if (!extrema_.full())
        return;
    const double hh = extrema_.max();
    const double ll = extrema_.min();
    const double range = hh - ll;
    value_ = range > 1e-10 ? (hh - close) / range * -100.0 : -50.0;
}

// ── Atr — mirrors IndicatorEngine::compute_atr / true_range_series ──────────

Atr::Atr(int period) : period_(at_least(period, 1)) {}

void Atr::update(double high, double low, double close) {
    if (!all_finite(high, low, close))
        return;
    if (!has_prev_) {
        has_prev_ = true;
        prev_close_ = close;
        return;
    }
    const double hl = high - low;
    const double hc = std::abs(high - prev_close_);
    const double lc = std::abs(low - prev_close_);
    const double tr = std::max({hl, hc, lc});
    prev_close_ = close;

    if (ranges_ < period_) {
        atr_ += tr;
        if (++ranges_ == period_)
            atr_ /= period_;
        return;
    }
    atr_ = (atr_ * (period_ - 1) + tr) / period_;
}

// ── Bollinger — formulas of IndicatorEngine::compute_bollinger ──────────────

Bollinger::Bollinger(int period, double std_dev_mult) : moments_(period), mult_(std_dev_mult) {}

void Bollinger::update(double close) {
    if (!all_finite(close))
        return;
    moments_.push(close);
    last_close_ = close;
}

Bollinger::Bands Bollinger::bands() const {
    Bands b;
    if (!ready())
        return b;
    const double mid = moments_.mean();
    const double sd = std::sqrt(moments_.variance_population());
    b.middle = mid;
    b.upper = mid + mult_ * sd;
    b.lower = mid - mult_ * sd;
    b.width = mid > 1e-10 ? (b.upper - b.lower) / mid : 0;
    b.pct_b = (b.upper - b.lower) > 1e-10 ? (last_close_ - b.lower) / (b.upper - b.lower) : 0.5;
    return b;
}

// ── Donchian ────────────────────────────────────────────────────────────────

Donchian::Donchian(int period) : extrema_(period) {}

void Donchian::update(double high, double low) {
    if (!all_finite(high) || !all_finite(low))
        return;
    extrema_.push(high, low);
}

// ── ZScore ──────────────────────────────────────────────────────────────────

ZScore::ZScore(int period) : moments_(period) {}

void ZScore::update(double x) {
    if (!all_finite(x))
        return;
    moments_.push(x);
    last_ = x;
}

double ZScore::value() const {
    if (!ready())
        return kNaN;
    const double mean = moments_.mean();
    const double sd = std::sqrt(moments_.variance_population());
    if (sd <= 1e-9 * std::max(1.0, std::abs(mean)))
        return 0.0;
    return (last_ - mean) / sd;
}

// ── RealizedVolatility ──────────────────────────────────────────────────────

RealizedVolatility::RealizedVolatility(int period, double periods_per_year)
    : returns_(at_least(period, 2)),
      annualizer_(std::sqrt(std::isfinite(periods_per_year) && periods_per_year > 0 ? periods_per_year : 252.0)) {}

void RealizedVolatility::update(double close) {
    if (!all_finite(close) || close <= 0.0)
        return;
    if (has_prev_)
        returns_.push(std::log(close / prev_close_));
    has_prev_ = true;
    prev_close_ = close;
}

double RealizedVolatility::value() const {
    if (!ready())
        return kNaN;
    return std::sqrt(returns_.variance_sample()) * annualizer_;
}

// ── Obv — mirrors IndicatorEngine::compute_obv ──────────────────────────────

void Obv::update(double close, double volume) {
    if (!all_finite(close) || !all_finite(volume))
        return;
    if (has_prev_) {
        if (close > prev_close_)
            obv_ += volume;
        else if (close < prev_close_)
            obv_ -= volume;
    }
    has_prev_ = true;
    prev_close_ = close;
}

} // namespace fincept::algo::streaming
