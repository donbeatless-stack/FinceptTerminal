// src/algo_engine/SeriesOperandSource.cpp
#include "algo_engine/SeriesOperandSource.h"

#include "algo_engine/StreamingIndicators.h"


#include <algorithm>
#include <cmath>
#include <initializer_list>
#include <limits>
#include <utility>

namespace fincept::algo {

namespace {

// Names below carry a sos_ prefix: unity builds merge this file's anonymous
// namespace with its neighbours' (ConditionEvaluator, IndicatorEngine, ...).

// IndicatorEngine::compute's parameter parsing — same keys, defaults and clamp.
int sos_period(const QJsonObject& p, const char* key, int fallback) {
    const int v = p.value(QLatin1String(key)).toInt(fallback);
    return v >= 1 ? v : 1;
}

// Runs `ind` over every candle and records `read(ind)` after each one.
template <class Ind, class Feed, class Read>
QVector<double> sos_run(const QVector<OhlcvCandle>& candles, Ind ind, Feed feed, Read read) {
    QVector<double> out(candles.size());
    for (qsizetype i = 0; i < candles.size(); ++i) {
        feed(ind, candles[i]);
        out[i] = read(ind);
    }
    return out;
}

bool sos_one_of(const QString& field, std::initializer_list<const char*> names) {
    for (const char* n : names)
        if (field == QLatin1String(n))
            return true;
    return false;
}

// Full-history series for (indicator, params, field), or false when there is no
// streaming implementation for that combination. `warmup_error` is the message
// IndicatorEngine reports while the indicator is still warming up.
bool sos_build(const QString& ind, const QJsonObject& p, const QString& field, const QVector<OhlcvCandle>& c,
               QVector<double>* out, QString* warmup_error) {
    const auto feed_close = [](auto& s, const OhlcvCandle& k) { s.update(k.close); };
    const auto feed_hlc = [](auto& s, const OhlcvCandle& k) { s.update(k.high, k.low, k.close); };
    const auto read_value = [](const auto& s) { return s.value(); };
    const int period = sos_period(p, "period", 14);

    if (field == QLatin1String("value")) {
        // Price attributes. VWAP is deliberately absent: it is cumulative over the
        // window the batch engine is handed, so it keeps the window path.
        double OhlcvCandle::*attr = nullptr;
        if (ind == "CLOSE")
            attr = &OhlcvCandle::close;
        else if (ind == "OPEN")
            attr = &OhlcvCandle::open;
        else if (ind == "HIGH")
            attr = &OhlcvCandle::high;
        else if (ind == "LOW")
            attr = &OhlcvCandle::low;
        else if (ind == "VOLUME")
            attr = &OhlcvCandle::volume;
        if (attr) {
            out->resize(c.size());
            for (qsizetype i = 0; i < c.size(); ++i)
                (*out)[i] = c[i].*attr;
            *warmup_error = QStringLiteral("Insufficient data");
            return true;
        }

        if (ind == "SMA") {
            *out = sos_run(c, streaming::Sma(period), feed_close, read_value);
            *warmup_error = QStringLiteral("Insufficient data for SMA");
            return true;
        }
        if (ind == "EMA") {
            *out = sos_run(c, streaming::Ema(period), feed_close, read_value);
            *warmup_error = QStringLiteral("Insufficient data for EMA");
            return true;
        }
        if (ind == "RSI") {
            *out = sos_run(c, streaming::Rsi(period), feed_close, read_value);
            *warmup_error = QStringLiteral("Insufficient data for RSI");
            return true;
        }
        if (ind == "ATR") {
            *out = sos_run(c, streaming::Atr(period), feed_hlc, read_value);
            *warmup_error = QStringLiteral("Insufficient data for ATR");
            return true;
        }
        if (ind == "WILLIAMS_R") {
            *out = sos_run(c, streaming::WilliamsR(period), feed_hlc, read_value);
            *warmup_error = QStringLiteral("Insufficient data for Williams %R");
            return true;
        }
        return false;
    }

    if (ind == "MACD" && sos_one_of(field, {"line", "signal_line", "histogram"})) {
        const streaming::Macd macd(sos_period(p, "fast", 12), sos_period(p, "slow", 26), sos_period(p, "signal", 9));
        *out = sos_run(c, macd, feed_close, [&field](const streaming::Macd& m) {
            if (field == QLatin1String("line"))
                return m.line();
            if (field == QLatin1String("signal_line"))
                return m.signal();
            return m.histogram();
        });
        *warmup_error = QStringLiteral("Insufficient data for MACD");
        return true;
    }
    if (ind == "BOLLINGER" && sos_one_of(field, {"upper", "middle", "lower", "width", "pct_b"})) {
        const streaming::Bollinger bb(period, p.value(QLatin1String("std_dev")).toDouble(2.0));
        *out = sos_run(c, bb, feed_close, [&field](const streaming::Bollinger& b) {
            const auto bands = b.bands();
            if (field == QLatin1String("upper"))
                return bands.upper;
            if (field == QLatin1String("middle"))
                return bands.middle;
            if (field == QLatin1String("lower"))
                return bands.lower;
            if (field == QLatin1String("width"))
                return bands.width;
            return bands.pct_b;
        });
        *warmup_error = QStringLiteral("Insufficient data for Bollinger");
        return true;
    }
    if (ind == "DONCHIAN" && sos_one_of(field, {"upper", "lower"})) {
        const bool upper = field == QLatin1String("upper");
        *out = sos_run(
            c, streaming::Donchian(period), [](streaming::Donchian& d, const OhlcvCandle& k) { d.update(k.high, k.low); },
            [upper](const streaming::Donchian& d) { return upper ? d.upper() : d.lower(); });
        *warmup_error = QStringLiteral("Insufficient data for Donchian");
        return true;
    }
    if (ind == "STOCHASTIC" && sos_one_of(field, {"k", "d"})) {
        const bool k_line = field == QLatin1String("k");
        const streaming::Stochastic sto(sos_period(p, "k_period", 14), sos_period(p, "d_period", 3));
        *out = sos_run(c, sto, feed_hlc, [k_line](const streaming::Stochastic& s) { return k_line ? s.k() : s.d(); });
        *warmup_error = QStringLiteral("Insufficient data for Stochastic");
        return true;
    }
    return false;
}

} // namespace

SeriesOperandSource::SeriesOperandSource(const QVector<OhlcvCandle>& candles, int window)
    : candles_(&candles), window_(std::max(1, window)) {}

double SeriesOperandSource::value(const QString& indicator, const QJsonObject& params, const QString& field,
                                  int offset, QString* error) {
    const Series& s = series_for(indicator, params, field);
    if (!s.streamed) {
        WindowOperandSource window(trailing_window());
        return window.value(indicator, params, field, offset, error);
    }

    // Same checks, in the same order and with the same messages, as
    // WindowOperandSource over the full history up to bar_.
    constexpr double nan = std::numeric_limits<double>::quiet_NaN();
    if (offset < 0)
        offset = 0;
    if (offset > bar_) {
        if (error)
            *error = QStringLiteral("insufficient data for offset %1").arg(offset);
        return nan;
    }
    const int idx = bar_ - offset;
    if (idx < 1 || idx >= s.values.size()) { // IndicatorEngine refuses a single candle
        if (error)
            *error = QStringLiteral("Insufficient data");
        return nan;
    }
    const double v = s.values[idx];
    if (std::isnan(v)) {
        if (error)
            *error = s.warmup_error;
        return nan;
    }
    if (!std::isfinite(v)) {
        if (error)
            *error = QStringLiteral("%1 produced a non-finite value").arg(indicator);
        return nan;
    }
    return v;
}

const SeriesOperandSource::Series& SeriesOperandSource::series_for(const QString& indicator,
                                                                   const QJsonObject& params, const QString& field) {
    for (const Series& s : cache_)
        if (s.indicator == indicator && s.field == field && s.params == params)
            return s;

    Series s;
    s.indicator = indicator;
    s.params = params;
    s.field = field;
    s.streamed = sos_build(indicator, params, field, *candles_, &s.values, &s.warmup_error);
    ++(s.streamed ? streamed_ : windowed_);
    cache_.push_back(std::move(s));
    return cache_.back();
}

const QVector<OhlcvCandle>& SeriesOperandSource::trailing_window() {
    if (window_bar_ != bar_) {
        const int start = std::max(0, bar_ - window_ + 1);
        window_candles_ = candles_->mid(start, bar_ - start + 1);
        window_bar_ = bar_;
    }
    return window_candles_;
}

} // namespace fincept::algo
