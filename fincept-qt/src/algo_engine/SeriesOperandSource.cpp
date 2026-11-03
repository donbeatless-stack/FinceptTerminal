// src/algo_engine/SeriesOperandSource.cpp
#include "algo_engine/SeriesOperandSource.h"

#include "algo_engine/StreamingOperand.h"

#include <algorithm>
#include <cmath>
#include <limits>
#include <utility>

namespace fincept::algo {

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
    if (auto op = StreamingOperand::create(indicator, params, field)) {
        s.streamed = true;
        s.warmup_error = op->warmup_error();
        s.values.resize(candles_->size());
        for (qsizetype i = 0; i < candles_->size(); ++i) {
            op->update((*candles_)[i]);
            s.values[i] = op->value();
        }
    }
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
