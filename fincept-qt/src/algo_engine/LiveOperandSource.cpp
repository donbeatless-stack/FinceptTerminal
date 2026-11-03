// src/algo_engine/LiveOperandSource.cpp
#include "algo_engine/LiveOperandSource.h"

#include <cmath>
#include <cstddef>
#include <limits>

namespace fincept::algo {

LiveOperandSource::LiveOperandSource(const std::vector<OperandRef>& operands) {
    for (const OperandRef& r : operands) {
        if (!StreamingOperand::create(r.indicator, r.params, r.field)) {
            needs_window_ = true;
            continue;
        }
        Tracked t;
        t.indicator = r.indicator;
        t.params = r.params;
        t.field = r.field;
        t.depth = r.max_offset + 1;
        t.recent.assign(static_cast<std::size_t>(t.depth), std::numeric_limits<double>::quiet_NaN());
        t.state = StreamingOperand::create(r.indicator, r.params, r.field);
        tracked_.push_back(std::move(t));
    }
}

void LiveOperandSource::push_recent(Tracked& t, double v) {
    t.recent_head = (t.recent_head + 1) % t.depth;
    t.recent[static_cast<std::size_t>(t.recent_head)] = v;
    if (t.recent_count < t.depth)
        ++t.recent_count;
}

void LiveOperandSource::reset(const QVector<OhlcvCandle>& history) {
    for (Tracked& t : tracked_) {
        t.state = StreamingOperand::create(t.indicator, t.params, t.field);
        t.recent_head = 0;
        t.recent_count = 0;
        for (const OhlcvCandle& c : history) {
            t.state->update(c);
            push_recent(t, t.state->value());
        }
        t.with_ticks_valid = false;
    }
    closed_ = history.size();
}

void LiveOperandSource::on_closed(const OhlcvCandle& candle) {
    for (Tracked& t : tracked_) {
        t.state->update(candle);
        push_recent(t, t.state->value());
        t.with_ticks_valid = false;
    }
    ++closed_;
}

void LiveOperandSource::set_ticks(const QVector<OhlcvCandle>& ticks) {
    ticks_ = ticks;
    for (Tracked& t : tracked_)
        t.with_ticks_valid = false;
}

LiveOperandSource::Tracked* LiveOperandSource::find(const QString& indicator, const QJsonObject& params,
                                                    const QString& field) {
    for (Tracked& t : tracked_)
        if (t.indicator == indicator && t.field == field && t.params == params)
            return &t;
    return nullptr;
}

double LiveOperandSource::from_window(const QString& indicator, const QJsonObject& params, const QString& field,
                                      int offset, QString* error) const {
    if (!window_) {
        if (error)
            *error = QStringLiteral("%1: no live candle window").arg(indicator);
        return std::numeric_limits<double>::quiet_NaN();
    }
    WindowOperandSource window(*window_);
    return window.value(indicator, params, field, offset, error);
}

double LiveOperandSource::value(const QString& indicator, const QJsonObject& params, const QString& field, int offset,
                                QString* error) {
    Tracked* t = find(indicator, params, field);
    if (!t)
        return from_window(indicator, params, field, offset, error);

    // Same checks, order and messages as WindowOperandSource over the full live
    // window (every closed candle + the tick bars).
    constexpr double nan = std::numeric_limits<double>::quiet_NaN();
    if (offset < 0)
        offset = 0;
    const std::int64_t n = bar_count();
    if (offset >= n) {
        if (error)
            *error = QStringLiteral("insufficient data for offset %1").arg(offset);
        return nan;
    }
    if (n - offset < 2) { // IndicatorEngine refuses a single candle
        if (error)
            *error = QStringLiteral("Insufficient data");
        return nan;
    }

    const int ticks = static_cast<int>(ticks_.size());
    double v;
    if (offset < ticks) {
        // A bar the ticks contribute: run them through a copy of the closed state.
        if (!t->with_ticks_valid) {
            auto trial = t->state->clone();
            t->with_ticks.resize(static_cast<std::size_t>(ticks));
            for (int k = 0; k < ticks; ++k) {
                trial->update(ticks_[k]);
                t->with_ticks[static_cast<std::size_t>(k)] = trial->value();
            }
            t->with_ticks_valid = true;
        }
        v = t->with_ticks[static_cast<std::size_t>(ticks - 1 - offset)];
    } else {
        const int back = offset - ticks; // 0 = the latest closed candle
        if (back >= t->recent_count)     // deeper than registered
            return from_window(indicator, params, field, offset, error);
        v = t->recent[static_cast<std::size_t>((t->recent_head - back + t->depth) % t->depth)];
    }

    if (std::isnan(v)) {
        if (error)
            *error = t->state->warmup_error();
        return nan;
    }
    if (!std::isfinite(v)) {
        if (error)
            *error = QStringLiteral("%1 produced a non-finite value").arg(indicator);
        return nan;
    }
    return v;
}

} // namespace fincept::algo
