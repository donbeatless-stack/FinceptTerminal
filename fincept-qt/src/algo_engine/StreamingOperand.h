// src/algo_engine/StreamingOperand.h
#pragma once
#include "algo_engine/AlgoEngineTypes.h"

#include <QJsonObject>
#include <QString>

#include <memory>
#include <utility>

namespace fincept::algo {

/// One condition operand — `field` of `indicator(params)` — as incremental state
/// over a stream of candles, built on StreamingIndicators. This is the one place
/// that maps IndicatorEngine's indicator names, parameter keys and defaults, and
/// output fields onto the streaming classes; SeriesOperandSource (backtests) and
/// LiveOperandSource (realtime scans) both go through it.
///
/// Price attributes, SMA, EMA, RSI, ATR, MACD, Bollinger, Donchian, Stochastic and
/// Williams %R are covered. VWAP and OBV are deliberately not: their values are
/// cumulative from the first bar of whatever window the batch engine is given.
class StreamingOperand {
  public:
    virtual ~StreamingOperand() = default;

    /// nullptr when this (indicator, params, field) has no streaming implementation.
    static std::unique_ptr<StreamingOperand> create(const QString& indicator, const QJsonObject& params,
                                                    const QString& field);

    virtual void update(const OhlcvCandle& bar) = 0;
    /// NaN while warming up.
    virtual double value() const = 0;
    /// An independent copy of the current state, e.g. to try bars without
    /// committing them.
    virtual std::unique_ptr<StreamingOperand> clone() const = 0;

    /// IndicatorEngine's error message for this indicator while it warms up.
    const QString& warmup_error() const { return warmup_error_; }

  protected:
    explicit StreamingOperand(QString warmup_error) : warmup_error_(std::move(warmup_error)) {}
    StreamingOperand(const StreamingOperand&) = default;
    StreamingOperand& operator=(const StreamingOperand&) = delete;

  private:
    QString warmup_error_;
};

} // namespace fincept::algo
