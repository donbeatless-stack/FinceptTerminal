// src/algo_engine/OperandSource.h
#pragma once
#include "algo_engine/AlgoEngineTypes.h"

#include <QJsonObject>
#include <QString>
#include <QVector>

namespace fincept::algo {

/// Supplies the indicator values strategy conditions compare. ConditionEvaluator
/// asks for "`field` of `indicator(params)`, `offset` bars before the current
/// bar"; how the value is produced is up to the source.
class OperandSource {
  public:
    virtual ~OperandSource() = default;

    /// Returns NaN and sets `*error` (when non-null) on failure, insufficient
    /// data or a non-finite indicator value, so a returned finite value is always
    /// safe to compare.
    virtual double value(const QString& indicator, const QJsonObject& params, const QString& field, int offset,
                         QString* error) = 0;
};

/// The original evaluation path: recompute the indicator with IndicatorEngine on
/// `candles` (last element = current bar), truncated by `offset` bars. Used by
/// live deployments and scanners, and by SeriesOperandSource as its fallback.
/// Holds a pointer to `candles`, which must outlive it.
class WindowOperandSource final : public OperandSource {
  public:
    explicit WindowOperandSource(const QVector<OhlcvCandle>& candles) : candles_(&candles) {}

    double value(const QString& indicator, const QJsonObject& params, const QString& field, int offset,
                 QString* error) override;

  private:
    const QVector<OhlcvCandle>* candles_;
};

} // namespace fincept::algo
