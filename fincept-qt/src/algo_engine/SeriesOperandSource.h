// src/algo_engine/SeriesOperandSource.h
#pragma once
#include "algo_engine/OperandSource.h"

#include <QJsonObject>
#include <QString>
#include <QVector>

#include <vector>

namespace fincept::algo {

/// Operand source for replaying a fixed candle history bar by bar (BacktestEngine).
///
/// The first time an operand is requested, an indicator with a streaming
/// implementation (StreamingOperand) is run ONCE over the whole history and
/// its per-bar values are cached; every later request — any bar, any offset — is
/// an array read. That is O(N) per distinct operand for the whole replay, where
/// recomputing on a trailing window costs O(window) per bar per operand read,
/// plus a candle-window copy.
///
/// Served values are full-history values. For finite-memory indicators (SMA,
/// Bollinger, Donchian, Stochastic, Williams %R, price attributes) that is what
/// the trailing-window recompute returns too, whenever the window covers the
/// period. For recursive ones (EMA, RSI, ATR, MACD) the window recompute re-seeds
/// at the window start on every bar and only approximates them — negligibly for
/// short periods, visibly for long ones such as EMA(200). Error messages and
/// failure order match WindowOperandSource over the full history.
///
/// Everything else — other indicators, unknown fields, and VWAP / OBV (whose
/// value is defined relative to the first bar of whatever window they are given)
/// — falls back to WindowOperandSource over the trailing `window` candles,
/// exactly as BacktestEngine evaluated before.
///
/// Holds a pointer to `candles`, which must outlive it.
class SeriesOperandSource final : public OperandSource {
  public:
    SeriesOperandSource(const QVector<OhlcvCandle>& candles, int window);

    /// Index into `candles` of the bar being evaluated.
    void set_bar(int index) { bar_ = index; }

    double value(const QString& indicator, const QJsonObject& params, const QString& field, int offset,
                 QString* error) override;

    /// Distinct operands served from a precomputed series / by the window fallback.
    int streamed_operands() const { return streamed_; }
    int windowed_operands() const { return windowed_; }

  private:
    struct Series {
        QString indicator;
        QJsonObject params;
        QString field;
        bool streamed = false;  ///< false: serve through the window fallback
        QVector<double> values; ///< one per candle; NaN while warming up
        QString warmup_error;   ///< the batch engine's message for a warming bar
    };

    const Series& series_for(const QString& indicator, const QJsonObject& params, const QString& field);
    const QVector<OhlcvCandle>& trailing_window();

    const QVector<OhlcvCandle>* candles_;
    int window_;
    int bar_ = 0;
    // A strategy has a handful of distinct operands and each bar reads several
    // of them, so a linear scan comparing in place beats hashing a serialised key.
    std::vector<Series> cache_;
    QVector<OhlcvCandle> window_candles_;
    int window_bar_ = -1;
    int streamed_ = 0;
    int windowed_ = 0;
};

} // namespace fincept::algo
