// src/algo_engine/LiveOperandSource.h
#pragma once
#include "algo_engine/OperandSource.h"
#include "algo_engine/StreamingOperand.h"

#include <QJsonObject>
#include <QString>
#include <QVector>

#include <cstdint>
#include <memory>
#include <vector>

namespace fincept::algo {

/// Operand source for evaluating one symbol live (RealtimeScanRunner).
///
/// The live evaluation window is the symbol's closed candles followed by
/// synthetic bars for the previous and the current tick. Each streamable operand
/// (StreamingOperand) is kept as incremental state over EVERY closed candle the
/// symbol has seen — the whole warm-up history, then each live close — and an
/// evaluation applies the tick bars to a copy of that state. Values are therefore
/// full-history values, not ones re-seeded at the start of a short candle buffer
/// (which left a live EMA(200) several percent of price off), and an evaluation
/// costs O(1) per operand instead of a recompute over the buffer.
///
/// Operands are registered up front (CompiledConditions::operands()). Anything
/// unregistered, unstreamable, or read deeper than registered is served by
/// WindowOperandSource over the window given to set_window(), as before.
/// Single-threaded: owned and used on one thread.
class LiveOperandSource final : public OperandSource {
  public:
    explicit LiveOperandSource(const std::vector<OperandRef>& operands);

    /// Replaces all state with `history`, oldest first (the warm-up fetch).
    void reset(const QVector<OhlcvCandle>& history);
    /// Folds in one newly closed candle.
    void on_closed(const OhlcvCandle& candle);
    /// The bars after the last closed candle (previous tick, current tick) that
    /// the next evaluations see.
    void set_ticks(const QVector<OhlcvCandle>& ticks);
    /// Candles for operands without a streaming path (closed buffer + ticks);
    /// nullptr when none is available. Must outlive the evaluations using it.
    void set_window(const QVector<OhlcvCandle>* window) { window_ = window; }
    /// True when some registered operand is served from the window.
    bool needs_window() const { return needs_window_; }
    /// Length of the live window: every closed candle plus the tick bars.
    std::int64_t bar_count() const { return closed_ + ticks_.size(); }

    double value(const QString& indicator, const QJsonObject& params, const QString& field, int offset,
                 QString* error) override;

  private:
    struct Tracked {
        QString indicator;
        QJsonObject params;
        QString field;
        int depth = 1;                           ///< closed-bar values kept for offsets
        std::unique_ptr<StreamingOperand> state; ///< over every closed candle
        std::vector<double> recent;              ///< ring of the last `depth` closed-bar values
        int recent_head = 0;                     ///< slot of the latest value
        int recent_count = 0;
        std::vector<double> with_ticks; ///< with_ticks[k]: value after ticks_[0..k]
        bool with_ticks_valid = false;
    };

    Tracked* find(const QString& indicator, const QJsonObject& params, const QString& field);
    static void push_recent(Tracked& t, double v);
    double from_window(const QString& indicator, const QJsonObject& params, const QString& field, int offset,
                       QString* error) const;

    std::vector<Tracked> tracked_;
    QVector<OhlcvCandle> ticks_;
    const QVector<OhlcvCandle>* window_ = nullptr;
    std::int64_t closed_ = 0;
    bool needs_window_ = false;
};

} // namespace fincept::algo
