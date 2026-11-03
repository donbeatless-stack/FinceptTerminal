// src/algo_engine/ConditionEvaluator.h
#pragma once
#include "algo_engine/AlgoEngineTypes.h"
#include "algo_engine/IndicatorEngine.h"
#include "algo_engine/OperandSource.h"
#include "services/algo_trading/AlgoTradingTypes.h"

#include <QJsonArray>
#include <QJsonObject>

#include <vector>

namespace fincept::algo {

/// Evaluates a strategy's entry/exit logic against a window of candles whose
/// LAST element is the current bar.
///
/// The condition format is a backward-compatible superset of the original flat
/// schema: `evaluate_group` receives an array of nodes joined by `logic`
/// (AND/OR). Each node is either
///   • a comparison leaf  — `{indicator, params, field, offset, operator,
///                            compare_mode, value | compare_indicator …}`, or
///   • a nested group     — `{"children": [...], "logic": "AND"|"OR",
///                            "negate": bool}`
/// so `(A AND B) OR C` is expressible. Legacy strategies (all-leaf arrays) keep
/// evaluating unchanged — no migration required.
///
/// Every operand carries an integer `offset` (bars ago, 0 = current), resolved by
/// an OperandSource. The candle overloads use WindowOperandSource, which
/// recomputes the indicator on the window truncated by N bars; BacktestEngine
/// passes a SeriesOperandSource instead. `crosses_*` / `rising` / `falling` read
/// the operand one extra bar back.
class ConditionEvaluator {
  public:
    static ConditionResult evaluate_single(const fincept::services::algo::ConditionDef& condition,
                                           const QVector<OhlcvCandle>& candles);

    static GroupEvalResult evaluate_group(const QJsonArray& children, const QString& logic,
                                          const QVector<OhlcvCandle>& candles);

    static ConditionResult evaluate_single(const fincept::services::algo::ConditionDef& condition,
                                           OperandSource& operands);

    /// Parses `children` and evaluates them once: CompiledConditions(children,
    /// logic).evaluate(operands). Callers evaluating the same tree on many bars
    /// should hold a CompiledConditions instead.
    static GroupEvalResult evaluate_group(const QJsonArray& children, const QString& logic, OperandSource& operands);

  private:
    friend class CompiledConditions;

    static fincept::services::algo::ConditionDef parse_condition(const QJsonObject& obj);
    static bool is_group_node(const QJsonObject& node);
    static bool apply_comparison(double lhs, const QString& op, double rhs);
    static bool apply_crossing(double curr, double prev, double target_curr, double target_prev, const QString& op);
};

/// A condition tree (the JSON ConditionEvaluator::evaluate_group takes) parsed
/// once. evaluate() does no JSON work — no key lookups, no re-parsing of every
/// leaf — which made up nearly all of BacktestEngine's per-bar evaluation cost
/// once its operands were streamed. evaluate_group() is implemented as
/// compile-then-evaluate, so the two paths share one set of semantics.
class CompiledConditions {
  public:
    CompiledConditions(const QJsonArray& children, const QString& logic);

    bool empty() const { return root_.nodes.empty(); }
    GroupEvalResult evaluate(OperandSource& operands) const;

  private:
    struct Node;
    struct Group {
        QString logic; ///< as given — reported back in GroupEvalResult::logic
        bool is_and = true;
        std::vector<Node> nodes;
    };
    struct Node {
        bool is_group = false;
        bool negate = false;                             ///< groups only
        fincept::services::algo::ConditionDef condition; ///< leaves only
        Group group;                                     ///< groups only
    };

    static Group compile(const QJsonArray& children, const QString& logic);
    static GroupEvalResult run_group(const Group& group, OperandSource& operands);

    Group root_;
};

} // namespace fincept::algo
