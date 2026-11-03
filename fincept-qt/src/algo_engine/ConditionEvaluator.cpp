// src/algo_engine/ConditionEvaluator.cpp
#include "algo_engine/ConditionEvaluator.h"

#include <QJsonObject>

#include <cmath>
#include <cstddef>
#include <limits>
#include <utility>

namespace fincept::algo {

using services::algo::ConditionDef;

namespace {
constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();

// A non-finite operand must fail every condition. std::isnan alone is not enough:
// std::isnan(inf) is false, so an ±inf indicator value (e.g. a % change against a
// zero reference price) satisfied `inf > threshold` and fired live entries. Every
// guard below tests this instead of std::isnan.
inline bool cond_non_finite(double v) {
    return !std::isfinite(v);
}

bool op_needs_prev(const QString& op) {
    return op == "crosses_above" || op == "crosses_below" || op == "rising" || op == "falling" ||
           // "==" is a level *touch*: it needs the previous sample to detect the
           // price passing through the target between two (polled) ticks.
           op == "==";
}
} // namespace

ConditionDef ConditionEvaluator::parse_condition(const QJsonObject& obj) {
    ConditionDef c;
    c.indicator = obj.value("indicator").toString();
    c.params = obj.value("params").toObject();
    c.field = obj.value("field").toString("value");
    c.op = obj.value("operator").toString(">");
    c.value = obj.value("value").toDouble(0);
    c.value2 = obj.value("value2").toDouble(0);
    c.compare_mode = obj.value("compare_mode").toString("value");
    c.compare_indicator = obj.value("compare_indicator").toString();
    c.compare_params = obj.value("compare_params").toObject();
    c.compare_field = obj.value("compare_field").toString("value");
    c.offset = obj.value("offset").toInt(0);
    c.compare_offset = obj.value("compare_offset").toInt(0);
    return c;
}

bool ConditionEvaluator::is_group_node(const QJsonObject& node) {
    return node.contains("children") || node.value("type").toString() == "group";
}

bool ConditionEvaluator::apply_comparison(double lhs, const QString& op, double rhs) {
    if (cond_non_finite(lhs) || cond_non_finite(rhs))
        return false;
    if (op == ">")
        return lhs > rhs;
    if (op == "<")
        return lhs < rhs;
    if (op == ">=")
        return lhs >= rhs;
    if (op == "<=")
        return lhs <= rhs;
    if (op == "==")
        return std::abs(lhs - rhs) < 1e-8;
    return false;
}

bool ConditionEvaluator::apply_crossing(double curr, double prev, double target_curr, double target_prev,
                                        const QString& op) {
    if (cond_non_finite(curr) || cond_non_finite(prev) || cond_non_finite(target_curr) ||
        cond_non_finite(target_prev))
        return false;
    if (op == "crosses_above")
        return prev <= target_prev && curr > target_curr;
    if (op == "crosses_below")
        return prev >= target_prev && curr < target_curr;
    return false;
}

ConditionResult ConditionEvaluator::evaluate_single(const ConditionDef& condition,
                                                    const QVector<OhlcvCandle>& candles) {
    WindowOperandSource operands(candles);
    return evaluate_single(condition, operands);
}

ConditionResult ConditionEvaluator::evaluate_single(const ConditionDef& condition, OperandSource& operands) {

    ConditionResult result;
    result.indicator = condition.indicator;
    result.field = condition.field;
    result.op = condition.op;

    const bool needs_prev = op_needs_prev(condition.op);

    QString err;
    double lhs_curr = operands.value(condition.indicator, condition.params, condition.field, condition.offset, &err);
    if (!err.isEmpty()) {
        result.error = err;
        return result;
    }
    result.computed_value = lhs_curr;

    double lhs_prev =
        needs_prev ? operands.value(condition.indicator, condition.params, condition.field, condition.offset + 1, nullptr)
                   : kNaN;

    double rhs_curr, rhs_prev;
    if (condition.compare_mode == "indicator") {
        QString cerr;
        rhs_curr = operands.value(condition.compare_indicator, condition.compare_params, condition.compare_field,
                                  condition.compare_offset, &cerr);
        if (!cerr.isEmpty()) {
            result.error = cerr;
            return result;
        }
        rhs_prev = needs_prev ? operands.value(condition.compare_indicator, condition.compare_params,
                                               condition.compare_field, condition.compare_offset + 1, nullptr)
                              : kNaN;
    } else {
        rhs_curr = condition.value;
        rhs_prev = condition.value;
    }
    result.target_value = rhs_curr;

    if (condition.op == "crosses_above" || condition.op == "crosses_below") {
        result.met = apply_crossing(lhs_curr, lhs_prev, rhs_curr, rhs_prev, condition.op);
    } else if (condition.op == "rising") {
        result.met = !cond_non_finite(lhs_curr) && !cond_non_finite(lhs_prev) && lhs_curr > lhs_prev;
    } else if (condition.op == "falling") {
        result.met = !cond_non_finite(lhs_curr) && !cond_non_finite(lhs_prev) && lhs_curr < lhs_prev;
    } else if (condition.op == "between") {
        result.met = !cond_non_finite(lhs_curr) && lhs_curr >= condition.value && lhs_curr <= condition.value2;
        result.target_value = condition.value2;
    } else if (condition.op == "==") {
        // Level touch / equality cross. A live price polled every few seconds skips
        // the exact value (280.35 → 280.45 never equals 280.40), so plain float ==
        // never fires. Instead, trigger when the operands meet OR cross between the
        // previous and current sample — i.e. lhs−rhs is ~0 now, or changed sign
        // since the last sample (passed through equality, from either direction).
        double tol = std::abs(rhs_curr) * 1e-7;
        if (tol < 1e-9)
            tol = 1e-9;
        const double curr_diff = lhs_curr - rhs_curr;
        const double prev_diff = lhs_prev - rhs_prev; // NaN on the first sample
        if (cond_non_finite(lhs_curr) || cond_non_finite(rhs_curr))
            result.met = false;
        else if (std::abs(curr_diff) <= tol)
            result.met = true; // sitting on the level (within float tolerance)
        else if (!cond_non_finite(prev_diff) && ((prev_diff < 0) != (curr_diff < 0)))
            result.met = true; // crossed through the level since the last sample
        else
            result.met = false;
    } else {
        result.met = apply_comparison(lhs_curr, condition.op, rhs_curr);
    }

    return result;
}

GroupEvalResult ConditionEvaluator::evaluate_group(const QJsonArray& children, const QString& logic,
                                                   const QVector<OhlcvCandle>& candles) {
    WindowOperandSource operands(candles);
    return evaluate_group(children, logic, operands);
}

GroupEvalResult ConditionEvaluator::evaluate_group(const QJsonArray& children, const QString& logic,
                                                   OperandSource& operands) {
    return CompiledConditions(children, logic).evaluate(operands);
}

// ── CompiledConditions ──────────────────────────────────────────────────────

CompiledConditions::CompiledConditions(const QJsonArray& children, const QString& logic)
    : root_(compile(children, logic)) {}

CompiledConditions::Group CompiledConditions::compile(const QJsonArray& children, const QString& logic) {
    Group group;
    group.logic = logic;
    group.is_and = (logic.toUpper() != "OR"); // default AND
    group.nodes.reserve(static_cast<std::size_t>(children.size()));
    for (const auto& val : children) {
        const QJsonObject obj = val.toObject();
        Node node;
        if (ConditionEvaluator::is_group_node(obj)) {
            node.is_group = true;
            node.negate = obj.value("negate").toBool(false);
            node.group = compile(obj.value("children").toArray(),
                                 obj.value("logic").toString(obj.value("op").toString("AND")));
        } else {
            node.condition = ConditionEvaluator::parse_condition(obj);
        }
        group.nodes.push_back(std::move(node));
    }
    return group;
}

GroupEvalResult CompiledConditions::evaluate(OperandSource& operands) const {
    return run_group(root_, operands);
}

GroupEvalResult CompiledConditions::run_group(const Group& group, OperandSource& operands) {
    GroupEvalResult result;
    result.logic = group.logic;

    if (group.nodes.empty()) {
        result.triggered = false;
        return result;
    }

    bool overall = group.is_and;
    for (const Node& node : group.nodes) {
        bool met;
        if (node.is_group) {
            auto sub = run_group(node.group, operands);
            met = node.negate ? !sub.triggered : sub.triggered;
            result.details.append(sub.details); // flatten nested detail for reporting
        } else {
            auto r = ConditionEvaluator::evaluate_single(node.condition, operands);
            result.details.append(r);
            met = r.met;
        }

        if (group.is_and) {
            overall = overall && met;
            if (!overall)
                break; // short-circuit
        } else {
            overall = overall || met;
            if (overall)
                break; // short-circuit
        }
    }

    result.triggered = overall;
    return result;
}

} // namespace fincept::algo
