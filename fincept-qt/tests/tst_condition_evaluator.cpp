// Characterisation tests for src/algo_engine/ConditionEvaluator.{h,cpp}.
//
// Written against the evaluator as it stood before parsing was split from
// evaluation (CompiledConditions), to pin what that split had to preserve:
// operator semantics, which operands are read at which offsets, how errors
// propagate, AND/OR short-circuiting, and how nested groups report details.
// Operands come from a scripted OperandSource, so no indicator maths is involved
// and every read is recorded.
//
// Links Qt6::Gui only because ConditionEvaluator.h includes
// services/algo_trading/AlgoTradingTypes.h, which includes <QColor>; nothing in
// Qt Gui is called.

#include "algo_engine/AlgoEngineTypes.h"
#include "algo_engine/ConditionEvaluator.h"
#include "algo_engine/OperandSource.h"

#include <QHash>
#include <QJsonArray>
#include <QJsonObject>
#include <QString>
#include <QStringList>
#include <QTest>

#include <cmath>
#include <limits>

using fincept::algo::ConditionEvaluator;
using fincept::algo::GroupEvalResult;
using fincept::algo::OperandSource;

namespace {

constexpr double kNaN = std::numeric_limits<double>::quiet_NaN();
constexpr double kInf = std::numeric_limits<double>::infinity();

// Serves operands from a table keyed "INDICATOR.field@offset" and records every
// read in order. A key with no entry reads as NaN with no error.
class ScriptedOperands final : public OperandSource {
  public:
    QHash<QString, double> values;
    QHash<QString, QString> errors;
    QStringList reads;
    QList<QJsonObject> params_seen;

    void set(const QString& key, double v) { values.insert(key, v); }

    double value(const QString& indicator, const QJsonObject& params, const QString& field, int offset,
                 QString* error) override {
        const QString key = QStringLiteral("%1.%2@%3").arg(indicator, field).arg(offset);
        reads << key;
        params_seen << params;
        if (errors.contains(key)) {
            if (error)
                *error = errors.value(key);
            return kNaN;
        }
        return values.value(key, kNaN);
    }
};

QJsonObject leaf(const QString& indicator, const QString& op, double value) {
    return {{"indicator", indicator}, {"operator", op}, {"value", value}};
}

QJsonObject leaf_vs(const QString& indicator, const QString& op, const QString& compare) {
    return {{"indicator", indicator}, {"operator", op}, {"compare_mode", "indicator"}, {"compare_indicator", compare}};
}

GroupEvalResult eval(const QJsonArray& nodes, const QString& logic, OperandSource& operands) {
    return ConditionEvaluator::evaluate_group(nodes, logic, operands);
}

bool met(const QJsonObject& node, ScriptedOperands& operands) {
    return eval(QJsonArray{node}, QStringLiteral("AND"), operands).triggered;
}

} // namespace

class TstConditionEvaluator : public QObject {
    Q_OBJECT

  private slots:
    void comparison_operators();
    void defaults_for_missing_keys();
    void non_finite_operands_never_match();
    void between_is_inclusive_and_reports_the_upper_bound();
    void crossings_read_the_previous_bar();
    void crossing_against_another_indicator();
    void rising_and_falling();
    void equality_is_a_level_touch();
    void offsets_shift_every_read();
    void operand_errors_fail_the_condition();
    void and_or_short_circuit_and_logic_parsing();
    void nested_groups_negate_and_flatten_details();
    void empty_groups_never_trigger();
    void result_records_carry_the_operands();
    void compiled_operands_list_every_read_and_its_depth();
};

void TstConditionEvaluator::comparison_operators() {
    ScriptedOperands ops;
    ops.set("X.value@0", 50);
    QVERIFY(met(leaf("X", ">", 40), ops));
    QVERIFY(!met(leaf("X", ">", 50), ops));
    QVERIFY(met(leaf("X", ">=", 50), ops));
    QVERIFY(met(leaf("X", "<", 60), ops));
    QVERIFY(!met(leaf("X", "<", 50), ops));
    QVERIFY(met(leaf("X", "<=", 50), ops));
    QVERIFY(!met(leaf("X", "!=", 0), ops)); // unknown operator: never true
    // Plain comparisons read the current bar only.
    for (const QString& r : ops.reads)
        QCOMPARE(r, QStringLiteral("X.value@0"));
}

void TstConditionEvaluator::defaults_for_missing_keys() {
    ScriptedOperands ops;
    ops.set("X.value@0", 50);
    // No operator -> ">", no field -> "value", no value -> 0.
    QVERIFY(met(QJsonObject{{"indicator", "X"}}, ops));
    ops.set("X.value@0", -1);
    QVERIFY(!met(QJsonObject{{"indicator", "X"}}, ops));
    QCOMPARE(ops.reads.last(), QStringLiteral("X.value@0"));
    // compare_field defaults to "value" too.
    ops.set("Y.value@0", -2);
    QVERIFY(met(leaf_vs("X", ">", "Y"), ops));
}

void TstConditionEvaluator::non_finite_operands_never_match() {
    ScriptedOperands ops;
    ops.set("X.value@0", kInf);
    ops.set("X.value@1", 0);
    QVERIFY(!met(leaf("X", ">", 0), ops)); // inf would compare true without the guard
    QVERIFY(!met(leaf("X", "rising", 0), ops));
    QVERIFY(!met(QJsonObject{{"indicator", "X"}, {"operator", "between"}, {"value", 0}, {"value2", kInf}}, ops));
    ops.set("X.value@0", kNaN);
    QVERIFY(!met(leaf("X", "<", 0), ops));
    QVERIFY(!met(leaf("X", ">=", 0), ops));
}

void TstConditionEvaluator::between_is_inclusive_and_reports_the_upper_bound() {
    ScriptedOperands ops;
    ops.set("X.value@0", 50);
    auto between = [](double lo, double hi) {
        return QJsonObject{{"indicator", "X"}, {"operator", "between"}, {"value", lo}, {"value2", hi}};
    };
    QVERIFY(met(between(50, 60), ops));
    QVERIFY(met(between(40, 50), ops));
    QVERIFY(!met(between(51, 60), ops));
    const auto g = eval(QJsonArray{between(40, 60)}, "AND", ops);
    QCOMPARE(g.details.first().target_value, 60.0);
    QCOMPARE(ops.reads.last(), QStringLiteral("X.value@0")); // no previous-bar read
}

void TstConditionEvaluator::crossings_read_the_previous_bar() {
    ScriptedOperands ops;
    ops.set("X.value@1", 49);
    ops.set("X.value@0", 51);
    QVERIFY(met(leaf("X", "crosses_above", 50), ops));
    QVERIFY(!met(leaf("X", "crosses_below", 50), ops));
    QCOMPARE(ops.reads.mid(0, 2), (QStringList{"X.value@0", "X.value@1"}));

    ops.set("X.value@1", 50); // starting ON the level counts (prev <= target)
    QVERIFY(met(leaf("X", "crosses_above", 50), ops));
    ops.set("X.value@0", 50); // ending on it does not (curr > target)
    QVERIFY(!met(leaf("X", "crosses_above", 50), ops));

    ops.set("X.value@1", 51);
    ops.set("X.value@0", 49);
    QVERIFY(met(leaf("X", "crosses_below", 50), ops));

    ops.values.remove("X.value@1"); // no previous sample: never a cross
    QVERIFY(!met(leaf("X", "crosses_below", 50), ops));
}

void TstConditionEvaluator::crossing_against_another_indicator() {
    ScriptedOperands ops;
    ops.set("FAST.value@1", 9.5);
    ops.set("FAST.value@0", 10.5);
    ops.set("SLOW.value@1", 10.0);
    ops.set("SLOW.value@0", 10.0);
    QVERIFY(met(leaf_vs("FAST", "crosses_above", "SLOW"), ops));
    // Read order: lhs now, lhs previous, rhs now, rhs previous.
    QCOMPARE(ops.reads, (QStringList{"FAST.value@0", "FAST.value@1", "SLOW.value@0", "SLOW.value@1"}));
    QVERIFY(!met(leaf_vs("FAST", "crosses_below", "SLOW"), ops));
    // The target's own previous value is what the previous lhs is compared with.
    ops.set("SLOW.value@1", 9.0);
    QVERIFY(!met(leaf_vs("FAST", "crosses_above", "SLOW"), ops));
}

void TstConditionEvaluator::rising_and_falling() {
    ScriptedOperands ops;
    ops.set("X.value@1", 4);
    ops.set("X.value@0", 5);
    QVERIFY(met(leaf("X", "rising", 0), ops));
    QVERIFY(!met(leaf("X", "falling", 0), ops));
    ops.set("X.value@0", 4);
    QVERIFY(!met(leaf("X", "rising", 0), ops));
    QVERIFY(!met(leaf("X", "falling", 0), ops));
    ops.set("X.value@0", 3);
    QVERIFY(met(leaf("X", "falling", 0), ops));
    ops.values.remove("X.value@1");
    QVERIFY(!met(leaf("X", "falling", 0), ops));
}

void TstConditionEvaluator::equality_is_a_level_touch() {
    ScriptedOperands ops;
    const double level = 280.40;
    ops.set("X.value@0", 280.40);
    QVERIFY(met(leaf("X", "==", level), ops)); // sitting on it

    ops.set("X.value@1", 280.35);
    ops.set("X.value@0", 280.45);
    QVERIFY(met(leaf("X", "==", level), ops)); // passed through it between samples
    ops.set("X.value@1", 280.45);
    ops.set("X.value@0", 280.35);
    QVERIFY(met(leaf("X", "==", level), ops)); // from either side

    ops.set("X.value@1", 280.45);
    ops.set("X.value@0", 280.50);
    QVERIFY(!met(leaf("X", "==", level), ops)); // stayed above

    // Tolerance is |level| * 1e-7 (~2.8e-5 here), not an absolute epsilon.
    ops.set("X.value@1", 280.41);
    ops.set("X.value@0", level + 2e-5);
    QVERIFY(met(leaf("X", "==", level), ops));
    ops.set("X.value@0", level + 5e-5);
    QVERIFY(!met(leaf("X", "==", level), ops));

    ops.values.remove("X.value@1"); // first sample: only an exact touch counts
    ops.set("X.value@0", 280.45);
    QVERIFY(!met(leaf("X", "==", level), ops));
}

void TstConditionEvaluator::offsets_shift_every_read() {
    ScriptedOperands ops;
    QJsonObject c = leaf_vs("FAST", "crosses_above", "SLOW");
    c["offset"] = 2;
    c["compare_offset"] = 3;
    met(c, ops);
    QCOMPARE(ops.reads, (QStringList{"FAST.value@2", "FAST.value@3", "SLOW.value@3", "SLOW.value@4"}));
}

void TstConditionEvaluator::operand_errors_fail_the_condition() {
    ScriptedOperands ops;
    ops.errors.insert("X.value@0", "Insufficient data for X");
    auto g = eval(QJsonArray{leaf("X", "<", 1e9)}, "AND", ops);
    QVERIFY(!g.triggered);
    QCOMPARE(g.details.size(), qsizetype(1));
    QCOMPARE(g.details.first().error, QStringLiteral("Insufficient data for X"));
    QVERIFY(!g.details.first().met);

    // An error on the compared indicator is reported; the lhs value is kept.
    ScriptedOperands ops2;
    ops2.set("X.value@0", 5);
    ops2.errors.insert("Y.value@0", "Y failed");
    g = eval(QJsonArray{leaf_vs("X", ">", "Y")}, "AND", ops2);
    QVERIFY(!g.triggered);
    QCOMPARE(g.details.first().error, QStringLiteral("Y failed"));
    QCOMPARE(g.details.first().computed_value, 5.0);

    // A previous-bar error is silent: the crossing just cannot be confirmed.
    ScriptedOperands ops3;
    ops3.set("X.value@0", 51);
    ops3.errors.insert("X.value@1", "no history");
    g = eval(QJsonArray{leaf("X", "crosses_above", 50)}, "AND", ops3);
    QVERIFY(!g.triggered);
    QVERIFY(g.details.first().error.isEmpty());
}

void TstConditionEvaluator::and_or_short_circuit_and_logic_parsing() {
    ScriptedOperands ops;
    ops.set("A.value@0", 1);
    ops.set("B.value@0", -1);
    ops.set("C.value@0", 1);
    const QJsonArray abc{leaf("A", ">", 0), leaf("B", ">", 0), leaf("C", ">", 0)};

    auto g = eval(abc, "AND", ops);
    QVERIFY(!g.triggered);
    QCOMPARE(g.details.size(), qsizetype(2)); // stopped at B
    QCOMPARE(ops.reads, (QStringList{"A.value@0", "B.value@0"}));
    QCOMPARE(g.logic, QStringLiteral("AND"));

    ops.reads.clear();
    g = eval(QJsonArray{leaf("B", ">", 0), leaf("A", ">", 0), leaf("C", ">", 0)}, "OR", ops);
    QVERIFY(g.triggered);
    QCOMPARE(g.details.size(), qsizetype(2)); // stopped at A
    QCOMPARE(ops.reads, (QStringList{"B.value@0", "A.value@0"}));

    QVERIFY(eval(QJsonArray{leaf("B", ">", 0), leaf("A", ">", 0)}, "or", ops).triggered); // case-insensitive
    QVERIFY(!eval(QJsonArray{leaf("B", ">", 0), leaf("A", ">", 0)}, "", ops).triggered); // default AND
    QVERIFY(!eval(QJsonArray{leaf("B", ">", 0), leaf("A", ">", 0)}, "XOR", ops).triggered);
    QCOMPARE(eval(abc, "whatever", ops).logic, QStringLiteral("whatever")); // reported as given
}

void TstConditionEvaluator::nested_groups_negate_and_flatten_details() {
    ScriptedOperands ops;
    ops.set("A.value@0", 1);
    ops.set("B.value@0", -1);
    ops.set("C.value@0", 1);
    const QJsonObject a = leaf("A", ">", 0), b = leaf("B", ">", 0), c = leaf("C", ">", 0);

    // (A AND B) OR C
    const QJsonObject a_and_b{{"children", QJsonArray{a, b}}, {"logic", "AND"}};
    auto g = eval(QJsonArray{a_and_b, c}, "OR", ops);
    QVERIFY(g.triggered);
    QCOMPARE(g.details.size(), qsizetype(3)); // A, B from the group, then C
    QCOMPARE(g.details[0].indicator, QStringLiteral("A"));
    QCOMPARE(g.details[1].indicator, QStringLiteral("B"));
    QCOMPARE(g.details[2].indicator, QStringLiteral("C"));

    // NOT (B)
    QVERIFY(eval(QJsonArray{QJsonObject{{"children", QJsonArray{b}}, {"negate", true}}}, "AND", ops).triggered);
    // Legacy "op" key names the group's logic when "logic" is absent...
    QVERIFY(eval(QJsonArray{QJsonObject{{"children", QJsonArray{b, a}}, {"op", "OR"}}}, "AND", ops).triggered);
    // ...and "logic" wins when both are present; a group defaults to AND.
    QVERIFY(!eval(QJsonArray{QJsonObject{{"children", QJsonArray{b, a}}, {"op", "OR"}, {"logic", "AND"}}}, "AND", ops)
                 .triggered);
    QVERIFY(!eval(QJsonArray{QJsonObject{{"children", QJsonArray{b, a}}}}, "AND", ops).triggered);
    // {"type": "group"} marks a group even without children (which then never triggers).
    QVERIFY(!eval(QJsonArray{QJsonObject{{"type", "group"}, {"indicator", "A"}}}, "AND", ops).triggered);
    QVERIFY(eval(QJsonArray{QJsonObject{{"type", "group"}, {"negate", true}}}, "AND", ops).triggered);
}

void TstConditionEvaluator::empty_groups_never_trigger() {
    ScriptedOperands ops;
    auto g = eval(QJsonArray{}, "OR", ops);
    QVERIFY(!g.triggered);
    QVERIFY(g.details.isEmpty());
    QCOMPARE(g.logic, QStringLiteral("OR"));
    QVERIFY(ops.reads.isEmpty());
    QVERIFY(!eval(QJsonArray{QJsonObject{{"children", QJsonArray{}}}}, "AND", ops).triggered);
}

void TstConditionEvaluator::result_records_carry_the_operands() {
    ScriptedOperands ops;
    ops.set("RSI.value@0", 27.5);
    ops.set("EMA.signal@0", 101.25);
    QJsonObject c = leaf("RSI", "<", 30);
    c["params"] = QJsonObject{{"period", 14}};
    auto g = eval(QJsonArray{c}, "AND", ops);
    const auto& r = g.details.first();
    QCOMPARE(r.indicator, QStringLiteral("RSI"));
    QCOMPARE(r.field, QStringLiteral("value"));
    QCOMPARE(r.op, QStringLiteral("<"));
    QCOMPARE(r.computed_value, 27.5);
    QCOMPARE(r.target_value, 30.0);
    QVERIFY(r.met);
    QVERIFY(ops.params_seen.first() == (QJsonObject{{"period", 14}}));

    QJsonObject vs = leaf_vs("RSI", ">", "EMA");
    vs["compare_field"] = "signal";
    vs["compare_params"] = QJsonObject{{"period", 9}};
    ops.params_seen.clear();
    g = eval(QJsonArray{vs}, "AND", ops);
    QCOMPARE(g.details.first().target_value, 101.25);
    QVERIFY(!g.details.first().met);
    QVERIFY(ops.params_seen.last() == (QJsonObject{{"period", 9}}));
}

void TstConditionEvaluator::compiled_operands_list_every_read_and_its_depth() {
    // (FAST crosses_above SLOW) OR (RSI < 30 AND CLOSE[offset 2] > SLOW) — nested,
    // compared, a crossing (reads one bar further back), a duplicate, an offset.
    QJsonObject cross = leaf_vs("FAST", "crosses_above", "SLOW");
    cross["params"] = QJsonObject{{"period", 12}};
    cross["compare_params"] = QJsonObject{{"period", 26}};
    QJsonObject close_vs_slow = leaf_vs("CLOSE", ">", "SLOW");
    close_vs_slow["offset"] = 2;
    close_vs_slow["compare_params"] = QJsonObject{{"period", 26}};
    QJsonObject negative = leaf("NEG", ">", 0);
    negative["offset"] = -4;
    const QJsonArray tree{cross,
                          QJsonObject{{"children", QJsonArray{leaf("RSI", "<", 30), close_vs_slow}}, {"logic", "AND"}},
                          negative};

    const fincept::algo::CompiledConditions compiled(tree, "OR");
    const auto refs = compiled.operands();
    QHash<QString, int> depth;
    for (const auto& r : refs)
        depth.insert(QStringLiteral("%1.%2/%3").arg(r.indicator, r.field).arg(r.params.value("period").toInt()),
                     r.max_offset);
    QCOMPARE(refs.size(), std::size_t(5)); // SLOW(26) listed once
    QCOMPARE(depth.value("FAST.value/12"), 1); // crossing: one bar back
    QCOMPARE(depth.value("SLOW.value/26"), 1); // max of the crossing (1) and the plain read (0)
    QCOMPARE(depth.value("RSI.value/0"), 0);
    QCOMPARE(depth.value("CLOSE.value/0"), 2);
    QCOMPARE(depth.value("NEG.value/0"), 0); // negative offsets read as 0

    // Every read evaluate() makes is covered by the list, at no deeper offset.
    ScriptedOperands ops;
    for (const auto& r : refs)
        for (int k = 0; k <= r.max_offset; ++k)
            ops.set(QStringLiteral("%1.%2@%3").arg(r.indicator, r.field).arg(k), 1);
    compiled.evaluate(ops);
    for (const QString& read : ops.reads)
        QVERIFY2(ops.values.contains(read) || read == QStringLiteral("NEG.value@-4"), qPrintable(read));
}

QTEST_GUILESS_MAIN(TstConditionEvaluator)
#include "tst_condition_evaluator.moc"
