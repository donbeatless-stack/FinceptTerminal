// Unit tests for IndicatorEngine::warmup_bars and ConditionEvaluator::required_bars
//
// warmup_bars() is a table kept next to IndicatorEngine::compute(); nothing in the
// compiler ties the two together. These tests do. For every indicator the strategy
// builder offers (algo_indicators()), at its defaults, at both ends of every
// parameter range and at 40 random points inside them, compute() must return a
// finite value for every field on exactly warmup_bars() candles, and must not on
// one candle fewer: the count is the real minimum, not a safe guess.
//
// required_bars() is checked one level up, through the operand source the live
// paths use: on exactly required_bars() candles both sides of a condition have a
// finite value at their offset, and one bar further back when the operator reads
// the previous sample. Hand-counted cases pin the formula, including the ones the
// old period-only count got wrong.
//
// Links ConditionEvaluator.cpp, OperandSource.cpp and IndicatorEngine.cpp; Qt Gui
// only because AlgoTradingTypes.h (the catalog) includes QColor.

#include "algo_engine/AlgoEngineTypes.h"
#include "algo_engine/ConditionEvaluator.h"
#include "algo_engine/IndicatorEngine.h"
#include "algo_engine/OperandSource.h"
#include "services/algo_trading/AlgoTradingTypes.h"

#include <QJsonArray>
#include <QJsonObject>
#include <QString>
#include <QStringList>
#include <QTest>
#include <QVector>

#include <cmath>
#include <functional>
#include <limits>
#include <random>

using fincept::algo::ConditionEvaluator;
using fincept::algo::IndicatorEngine;
using fincept::algo::IndicatorResult;
using fincept::algo::OhlcvCandle;
using fincept::algo::WindowOperandSource;
using fincept::services::algo::algo_indicators;
using fincept::services::algo::algo_operators;
using fincept::services::algo::IndicatorDef;
using fincept::services::algo::ParamSpec;

namespace {

// A seeded log-normal walk with real wicks and positive volume, so no indicator
// sits on a zero-range or zero-volume branch.
QVector<OhlcvCandle> walk(int n) {
    std::mt19937_64 rng(20261104);
    std::normal_distribution<double> ret(0.0, 0.01);
    std::uniform_real_distribution<double> wick(0.0005, 0.004);
    std::uniform_real_distribution<double> vol(1'000.0, 5'000.0);
    QVector<OhlcvCandle> out;
    out.reserve(n);
    double close = 100.0;
    for (int i = 0; i < n; ++i) {
        OhlcvCandle c;
        c.open_time = c.close_time = i;
        c.open = close;
        close *= std::exp(ret(rng));
        c.close = close;
        c.high = std::max(c.open, c.close) * (1.0 + wick(rng));
        c.low = std::min(c.open, c.close) * (1.0 - wick(rng));
        c.volume = vol(rng);
        c.is_closed = true;
        out.append(c);
    }
    return out;
}

// Longest series any case below needs: TEMA(500) = 3 * 500 - 2, plus offsets.
const QVector<OhlcvCandle>& bars() {
    static const QVector<OhlcvCandle> b = walk(1600);
    return b;
}

QVector<OhlcvCandle> first(int n) {
    return bars().mid(0, n);
}

struct Case {
    QString id;
    QJsonObject params;
    QStringList fields;
    QString label;
};

QJsonObject make_params(const IndicatorDef& d, const std::function<double(const ParamSpec&)>& pick) {
    QJsonObject p;
    for (const ParamSpec& s : d.params) {
        const double v = s.decimals == 0 ? pick(s) : s.def; // non-integer params set no lookback
        p.insert(s.name, s.decimals == 0 ? QJsonValue(static_cast<int>(std::lround(v))) : QJsonValue(v));
    }
    return p;
}

QString describe(const QString& id, const QJsonObject& p) {
    QStringList kv;
    for (auto it = p.begin(); it != p.end(); ++it)
        kv << QStringLiteral("%1=%2").arg(it.key(), it.value().toVariant().toString());
    return QStringLiteral("%1(%2)").arg(id, kv.join(QLatin1Char(',')));
}

// Every catalog indicator at defaults, all-min, all-max and 40 random points, plus
// the Ichimoku layout that used to read before the start of the series.
QVector<Case> catalog_cases() {
    std::mt19937 rng(7);
    QVector<Case> out;
    for (const IndicatorDef& d : algo_indicators()) {
        auto add = [&](const QJsonObject& p) { out.append({d.id, p, d.fields, describe(d.id, p)}); };
        add(make_params(d, [](const ParamSpec& s) { return s.def; }));
        add(make_params(d, [](const ParamSpec& s) { return s.min; }));
        add(make_params(d, [](const ParamSpec& s) { return s.max; }));
        if (d.params.isEmpty())
            continue;
        for (int i = 0; i < 40; ++i)
            add(make_params(d, [&](const ParamSpec& s) {
                return std::uniform_int_distribution<int>(static_cast<int>(s.min), static_cast<int>(s.max))(rng);
            }));
    }
    const QJsonObject ichimoku{{"tenkan", 9}, {"kijun", 100}, {"senkou", 52}};
    out.append({"ICHIMOKU", ichimoku, {"tenkan_sen", "kijun_sen", "senkou_a", "senkou_b"},
                describe("ICHIMOKU", ichimoku)});
    return out;
}

// The current value of every field is finite.
bool all_finite(const IndicatorResult& r, const QStringList& fields) {
    if (!r.valid)
        return false;
    for (const QString& f : fields)
        if (!std::isfinite(r.current.value(f, std::numeric_limits<double>::quiet_NaN())))
            return false;
    return true;
}

QJsonObject leaf(const QString& ind, const QJsonObject& params, const QString& op, double value = 0) {
    return QJsonObject{{"indicator", ind}, {"params", params}, {"field", "value"}, {"operator", op}, {"value", value}};
}

QJsonObject period(int p) {
    return QJsonObject{{"period", p}};
}

int required(const QJsonObject& condition) {
    return ConditionEvaluator::required_bars(QJsonArray{condition});
}

// Both sides of `c` (as evaluate_single reads them) are finite on `candles`.
bool condition_has_values(const QJsonObject& c, const QVector<OhlcvCandle>& candles, QString* why) {
    WindowOperandSource src(candles);
    const QString op = c.value("operator").toString();
    const bool needs_prev = op == "crosses_above" || op == "crosses_below" || op == "rising" || op == "falling" ||
                            op == "==";
    auto side = [&](const QString& ind, const QJsonObject& params, const QString& field, int offset) {
        for (int back = 0; back <= (needs_prev ? 1 : 0); ++back) {
            QString err;
            const double v = src.value(ind, params, field, offset + back, &err);
            if (!std::isfinite(v)) {
                *why = QStringLiteral("%1.%2 at offset %3: %4").arg(describe(ind, params), field).arg(offset + back).arg(err);
                return false;
            }
        }
        return true;
    };
    if (!side(c.value("indicator").toString(), c.value("params").toObject(), c.value("field").toString("value"),
              c.value("offset").toInt(0)))
        return false;
    if (c.value("compare_mode").toString() == "indicator")
        return side(c.value("compare_indicator").toString(), c.value("compare_params").toObject(),
                    c.value("compare_field").toString("value"), c.value("compare_offset").toInt(0));
    return true;
}

} // namespace

class TstIndicatorWarmup : public QObject {
    Q_OBJECT

  private slots:
    void every_catalog_indicator_is_valid_on_exactly_warmup_bars();
    void one_bar_fewer_is_not_enough();
    void ichimoku_with_kijun_longer_than_senkou_waits_for_kijun();

    void required_bars_hand_counted();
    void required_bars_old_count_was_short();
    void random_conditions_have_values_on_exactly_required_bars();
};

void TstIndicatorWarmup::every_catalog_indicator_is_valid_on_exactly_warmup_bars() {
    const QVector<Case> cases = catalog_cases();
    QVERIFY(cases.size() > 1000);
    for (const Case& c : cases) {
        const int w = IndicatorEngine::warmup_bars(c.id, c.params);
        QVERIFY2(w >= 1, qPrintable(c.label + QStringLiteral(": no warm-up count")));
        QVERIFY2(w <= bars().size(), qPrintable(c.label));
        // compute() refuses fewer than two candles whatever the indicator.
        const IndicatorResult r = IndicatorEngine::compute(c.id, first(std::max(w, 2)), c.params, QString());
        QVERIFY2(all_finite(r, c.fields),
                 qPrintable(QStringLiteral("%1 on %2 bars: %3").arg(c.label).arg(std::max(w, 2)).arg(r.error)));
    }
}

void TstIndicatorWarmup::one_bar_fewer_is_not_enough() {
    int checked = 0;
    for (const Case& c : catalog_cases()) {
        const int w = IndicatorEngine::warmup_bars(c.id, c.params);
        if (w <= 2)
            continue; // compute()'s own two-candle floor decides these
        const IndicatorResult r = IndicatorEngine::compute(c.id, first(w - 1), c.params, QString());
        if (c.id == "STOCHASTIC" && r.valid) {
            // compute_stochastic reports %K as %D until %D has d_period values of %K;
            // warmup_bars counts the bars for the real %D.
            QCOMPARE(r.current.value("d"), r.current.value("k"));
            ++checked;
            continue;
        }
        QVERIFY2(!all_finite(r, c.fields),
                 qPrintable(QStringLiteral("%1 is already valid on %2 bars: warmup_bars says %3")
                                .arg(c.label)
                                .arg(w - 1)
                                .arg(w)));
        ++checked;
    }
    QVERIFY(checked > 800);
}

void TstIndicatorWarmup::ichimoku_with_kijun_longer_than_senkou_waits_for_kijun() {
    // Used to check only n >= senkou, so 52..99 bars read kijun's lookback before
    // the start of the series.
    const QJsonObject p{{"tenkan", 9}, {"kijun", 100}, {"senkou", 52}};
    QCOMPARE(IndicatorEngine::warmup_bars("ICHIMOKU", p), 100);
    QVERIFY(!IndicatorEngine::compute("ICHIMOKU", first(60), p, QString()).valid);
    const IndicatorResult r = IndicatorEngine::compute("ICHIMOKU", first(100), p, QString());
    QVERIFY(r.valid);
    QVERIFY(r.previous.isEmpty()); // the previous bar needs 101
    QVERIFY(!IndicatorEngine::compute("ICHIMOKU", first(101), p, QString()).previous.isEmpty());
}

void TstIndicatorWarmup::required_bars_hand_counted() {
    // Price level: compute()'s two-candle floor; a crossing reads one bar more.
    QCOMPARE(required(leaf("CLOSE", {}, ">", 100)), 2);
    QCOMPARE(required(leaf("CLOSE", {}, "crosses_above", 100)), 3);
    QCOMPARE(ConditionEvaluator::required_bars(QJsonArray{}), 2);

    // SMA(50) three bars back, crossing: 50 + 3 + 1.
    QJsonObject sma = leaf("SMA", period(50), "crosses_above", 100);
    sma["offset"] = 3;
    QCOMPARE(required(sma), 54);

    // MACD line crossing its signal: max(12, 26) + 9 - 1 = 34, + 1 for the crossing.
    const QJsonObject macd_p{{"fast", 12}, {"slow", 26}, {"signal", 9}};
    QJsonObject macd = leaf("MACD", macd_p, "crosses_above");
    macd["field"] = "line";
    macd["compare_mode"] = "indicator";
    macd["compare_indicator"] = "MACD";
    macd["compare_params"] = macd_p;
    macd["compare_field"] = "signal_line";
    QCOMPARE(required(macd), 35);

    // The compare side counts only in indicator mode.
    QJsonObject rsi = leaf("RSI", period(14), ">", 70);
    rsi["compare_indicator"] = "SMA";
    rsi["compare_params"] = period(200);
    QCOMPARE(required(rsi), 15);
    rsi["compare_mode"] = "indicator";
    QCOMPARE(required(rsi), 200);

    // Groups: the deepest, longest leaf wins.
    const QJsonObject inner{{"type", "group"},
                            {"logic", "OR"},
                            {"children", QJsonArray{leaf("EMA", period(100), "rising")}}};
    QCOMPARE(ConditionEvaluator::required_bars(QJsonArray{leaf("SMA", period(20), ">"), inner}), 101);
}

void TstIndicatorWarmup::required_bars_old_count_was_short() {
    // The old count read only params.period: max(period) + max(offset) + 2. The
    // realtime scan sized its window buffer from it (+ 50, at least 60), so these
    // conditions never got a value there.
    struct Old {
        QJsonObject condition;
        int old_count;
    };
    QJsonObject tema = leaf("TEMA", period(30), ">");
    tema["compare_mode"] = "indicator";
    tema["compare_indicator"] = "CLOSE";
    QJsonObject ichimoku = leaf("ICHIMOKU", QJsonObject{{"senkou", 78}}, ">");
    ichimoku["field"] = "tenkan_sen";
    const QVector<Old> cases = {
        {tema, 32},                                                 // needs 88
        {leaf("VOL_WIN_CHG", QJsonObject{{"window", 40}}, ">"), 2}, // needs 80
        {ichimoku, 2},                                              // needs 78
        {leaf("ADX", period(60), "crosses_above", 25), 62},         // needs 122
    };
    for (const Old& o : cases) {
        const int need = required(o.condition);
        const int old_buffer = std::max(o.old_count + 50, 60);
        QString why;
        QVERIFY2(need > old_buffer,
                 qPrintable(QStringLiteral("%1: %2").arg(o.condition.value("indicator").toString()).arg(need)));
        QVERIFY2(!condition_has_values(o.condition, first(old_buffer), &why),
                 qPrintable(o.condition.value("indicator").toString() + QStringLiteral(" fits the old buffer")));
        QVERIFY2(condition_has_values(o.condition, first(need), &why), qPrintable(why));
    }
}

void TstIndicatorWarmup::random_conditions_have_values_on_exactly_required_bars() {
    const QVector<IndicatorDef> defs = algo_indicators();
    const QStringList ops = algo_operators();
    std::mt19937 rng(11);
    auto pick = [&](int n) { return std::uniform_int_distribution<int>(0, n - 1)(rng); };
    auto random_params = [&](const IndicatorDef& d) {
        return make_params(d, [&](const ParamSpec& s) {
            // Keep most lookbacks small so a few hundred conditions stay fast; the
            // full ranges are covered indicator by indicator above.
            const int hi = static_cast<int>(std::min(s.max, s.min + 120.0));
            return std::uniform_int_distribution<int>(static_cast<int>(s.min), hi)(rng);
        });
    };
    for (int i = 0; i < 400; ++i) {
        const IndicatorDef& lhs = defs[pick(defs.size())];
        QJsonObject c = leaf(lhs.id, random_params(lhs), ops[pick(ops.size())], 1.0);
        c["field"] = lhs.fields[pick(lhs.fields.size())];
        c["value2"] = 2.0;
        c["offset"] = pick(4);
        if (pick(2) == 0) {
            const IndicatorDef& rhs = defs[pick(defs.size())];
            c["compare_mode"] = "indicator";
            c["compare_indicator"] = rhs.id;
            c["compare_params"] = random_params(rhs);
            c["compare_field"] = rhs.fields[pick(rhs.fields.size())];
            c["compare_offset"] = pick(4);
        }
        const int need = required(c);
        QString why;
        QVERIFY2(condition_has_values(c, first(need), &why),
                 qPrintable(QStringLiteral("condition %1 on %2 bars: %3").arg(i).arg(need).arg(why)));
    }
}

QTEST_GUILESS_MAIN(TstIndicatorWarmup)
#include "tst_indicator_warmup.moc"
