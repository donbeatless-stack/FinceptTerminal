// Unit tests for src/algo_engine/SeriesOperandSource.{h,cpp} and the
// WindowOperandSource (src/algo_engine/OperandSource.{h,cpp}) it falls back to.
//
// BacktestEngine used to recompute every condition operand on a trailing
// 500-candle window at every bar; it now reads streamed operands from series
// computed once. ConditionEvaluator's verdicts are a pure function of the
// operand values it is handed, so pinning the operands pins the backtest. These
// tests pin, at every bar and several offsets of a 650-bar walk:
//   1. A streamed operand equals WindowOperandSource over the FULL history —
//      value, error text, and the bar its warm-up ends.
//   2. Against the old trailing-window path, finite-memory indicators are
//      unchanged; recursive ones differ only by that window's re-seeding error,
//      which is negligible for short periods and measurable for EMA(200).
//   3. Operands without a streaming implementation take the old path verbatim.
//
// Links OperandSource, SeriesOperandSource, StreamingIndicators and
// IndicatorEngine only — Qt Core + Test.

#include "algo_engine/AlgoEngineTypes.h"
#include "algo_engine/OperandSource.h"
#include "algo_engine/SeriesOperandSource.h"

#include <QJsonDocument>
#include <QJsonObject>
#include <QString>
#include <QTest>
#include <QVector>

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>

using fincept::algo::OhlcvCandle;
using fincept::algo::SeriesOperandSource;
using fincept::algo::WindowOperandSource;

namespace {

constexpr int kWindow = 500; // BacktestEngine::kEvalWindow
constexpr int kBars = 650;   // past the window, so its truncation is exercised

QVector<OhlcvCandle> make_bars() {
    std::mt19937_64 rng(97);
    std::normal_distribution<double> ret(0.0, 0.012);
    std::uniform_real_distribution<double> wick(0.0, 0.004);
    std::uniform_real_distribution<double> vol(1'000.0, 5'000.0);
    QVector<OhlcvCandle> out;
    double close = 100.0;
    for (int i = 0; i < kBars; ++i) {
        OhlcvCandle c;
        c.open_time = c.close_time = i;
        c.open = close;
        if (i >= 300 && i < 330) { // flat, nothing traded
            c.close = c.high = c.low = close;
            c.volume = 0;
        } else {
            close *= std::exp(ret(rng));
            c.close = close;
            c.high = std::max(c.open, c.close) * (1.0 + wick(rng));
            c.low = std::min(c.open, c.close) * (1.0 - wick(rng));
            c.volume = vol(rng);
        }
        c.is_closed = true;
        out.append(c);
    }
    return out;
}

struct Operand {
    QString indicator;
    QJsonObject params;
    QString field;
    double tol; ///< relative tolerance vs the batch engine
};

// Every (indicator, field) the series source streams. 1e-12 is bit-exact in
// practice (see tst_streaming_indicators); Bollinger's Welford variance gets 1e-9.
std::vector<Operand> streamed_cases() {
    const QJsonObject bb{{"period", 20}, {"std_dev", 2.0}};
    return {
        {"CLOSE", {}, "value", 1e-12},
        {"OPEN", {}, "value", 1e-12},
        {"HIGH", {}, "value", 1e-12},
        {"LOW", {}, "value", 1e-12},
        {"VOLUME", {}, "value", 1e-12},
        {"SMA", {{"period", 20}}, "value", 1e-12},
        {"SMA", {}, "value", 1e-12}, // default period 14
        {"EMA", {{"period", 12}}, "value", 1e-12},
        {"RSI", {{"period", 14}}, "value", 1e-12},
        {"ATR", {{"period", 14}}, "value", 1e-12},
        {"MACD", {}, "line", 1e-12},
        {"MACD", {{"fast", 5}, {"slow", 35}, {"signal", 5}}, "signal_line", 1e-12},
        {"MACD", {}, "histogram", 1e-12},
        {"BOLLINGER", bb, "upper", 1e-9},
        {"BOLLINGER", bb, "middle", 1e-9},
        {"BOLLINGER", bb, "lower", 1e-9},
        {"BOLLINGER", bb, "width", 1e-9},
        {"BOLLINGER", bb, "pct_b", 1e-9},
        {"DONCHIAN", {{"period", 20}}, "upper", 1e-12},
        {"DONCHIAN", {{"period", 20}}, "lower", 1e-12},
        {"STOCHASTIC", {}, "k", 1e-12},
        {"STOCHASTIC", {{"k_period", 9}, {"d_period", 5}}, "d", 1e-12},
        {"WILLIAMS_R", {{"period", 14}}, "value", 1e-12},
    };
}

bool near(double got, double want, double rel) {
    if (std::isnan(got) || std::isnan(want))
        return std::isnan(got) && std::isnan(want);
    return std::abs(got - want) <= rel * std::max({1.0, std::abs(got), std::abs(want)});
}

QString describe(const Operand& o, int offset) {
    return QStringLiteral("%1(%2).%3 offset %4")
        .arg(o.indicator, QString::fromUtf8(QJsonDocument(o.params).toJson(QJsonDocument::Compact)), o.field)
        .arg(offset);
}

// The pre-change BacktestEngine operand: recompute on the trailing window.
double trailing_window_value(const QVector<OhlcvCandle>& bars, int bar, const Operand& o, int offset, QString* err) {
    const int start = std::max(0, bar - kWindow + 1);
    const QVector<OhlcvCandle> window = bars.mid(start, bar - start + 1);
    WindowOperandSource w(window);
    return w.value(o.indicator, o.params, o.field, offset, err);
}

} // namespace

class TstSeriesOperandSource : public QObject {
    Q_OBJECT

  private slots:
    void streamed_operands_match_the_full_history_batch_engine();
    void finite_memory_operands_are_unchanged_from_the_window_path();
    void recursive_operands_lose_only_the_window_reseeding_error();
    void operands_without_a_streaming_path_use_the_window_verbatim();
    void periods_longer_than_the_window_now_warm_up();
    void offsets_and_cache();
};

void TstSeriesOperandSource::streamed_operands_match_the_full_history_batch_engine() {
    const QVector<OhlcvCandle> bars = make_bars();
    for (const Operand& o : streamed_cases()) {
        for (int offset : {0, 1, 5}) {
            SeriesOperandSource series(bars, kWindow);
            int valid = 0;
            for (int bar = 0; bar < bars.size(); ++bar) {
                series.set_bar(bar);
                QString s_err, w_err;
                const double s = series.value(o.indicator, o.params, o.field, offset, &s_err);
                const QVector<OhlcvCandle> history = bars.mid(0, bar + 1);
                WindowOperandSource full(history);
                const double w = full.value(o.indicator, o.params, o.field, offset, &w_err);

                QVERIFY2(s_err == w_err, qPrintable(QStringLiteral("%1 at bar %2: error '%3' vs '%4'")
                                                        .arg(describe(o, offset))
                                                        .arg(bar)
                                                        .arg(s_err, w_err)));
                QVERIFY2(near(s, w, o.tol), qPrintable(QStringLiteral("%1 at bar %2: %3 vs %4")
                                                           .arg(describe(o, offset))
                                                           .arg(bar)
                                                           .arg(s, 0, 'g', 17)
                                                           .arg(w, 0, 'g', 17)));
                if (s_err.isEmpty())
                    ++valid;
            }
            // Really streamed — a silent fallback would make this test vacuous.
            QCOMPARE(series.streamed_operands(), 1);
            QCOMPARE(series.windowed_operands(), 0);
            QVERIFY2(valid > kBars / 2, qPrintable(describe(o, offset)));
        }
    }
}

void TstSeriesOperandSource::finite_memory_operands_are_unchanged_from_the_window_path() {
    // A 500-bar window covers every period here, so the old per-bar recompute saw
    // exactly the bars the series sees: same values, same errors, every bar.
    const QVector<OhlcvCandle> bars = make_bars();
    for (const Operand& o : streamed_cases()) {
        const QString& ind = o.indicator;
        if (ind == "EMA" || ind == "RSI" || ind == "ATR" || ind == "MACD")
            continue; // recursive: next test
        for (int offset : {0, 1}) {
            SeriesOperandSource series(bars, kWindow);
            for (int bar = 0; bar < bars.size(); ++bar) {
                series.set_bar(bar);
                QString s_err, w_err;
                const double s = series.value(o.indicator, o.params, o.field, offset, &s_err);
                const double w = trailing_window_value(bars, bar, o, offset, &w_err);
                QVERIFY2(s_err == w_err && near(s, w, o.tol),
                         qPrintable(QStringLiteral("%1 at bar %2: %3 '%4' vs %5 '%6'")
                                        .arg(describe(o, offset))
                                        .arg(bar)
                                        .arg(s, 0, 'g', 17)
                                        .arg(s_err)
                                        .arg(w, 0, 'g', 17)
                                        .arg(w_err)));
            }
        }
    }
}

void TstSeriesOperandSource::recursive_operands_lose_only_the_window_reseeding_error() {
    const QVector<OhlcvCandle> bars = make_bars();

    // Short periods: the re-seeding error has decayed below anything a condition
    // could see by the time the window is 500 bars long.
    for (const Operand& o : streamed_cases()) {
        const QString& ind = o.indicator;
        if (ind != "EMA" && ind != "RSI" && ind != "ATR" && ind != "MACD")
            continue;
        SeriesOperandSource series(bars, kWindow);
        for (int bar = 0; bar < bars.size(); ++bar) {
            series.set_bar(bar);
            QString s_err, w_err;
            const double s = series.value(o.indicator, o.params, o.field, 0, &s_err);
            const double w = trailing_window_value(bars, bar, o, 0, &w_err);
            QVERIFY2(s_err == w_err && near(s, w, 1e-9),
                     qPrintable(QStringLiteral("%1 at bar %2: %3 vs %4")
                                    .arg(describe(o, 0))
                                    .arg(bar)
                                    .arg(s, 0, 'g', 17)
                                    .arg(w, 0, 'g', 17)));
        }
    }

    // EMA(200): identical while the window still holds the whole history, then the
    // window path drifts — the streamed value is the true full-history EMA.
    const Operand ema200{"EMA", {{"period", 200}}, "value", 1e-12};
    SeriesOperandSource series(bars, kWindow);
    double drift = 0.0;
    for (int bar = 0; bar < bars.size(); ++bar) {
        series.set_bar(bar);
        QString s_err, w_err, f_err;
        const double s = series.value(ema200.indicator, ema200.params, ema200.field, 0, &s_err);
        const double w = trailing_window_value(bars, bar, ema200, 0, &w_err);
        QCOMPARE(s_err, w_err);
        if (bar < kWindow) {
            QVERIFY2(near(s, w, 1e-12), qPrintable(QStringLiteral("bar %1").arg(bar)));
        } else {
            drift = std::max(drift, std::abs(s - w));
            const QVector<OhlcvCandle> history = bars.mid(0, bar + 1);
            WindowOperandSource full(history);
            QVERIFY(near(s, full.value(ema200.indicator, ema200.params, ema200.field, 0, &f_err), 1e-12));
        }
    }
    QVERIFY2(drift > 1e-6, qPrintable(QStringLiteral("EMA(200) window drift only %1").arg(drift)));
}

void TstSeriesOperandSource::operands_without_a_streaming_path_use_the_window_verbatim() {
    const QVector<OhlcvCandle> bars = make_bars();
    const std::vector<Operand> windowed = {
        {"CCI", {{"period", 20}}, "value", 0},
        {"ADX", {{"period", 14}}, "plus_di", 0},
        {"MFI", {{"period", 14}}, "value", 0},
        {"VWAP", {}, "value", 0}, // cumulative over the window it is handed
        {"OBV", {}, "value", 0},  // likewise
        {"SMA", {{"period", 20}}, "upper", 0}, // unknown field: keeps the batch error
        {"NOT_AN_INDICATOR", {}, "value", 0},
    };
    for (const Operand& o : windowed) {
        for (int offset : {0, 1}) {
            SeriesOperandSource series(bars, kWindow);
            for (int bar = 0; bar < bars.size(); ++bar) {
                series.set_bar(bar);
                QString s_err, w_err;
                const double s = series.value(o.indicator, o.params, o.field, offset, &s_err);
                const double w = trailing_window_value(bars, bar, o, offset, &w_err);
                const bool same = (std::isnan(s) && std::isnan(w)) || s == w;
                QVERIFY2(same && s_err == w_err, qPrintable(QStringLiteral("%1 at bar %2").arg(describe(o, offset)).arg(bar)));
            }
            QCOMPARE(series.streamed_operands(), 0);
            QCOMPARE(series.windowed_operands(), 1);
        }
    }
}

void TstSeriesOperandSource::periods_longer_than_the_window_now_warm_up() {
    // Behaviour change, pinned on purpose: SMA(600) never fit in the 500-bar
    // window, so the old path refused it on every bar; the series has the history.
    const QVector<OhlcvCandle> bars = make_bars();
    const Operand sma600{"SMA", {{"period", 600}}, "value", 1e-12};
    SeriesOperandSource series(bars, kWindow);
    for (int bar = 1; bar < bars.size(); ++bar) { // bar 0: one candle, refused by both
        series.set_bar(bar);
        QString s_err, w_err;
        const double s = series.value(sma600.indicator, sma600.params, sma600.field, 0, &s_err);
        trailing_window_value(bars, bar, sma600, 0, &w_err);
        QCOMPARE(w_err, QStringLiteral("Insufficient data for SMA"));
        if (bar < 599) {
            QCOMPARE(s_err, QStringLiteral("Insufficient data for SMA"));
        } else {
            QVERIFY2(s_err.isEmpty() && std::isfinite(s), qPrintable(QStringLiteral("bar %1: %2").arg(bar).arg(s_err)));
        }
    }
}

void TstSeriesOperandSource::offsets_and_cache() {
    const QVector<OhlcvCandle> bars = make_bars();
    SeriesOperandSource series(bars, kWindow);
    series.set_bar(100);
    QString err;

    // Offset beyond the history: the batch engine's wording.
    QVERIFY(std::isnan(series.value("CLOSE", {}, "value", 101, &err)));
    QCOMPARE(err, QStringLiteral("insufficient data for offset 101"));
    // One candle left after the offset: the batch engine refuses a single candle.
    err.clear();
    QVERIFY(std::isnan(series.value("CLOSE", {}, "value", 100, &err)));
    QCOMPARE(err, QStringLiteral("Insufficient data"));
    // A negative offset means "now".
    err.clear();
    QCOMPARE(series.value("CLOSE", {}, "value", -3, &err), bars[100].close);
    QVERIFY(err.isEmpty());
    QCOMPARE(series.value("CLOSE", {}, "value", 7, nullptr), bars[93].close);

    // One series per distinct (indicator, params, field), however often it is read.
    for (int bar = 50; bar < 60; ++bar) {
        series.set_bar(bar);
        series.value("SMA", {{"period", 10}}, "value", 0, nullptr);
        series.value("SMA", {{"period", 10}}, "value", 1, nullptr);
        series.value("SMA", {{"period", 20}}, "value", 0, nullptr);
    }
    QCOMPARE(series.streamed_operands(), 3); // CLOSE, SMA(10), SMA(20)
    series.set_bar(200);
    QVERIFY(series.value("SMA", {{"period", 10}}, "value", 0, nullptr) !=
            series.value("SMA", {{"period", 20}}, "value", 0, nullptr));
}

QTEST_GUILESS_MAIN(TstSeriesOperandSource)
#include "tst_series_operand_source.moc"
