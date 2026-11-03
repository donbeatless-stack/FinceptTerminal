// Unit tests for src/algo_engine/LiveOperandSource.{h,cpp} — the per-symbol
// operand state RealtimeScanRunner evaluates through.
//
// RealtimeScanRunner used to evaluate every sweep on its candle buffer (the
// last required_bars + 50 closed candles) plus synthetic bars for the previous
// and current tick, recomputing each indicator. LiveOperandSource keeps streamed
// operands as state over every closed candle and applies the tick bars to a
// copy. These tests replay that flow — warm-up, then ticks and closes — and pin:
//   1. Streamed operands equal WindowOperandSource over the FULL live window
//      (every closed candle + the tick bars): value, error text, warm-up bar,
//      at offsets 0-3, with one tick bar or two.
//   2. Against the old candle-buffer window, finite-memory operands are unchanged.
//   3. Unstreamable operands and reads deeper than registered use the window.
//
// Links LiveOperandSource, StreamingOperand, StreamingIndicators, OperandSource
// and IndicatorEngine — Qt Core + Test only.

#include "algo_engine/AlgoEngineTypes.h"
#include "algo_engine/LiveOperandSource.h"
#include "algo_engine/OperandSource.h"

#include <QJsonDocument>
#include <QJsonObject>
#include <QString>
#include <QTest>
#include <QVector>

#include <algorithm>
#include <cmath>
#include <random>
#include <vector>

using fincept::algo::LiveOperandSource;
using fincept::algo::OhlcvCandle;
using fincept::algo::OperandRef;
using fincept::algo::WindowOperandSource;

namespace {

constexpr int kWarm = 300;  // bars handed to reset()
constexpr int kBars = 400;  // the rest arrive live, one close at a time
constexpr int kDepth = 3;   // registered max_offset

QVector<OhlcvCandle> make_bars() {
    std::mt19937_64 rng(4242);
    std::normal_distribution<double> ret(0.0, 0.011);
    std::uniform_real_distribution<double> wick(0.0, 0.004);
    QVector<OhlcvCandle> out;
    double close = 80.0;
    for (int i = 0; i < kBars; ++i) {
        OhlcvCandle c;
        c.open_time = c.close_time = i;
        c.open = close;
        if (i >= 320 && i < 345) { // a quiet stretch arriving live
            c.close = c.high = c.low = close;
            c.volume = 0;
        } else {
            close *= std::exp(ret(rng));
            c.close = close;
            c.high = std::max(c.open, c.close) * (1.0 + wick(rng));
            c.low = std::min(c.open, c.close) * (1.0 - wick(rng));
            c.volume = 1'500;
        }
        c.is_closed = true;
        out.append(c);
    }
    return out;
}

OhlcvCandle tick(double price) {
    OhlcvCandle c;
    c.open = c.high = c.low = c.close = price;
    c.volume = 0;
    c.is_closed = true;
    return c;
}

struct Case {
    OperandRef ref;
    double tol;
};

std::vector<Case> streamed_cases() {
    const QJsonObject bb{{"period", 20}, {"std_dev", 2.0}};
    auto ref = [](const char* ind, const QJsonObject& params, const char* field) {
        return OperandRef{ind, params, field, kDepth};
    };
    return {
        {ref("CLOSE", {}, "value"), 1e-12},
        {ref("HIGH", {}, "value"), 1e-12},
        {ref("SMA", {{"period", 50}}, "value"), 1e-12},
        {ref("EMA", {{"period", 200}}, "value"), 1e-12},
        {ref("EMA", {{"period", 12}}, "value"), 1e-12},
        {ref("RSI", {{"period", 14}}, "value"), 1e-12},
        {ref("ATR", {{"period", 14}}, "value"), 1e-12},
        {ref("MACD", {}, "histogram"), 1e-12},
        {ref("BOLLINGER", bb, "upper"), 1e-9},
        {ref("BOLLINGER", bb, "pct_b"), 1e-9},
        {ref("DONCHIAN", {{"period", 20}}, "lower"), 1e-12},
        {ref("STOCHASTIC", {}, "d"), 1e-12},
        {ref("WILLIAMS_R", {{"period", 14}}, "value"), 1e-12},
    };
}

bool near(double got, double want, double rel) {
    if (std::isnan(got) || std::isnan(want))
        return std::isnan(got) && std::isnan(want);
    return std::abs(got - want) <= rel * std::max({1.0, std::abs(got), std::abs(want)});
}

QString describe(const OperandRef& r, int offset) {
    return QStringLiteral("%1(%2).%3 offset %4")
        .arg(r.indicator, QString::fromUtf8(QJsonDocument(r.params).toJson(QJsonDocument::Compact)), r.field)
        .arg(offset);
}

// Replays the live flow for every case: reset() on the first kWarm bars, then
// for each later bar evaluate with intrabar tick bars, then close it. `check`
// gets the source, the closed candles so far and the tick bars.
template <class Check>
void replay(const std::vector<OperandRef>& refs, Check check) {
    const QVector<OhlcvCandle> bars = make_bars();
    LiveOperandSource live(refs);
    live.reset(bars.mid(0, kWarm));
    QVector<OhlcvCandle> closed = bars.mid(0, kWarm);
    for (int i = kWarm; i < kBars; ++i) {
        const OhlcvCandle& b = bars[i];
        // Every other bar has no previous tick yet, as right after a restart.
        const QVector<OhlcvCandle> ticks =
            (i % 2) ? QVector<OhlcvCandle>{tick(b.open), tick(b.close)} : QVector<OhlcvCandle>{tick(b.close)};
        live.set_ticks(ticks);
        check(live, closed, ticks, i);
        if (QTest::currentTestFailed())
            return;
        live.on_closed(b);
        closed.append(b);
    }
}

} // namespace

class TstLiveOperandSource : public QObject {
    Q_OBJECT

  private slots:
    void streamed_operands_match_the_full_live_window();
    void finite_memory_operands_match_the_old_buffer_window();
    void unstreamable_operands_and_deep_reads_use_the_window();
    void reset_replaces_all_state();
};

void TstLiveOperandSource::streamed_operands_match_the_full_live_window() {
    for (const Case& c : streamed_cases()) {
        const std::vector<OperandRef> refs{c.ref};
        replay(refs, [&](LiveOperandSource& live, const QVector<OhlcvCandle>& closed,
                         const QVector<OhlcvCandle>& ticks, int bar) {
            QVERIFY(!live.needs_window());
            QCOMPARE(live.bar_count(), closed.size() + ticks.size());
            QVector<OhlcvCandle> full = closed;
            full.append(ticks);
            WindowOperandSource reference(full);
            for (int offset = 0; offset <= kDepth; ++offset) {
                QString l_err, w_err;
                const double l = live.value(c.ref.indicator, c.ref.params, c.ref.field, offset, &l_err);
                const double w = reference.value(c.ref.indicator, c.ref.params, c.ref.field, offset, &w_err);
                QVERIFY2(l_err == w_err && near(l, w, c.tol),
                         qPrintable(QStringLiteral("%1 at bar %2: %3 '%4' vs %5 '%6'")
                                        .arg(describe(c.ref, offset))
                                        .arg(bar)
                                        .arg(l, 0, 'g', 17)
                                        .arg(l_err)
                                        .arg(w, 0, 'g', 17)
                                        .arg(w_err)));
            }
        });
    }
}

void TstLiveOperandSource::finite_memory_operands_match_the_old_buffer_window() {
    // RealtimeScanRunner's buffer is required_bars + 50 closed candles: enough for
    // finite-memory indicators to see the same bars, so live alerts on them are
    // unchanged. (Recursive ones are the intended difference — they used to be
    // re-seeded at the buffer start.)
    for (const Case& c : streamed_cases()) {
        const QString& ind = c.ref.indicator;
        if (ind == "EMA" || ind == "RSI" || ind == "ATR" || ind == "MACD")
            continue;
        const int buffer = std::max(c.ref.params.value("period").toInt(0) + 2 + 50, 60);
        const std::vector<OperandRef> refs{c.ref};
        replay(refs, [&](LiveOperandSource& live, const QVector<OhlcvCandle>& closed,
                         const QVector<OhlcvCandle>& ticks, int bar) {
            QVector<OhlcvCandle> window = closed.mid(std::max<qsizetype>(0, closed.size() - buffer));
            window.append(ticks);
            WindowOperandSource old_path(window);
            for (int offset = 0; offset <= 1; ++offset) {
                QString l_err, w_err;
                const double l = live.value(c.ref.indicator, c.ref.params, c.ref.field, offset, &l_err);
                const double w = old_path.value(c.ref.indicator, c.ref.params, c.ref.field, offset, &w_err);
                QVERIFY2(l_err == w_err && near(l, w, c.tol),
                         qPrintable(QStringLiteral("%1 at bar %2").arg(describe(c.ref, offset)).arg(bar)));
            }
        });
    }
}

void TstLiveOperandSource::unstreamable_operands_and_deep_reads_use_the_window() {
    const OperandRef cci{"CCI", {{"period", 20}}, "value", 1};
    const OperandRef vwap{"VWAP", {}, "value", 0};
    const OperandRef ema{"EMA", {{"period", 12}}, "value", 1}; // registered 1 deep
    const std::vector<OperandRef> refs{cci, vwap, ema};
    replay(refs, [&](LiveOperandSource& live, const QVector<OhlcvCandle>& closed, const QVector<OhlcvCandle>& ticks,
                     int bar) {
        QVERIFY(live.needs_window());
        QVector<OhlcvCandle> window = closed.mid(closed.size() - 72); // a buffer
        window.append(ticks);

        // Without a window the unstreamable operand cannot be served.
        live.set_window(nullptr);
        QString err;
        QVERIFY(std::isnan(live.value("CCI", cci.params, "value", 0, &err)));
        QVERIFY(err.contains("no live candle window"));

        live.set_window(&window);
        WindowOperandSource old_path(window);
        for (const OperandRef& r : {cci, vwap}) {
            for (int offset = 0; offset <= 1; ++offset) {
                QString l_err, w_err;
                const double l = live.value(r.indicator, r.params, r.field, offset, &l_err);
                const double w = old_path.value(r.indicator, r.params, r.field, offset, &w_err);
                QVERIFY2(l_err == w_err && ((std::isnan(l) && std::isnan(w)) || l == w),
                         qPrintable(QStringLiteral("%1 at bar %2").arg(describe(r, offset)).arg(bar)));
            }
        }
        // EMA read deeper than registered (offset 4 lands past its kept values):
        // served by the window rather than refused.
        QString l_err, w_err;
        const double l = live.value("EMA", ema.params, "value", 4, &l_err);
        const double w = old_path.value("EMA", ema.params, "value", 4, &w_err);
        QVERIFY2(l_err == w_err && ((std::isnan(l) && std::isnan(w)) || l == w),
                 qPrintable(QStringLiteral("deep EMA read at bar %1").arg(bar)));
        live.set_window(nullptr);
    });
}

void TstLiveOperandSource::reset_replaces_all_state() {
    // A second warm-up (watch restarted, history refetched) must leave no trace of
    // the first one or of the closes in between.
    const QVector<OhlcvCandle> bars = make_bars();
    const std::vector<OperandRef> refs{{"EMA", {{"period", 20}}, "value", 2}, {"DONCHIAN", {{"period", 10}}, "upper", 2}};

    LiveOperandSource reused(refs);
    reused.reset(bars.mid(0, 150));
    for (int i = 150; i < 200; ++i)
        reused.on_closed(bars[i]);
    reused.reset(bars.mid(50, 250));

    LiveOperandSource fresh(refs);
    fresh.reset(bars.mid(50, 250));

    const QVector<OhlcvCandle> ticks{tick(bars[300].open), tick(bars[300].close)};
    reused.set_ticks(ticks);
    fresh.set_ticks(ticks);
    QCOMPARE(reused.bar_count(), fresh.bar_count());
    for (const OperandRef& r : refs)
        for (int offset = 0; offset <= 2; ++offset)
            QCOMPARE(reused.value(r.indicator, r.params, r.field, offset, nullptr),
                     fresh.value(r.indicator, r.params, r.field, offset, nullptr));
}

QTEST_GUILESS_MAIN(TstLiveOperandSource)
#include "tst_live_operand_source.moc"
