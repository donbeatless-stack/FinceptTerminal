// Unit tests for src/algo_engine/StreamingIndicators.{h,cpp}
//
// The streaming classes promise to reproduce IndicatorEngine's batch output bar
// by bar. Each *_matches_batch case feeds a seeded random walk one bar at a time
// and, at EVERY bar, compares against IndicatorEngine::compute() over the whole
// history up to that bar — quadratic on purpose: the batch engine is the oracle,
// and the comparison also pins that both sides finish warming up on the same bar.
// The walk includes a flat no-trade stretch and a gap, so the zero-range and
// zero-variance branches and the extrema deques' eviction are all exercised.
//
// ZScore and RealizedVolatility are also checked against a two-pass reference and
// hand-computed closed forms; their batch versions keep plain sums instead of
// rolling moments, so those two agree with the batch engine to rounding only.
//
// Links StreamingIndicators.cpp and IndicatorEngine.cpp only — Qt Core + Test.

#include "algo_engine/AlgoEngineTypes.h"
#include "algo_engine/IndicatorEngine.h"
#include "algo_engine/StreamingIndicators.h"

#include <QJsonObject>
#include <QString>
#include <QStringList>
#include <QTest>
#include <QVector>

#include <algorithm>
#include <cmath>
#include <deque>
#include <limits>
#include <random>
#include <vector>

using fincept::algo::IndicatorEngine;
using fincept::algo::IndicatorResult;
using fincept::algo::OhlcvCandle;
namespace st = fincept::algo::streaming;

namespace {

// Same arithmetic in the same order is bit-identical on MSVC x64. Leave 1e-12
// relative so a toolchain that contracts a*b+c into an FMA in only one of the two
// translation units does not fail CI over the last bit.
constexpr double kTightTol = 1e-12;
// Bollinger: sliding Welford (stream) vs two-pass sum of squares (batch).
constexpr double kWelfordTol = 1e-9;

bool near(double got, double want, double rel) {
    if (std::isnan(got) || std::isnan(want))
        return std::isnan(got) && std::isnan(want);
    return std::abs(got - want) <= rel * std::max({1.0, std::abs(got), std::abs(want)});
}

// 400 bars of a log-normal random walk. Bars 150–184 are flat (nothing traded:
// O = H = L = C, zero volume) and bar 260 gaps down 7%.
QVector<OhlcvCandle> make_bars(int n = 400) {
    std::mt19937_64 rng(20261102);
    std::normal_distribution<double> ret(0.0, 0.012);
    std::uniform_real_distribution<double> wick(0.0, 0.004);
    std::uniform_real_distribution<double> vol(1'000.0, 5'000.0);

    QVector<OhlcvCandle> out;
    out.reserve(n);
    double close = 100.0;
    for (int i = 0; i < n; ++i) {
        OhlcvCandle c;
        c.open_time = i;
        c.close_time = i;
        c.open = close;
        if (i >= 150 && i < 185) {
            c.close = c.high = c.low = close;
            c.volume = 0;
        } else {
            if (i == 260)
                close *= 0.93;
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

// Feeds the walk to `feed` one bar at a time and, from the second bar on (the
// batch dispatcher refuses a single candle), checks readiness and every field
// against IndicatorEngine::compute over the same history.
template <class Feed, class Ready, class Read>
void compare_with_batch(const QString& indicator, const QJsonObject& params, const QStringList& fields, double tol,
                        Feed feed, Ready ready, Read read) {
    const QVector<OhlcvCandle> bars = make_bars();
    QVector<OhlcvCandle> history;
    int compared = 0;
    for (int i = 0; i < bars.size(); ++i) {
        history.append(bars[i]);
        feed(bars[i]);
        if (history.size() < 2)
            continue;

        const IndicatorResult batch = IndicatorEngine::compute(indicator, history, params, QString());
        QVERIFY2(ready() == batch.valid, qPrintable(QStringLiteral("%1: readiness differs at bar %2 (stream %3, batch %4)")
                                                        .arg(indicator)
                                                        .arg(i)
                                                        .arg(ready() ? QStringLiteral("ready") : QStringLiteral("warming"))
                                                        .arg(batch.valid ? QStringLiteral("valid") : batch.error)));
        if (!batch.valid)
            continue;
        for (const QString& f : fields) {
            const double want = batch.current.value(f, st::kNaN);
            const double got = read(f);
            QVERIFY2(near(got, want, tol), qPrintable(QStringLiteral("%1.%2 at bar %3: stream %4 vs batch %5")
                                                          .arg(indicator, f)
                                                          .arg(i)
                                                          .arg(got, 0, 'g', 17)
                                                          .arg(want, 0, 'g', 17)));
        }
        ++compared;
    }
    // Guard against a vacuous pass (e.g. an indicator name the batch engine rejects).
    QVERIFY2(compared > bars.size() / 2,
             qPrintable(QStringLiteral("%1: only %2 bars compared").arg(indicator).arg(compared)));
}

QJsonObject period_param(int period) {
    return QJsonObject{{QStringLiteral("period"), period}};
}

// Textbook two-pass variance of a window — the reference for RollingMoments users.
double two_pass_variance(const std::deque<double>& w, bool sample) {
    const double n = static_cast<double>(w.size());
    double sum = 0.0;
    for (double v : w)
        sum += v;
    const double mean = sum / n;
    double m2 = 0.0;
    for (double v : w)
        m2 += (v - mean) * (v - mean);
    return m2 / (sample ? n - 1.0 : n);
}

} // namespace

class TstStreamingIndicators : public QObject {
    Q_OBJECT

  private slots:
    // building blocks
    void ring_buffer_keeps_the_most_recent_samples();
    void rolling_moments_stay_accurate_on_a_long_ill_conditioned_stream();
    void rolling_moments_report_exact_zero_variance_on_a_flat_window();

    // bar-by-bar equivalence with IndicatorEngine
    void sma_matches_batch();
    void ema_matches_batch();
    void rsi_matches_batch();
    void atr_matches_batch();
    void macd_matches_batch();
    void bollinger_matches_batch();
    void donchian_matches_batch();
    void stochastic_matches_batch();
    void williams_r_matches_batch();
    void obv_matches_batch();
    void zscore_matches_batch();
    void realized_volatility_matches_batch();

    // the same two against an independent reference
    void zscore_matches_two_pass_reference();
    void zscore_closed_form();
    void realized_volatility_matches_two_pass_reference();
    void realized_volatility_closed_form();

    // input hygiene
    void non_finite_ticks_are_ignored();
    void degenerate_periods_are_clamped();
};

// ── Building blocks ──────────────────────────────────────────────────────────

void TstStreamingIndicators::ring_buffer_keeps_the_most_recent_samples() {
    st::RingBuffer rb(3);
    QCOMPARE(rb.capacity(), 3);
    rb.push(1);
    rb.push(2);
    QVERIFY(!rb.full());
    QCOMPARE(rb.oldest(), 1.0);
    for (int x = 3; x <= 5; ++x)
        rb.push(x);
    QVERIFY(rb.full());
    QCOMPARE(rb.size(), 3);
    QCOMPARE(rb.oldest(), 3.0);
    QCOMPARE(rb.at(0), 3.0);
    QCOMPARE(rb.at(1), 4.0);
    QCOMPARE(rb.at(2), 5.0);
}

void TstStreamingIndicators::rolling_moments_stay_accurate_on_a_long_ill_conditioned_stream() {
    // Price level 1e5 with 0.01 noise: the classic catastrophic-cancellation case
    // for naive Σx² - (Σx)²/n. 300k pushes is ~300 forced re-anchors.
    constexpr int kWindow = 50;
    std::mt19937_64 rng(7);
    std::normal_distribution<double> noise(0.0, 0.01);
    st::RollingMoments m(kWindow);
    std::deque<double> window;
    double worst = 0.0;
    for (int i = 1; i <= 300'000; ++i) {
        const double x = 1e5 + noise(rng);
        m.push(x);
        window.push_back(x);
        if (static_cast<int>(window.size()) > kWindow)
            window.pop_front();
        if (i % 997 == 0) {
            const double want = two_pass_variance(window, false);
            worst = std::max(worst, std::abs(m.variance_population() - want) / want);
        }
    }
    QVERIFY2(worst < 1e-6, qPrintable(QStringLiteral("worst relative variance error %1").arg(worst)));
}

void TstStreamingIndicators::rolling_moments_report_exact_zero_variance_on_a_flat_window() {
    // A volatile stretch leaves rounding residue in M2; once the window is all one
    // price the variance must be exactly 0, not ~1e-15 surfacing through sqrt().
    st::RollingMoments m(5);
    for (double x : {101.3, 97.2, 104.9, 99.1, 102.6, 98.4})
        m.push(x);
    QVERIFY(m.variance_population() > 1.0);
    for (int i = 0; i < 5; ++i)
        m.push(100.07);
    QCOMPARE(m.variance_population(), 0.0);
    QCOMPARE(m.variance_sample(), 0.0);
    QCOMPARE(m.mean(), 100.07);
}

// ── Equivalence with the batch engine ────────────────────────────────────────

void TstStreamingIndicators::sma_matches_batch() {
    st::Sma s(20);
    compare_with_batch(
        "SMA", period_param(20), {"value"}, kTightTol, [&](const OhlcvCandle& c) { s.update(c.close); },
        [&] { return s.ready(); }, [&](const QString&) { return s.value(); });
}

void TstStreamingIndicators::ema_matches_batch() {
    st::Ema s(20);
    compare_with_batch(
        "EMA", period_param(20), {"value"}, kTightTol, [&](const OhlcvCandle& c) { s.update(c.close); },
        [&] { return s.ready(); }, [&](const QString&) { return s.value(); });
}

void TstStreamingIndicators::rsi_matches_batch() {
    st::Rsi s(14);
    compare_with_batch(
        "RSI", period_param(14), {"value"}, kTightTol, [&](const OhlcvCandle& c) { s.update(c.close); },
        [&] { return s.ready(); }, [&](const QString&) { return s.value(); });
}

void TstStreamingIndicators::atr_matches_batch() {
    st::Atr s(14);
    compare_with_batch(
        "ATR", period_param(14), {"value"}, kTightTol,
        [&](const OhlcvCandle& c) { s.update(c.high, c.low, c.close); }, [&] { return s.ready(); },
        [&](const QString&) { return s.value(); });
}

void TstStreamingIndicators::macd_matches_batch() {
    st::Macd s(12, 26, 9);
    const QJsonObject params{{"fast", 12}, {"slow", 26}, {"signal", 9}};
    compare_with_batch(
        "MACD", params, {"line", "signal_line", "histogram"}, kTightTol,
        [&](const OhlcvCandle& c) { s.update(c.close); }, [&] { return s.ready(); },
        [&](const QString& f) {
            if (f == "line")
                return s.line();
            if (f == "signal_line")
                return s.signal();
            return s.histogram();
        });
}

void TstStreamingIndicators::bollinger_matches_batch() {
    st::Bollinger s(20, 2.0);
    const QJsonObject params{{"period", 20}, {"std_dev", 2.0}};
    compare_with_batch(
        "BOLLINGER", params, {"upper", "middle", "lower", "width", "pct_b"}, kWelfordTol,
        [&](const OhlcvCandle& c) { s.update(c.close); }, [&] { return s.ready(); },
        [&](const QString& f) {
            const auto b = s.bands();
            if (f == "upper")
                return b.upper;
            if (f == "middle")
                return b.middle;
            if (f == "lower")
                return b.lower;
            if (f == "width")
                return b.width;
            return b.pct_b;
        });
}

void TstStreamingIndicators::donchian_matches_batch() {
    st::Donchian s(20);
    compare_with_batch(
        "DONCHIAN", period_param(20), {"upper", "lower"}, kTightTol,
        [&](const OhlcvCandle& c) { s.update(c.high, c.low); }, [&] { return s.ready(); },
        [&](const QString& f) { return f == "upper" ? s.upper() : s.lower(); });
}

void TstStreamingIndicators::stochastic_matches_batch() {
    st::Stochastic s(14, 3);
    const QJsonObject params{{"k_period", 14}, {"d_period", 3}};
    compare_with_batch(
        "STOCHASTIC", params, {"k", "d"}, kTightTol,
        [&](const OhlcvCandle& c) { s.update(c.high, c.low, c.close); }, [&] { return s.ready(); },
        [&](const QString& f) { return f == "k" ? s.k() : s.d(); });
}

void TstStreamingIndicators::williams_r_matches_batch() {
    st::WilliamsR s(14);
    compare_with_batch(
        "WILLIAMS_R", period_param(14), {"value"}, kTightTol,
        [&](const OhlcvCandle& c) { s.update(c.high, c.low, c.close); }, [&] { return s.ready(); },
        [&](const QString&) { return s.value(); });
}

void TstStreamingIndicators::obv_matches_batch() {
    st::Obv s;
    compare_with_batch(
        "OBV", QJsonObject{}, {"value"}, kTightTol, [&](const OhlcvCandle& c) { s.update(c.close, c.volume); },
        [&] { return s.ready(); }, [&](const QString&) { return s.value(); });
}

void TstStreamingIndicators::zscore_matches_batch() {
    // Rolling moments (stream) vs two-pass sums (batch): rounding-level agreement.
    // The flat stretch at bars 150-184 exercises the zero-dispersion branch on both.
    st::ZScore s(20);
    compare_with_batch(
        "ZSCORE", period_param(20), {"value"}, kWelfordTol, [&](const OhlcvCandle& c) { s.update(c.close); },
        [&] { return s.ready(); }, [&](const QString&) { return s.value(); });
}

void TstStreamingIndicators::realized_volatility_matches_batch() {
    // US 15-minute bars: 26 a session.
    st::RealizedVolatility s(20, 252.0 * 26);
    compare_with_batch(
        "REALIZED_VOL", QJsonObject{{"period", 20}, {"periods_per_year", 252 * 26}}, {"value"}, kWelfordTol,
        [&](const OhlcvCandle& c) { s.update(c.close); }, [&] { return s.ready(); },
        [&](const QString&) { return s.value(); });
}

// ── Against an independent reference ─────────────────────────────────────────

void TstStreamingIndicators::zscore_matches_two_pass_reference() {
    constexpr int kPeriod = 20;
    st::ZScore z(kPeriod);
    std::deque<double> window;
    int compared = 0;
    for (const OhlcvCandle& c : make_bars()) {
        z.update(c.close);
        window.push_back(c.close);
        if (static_cast<int>(window.size()) > kPeriod)
            window.pop_front();
        QCOMPARE(z.ready(), static_cast<int>(window.size()) == kPeriod);
        if (!z.ready())
            continue;
        double mean = 0.0;
        for (double v : window)
            mean += v;
        mean /= kPeriod;
        const double sd = std::sqrt(two_pass_variance(window, false));
        const double want = sd <= 1e-9 * std::max(1.0, std::abs(mean)) ? 0.0 : (c.close - mean) / sd;
        QVERIFY2(near(z.value(), want, kWelfordTol),
                 qPrintable(QStringLiteral("z %1 vs %2").arg(z.value(), 0, 'g', 17).arg(want, 0, 'g', 17)));
        ++compared;
    }
    QVERIFY(compared > 300);
}

void TstStreamingIndicators::zscore_closed_form() {
    // Window {1,2,3,4,5}: mean 3, population sd √2, so z(5) = 2/√2 = √2.
    st::ZScore z(5);
    for (int x = 1; x <= 5; ++x)
        z.update(x);
    QVERIFY(near(z.value(), std::sqrt(2.0), kTightTol));
    // A flat window has no dispersion to measure against: 0, never ±inf / NaN.
    for (int i = 0; i < 5; ++i)
        z.update(42.0);
    QCOMPARE(z.value(), 0.0);
}

void TstStreamingIndicators::realized_volatility_matches_two_pass_reference() {
    constexpr int kPeriod = 20;
    st::RealizedVolatility rv(kPeriod, 252.0);
    std::deque<double> returns;
    double prev = 0.0;
    bool has_prev = false;
    int compared = 0;
    for (const OhlcvCandle& c : make_bars()) {
        rv.update(c.close);
        if (has_prev) {
            returns.push_back(std::log(c.close / prev));
            if (static_cast<int>(returns.size()) > kPeriod)
                returns.pop_front();
        }
        prev = c.close;
        has_prev = true;
        QCOMPARE(rv.ready(), static_cast<int>(returns.size()) == kPeriod);
        if (!rv.ready())
            continue;
        const double want = std::sqrt(two_pass_variance(returns, true)) * std::sqrt(252.0);
        QVERIFY2(near(rv.value(), want, kWelfordTol),
                 qPrintable(QStringLiteral("rv %1 vs %2").arg(rv.value(), 0, 'g', 17).arg(want, 0, 'g', 17)));
        ++compared;
    }
    QVERIFY(compared > 300);
}

void TstStreamingIndicators::realized_volatility_closed_form() {
    // Closes alternate 100, 110, 100, … so log returns alternate ±r, r = ln 1.1.
    // Over an even window the mean return is 0 and the sample variance is
    // n·r² / (n - 1), so σ_annual = r · √(n / (n - 1)) · √252.
    constexpr int kPeriod = 10;
    st::RealizedVolatility rv(kPeriod, 252.0);
    for (int i = 0; i <= kPeriod; ++i) {
        QVERIFY(!rv.ready());
        rv.update(i % 2 == 0 ? 100.0 : 110.0);
    }
    QVERIFY(rv.ready());
    const double r = std::log(1.1);
    const double want = r * std::sqrt(static_cast<double>(kPeriod) / (kPeriod - 1)) * std::sqrt(252.0);
    QVERIFY2(near(rv.value(), want, kWelfordTol),
             qPrintable(QStringLiteral("%1 vs %2").arg(rv.value(), 0, 'g', 17).arg(want, 0, 'g', 17)));

    // A constant price has zero realised volatility, exactly.
    for (int i = 0; i <= kPeriod; ++i)
        rv.update(105.0);
    QCOMPARE(rv.value(), 0.0);
}

// ── Input hygiene ────────────────────────────────────────────────────────────

void TstStreamingIndicators::non_finite_ticks_are_ignored() {
    // Two copies of each indicator see the same walk; one also gets NaN / ±inf
    // (and, for realised vol, non-positive) ticks between real bars. A rejected
    // tick must leave no trace, so the two must end bit-identical.
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const double inf = std::numeric_limits<double>::infinity();
    const double junk[] = {nan, inf, -inf};

    st::Ema ema_a(20), ema_b(20);
    st::Rsi rsi_a(14), rsi_b(14);
    st::Atr atr_a(14), atr_b(14);
    st::Macd macd_a(12, 26, 9), macd_b(12, 26, 9);
    st::Bollinger bb_a(20, 2.0), bb_b(20, 2.0);
    st::Stochastic sto_a(14, 3), sto_b(14, 3);
    st::RealizedVolatility rv_a(20), rv_b(20);
    st::Obv obv_a, obv_b;

    int k = 0;
    for (const OhlcvCandle& c : make_bars()) {
        ema_a.update(c.close);
        rsi_a.update(c.close);
        atr_a.update(c.high, c.low, c.close);
        macd_a.update(c.close);
        bb_a.update(c.close);
        sto_a.update(c.high, c.low, c.close);
        rv_a.update(c.close);
        obv_a.update(c.close, c.volume);

        const double j = junk[k++ % 3];
        ema_b.update(j);
        rsi_b.update(j);
        atr_b.update(c.high, j, c.close);
        macd_b.update(j);
        bb_b.update(j);
        sto_b.update(j, c.low, c.close);
        rv_b.update(j);
        rv_b.update(0.0);
        rv_b.update(-c.close);
        obv_b.update(c.close, j);

        ema_b.update(c.close);
        rsi_b.update(c.close);
        atr_b.update(c.high, c.low, c.close);
        macd_b.update(c.close);
        bb_b.update(c.close);
        sto_b.update(c.high, c.low, c.close);
        rv_b.update(c.close);
        obv_b.update(c.close, c.volume);
    }

    QVERIFY(ema_a.ready() && rsi_a.ready() && atr_a.ready() && macd_a.ready());
    QCOMPARE(ema_b.value(), ema_a.value());
    QCOMPARE(rsi_b.value(), rsi_a.value());
    QCOMPARE(atr_b.value(), atr_a.value());
    QCOMPARE(macd_b.line(), macd_a.line());
    QCOMPARE(macd_b.signal(), macd_a.signal());
    QCOMPARE(bb_b.bands().upper, bb_a.bands().upper);
    QCOMPARE(bb_b.bands().pct_b, bb_a.bands().pct_b);
    QCOMPARE(sto_b.k(), sto_a.k());
    QCOMPARE(sto_b.d(), sto_a.d());
    QCOMPARE(rv_b.value(), rv_a.value());
    QCOMPARE(obv_b.value(), obv_a.value());
}

void TstStreamingIndicators::degenerate_periods_are_clamped() {
    // Same rule as IndicatorEngine::compute: a period below 1 behaves as 1, so a
    // malformed strategy cannot divide by zero or index out of range.
    st::Sma sma(0);
    st::Ema ema(-5);
    sma.update(7.0);
    ema.update(7.0);
    QVERIFY(sma.ready() && ema.ready());
    QCOMPARE(sma.value(), 7.0);
    QCOMPARE(ema.value(), 7.0);
    sma.update(9.0);
    ema.update(9.0);
    QCOMPARE(sma.value(), 9.0);
    QCOMPARE(ema.value(), 9.0); // multiplier 2/(1+1) = 1: tracks the input

    // A sample variance needs two returns, so realised vol clamps its period to 2.
    st::RealizedVolatility rv(1);
    rv.update(100.0);
    rv.update(101.0);
    QVERIFY(!rv.ready());
    rv.update(100.0);
    QVERIFY(rv.ready());
    QVERIFY(rv.value() > 0.0);
}

QTEST_GUILESS_MAIN(TstStreamingIndicators)
#include "tst_streaming_indicators.moc"
