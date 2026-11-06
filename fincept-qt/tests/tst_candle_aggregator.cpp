// tst_candle_aggregator.cpp — CandleAggregator: ticks into bars after a warm-up.
//
// Found in a live scan (2026-11-06): a REST quote stamped in seconds reached the
// scan before the first live tick. Read as milliseconds it opened a bar in January
// 1970, which that live tick then closed into the series, so the live price went
// into the EMA once more than it should. The first tick must never open a bar that
// ends before the warm history does.

#include "algo_engine/CandleAggregator.h"

#include <QSignalSpy>
#include <QtTest/QtTest>

using namespace fincept::algo;

namespace {

constexpr int64_t kMin = 60'000;
constexpr int64_t kBar = 15 * kMin;
// 2026-11-06 15:15 US Eastern (20:15 UTC), a 15m boundary.
constexpr int64_t k1515 = 1'793'996'100'000;

OhlcvCandle bar(int64_t open_time, double close) {
    OhlcvCandle c;
    c.open_time = open_time;
    c.close_time = open_time + kBar;
    c.open = c.high = c.low = c.close = close;
    c.volume = 100;
    c.is_closed = true;
    return c;
}

// Three warm bars, the last one 15:15-15:30.
QVector<OhlcvCandle> history() {
    return {bar(k1515 - 2 * kBar, 100.0), bar(k1515 - kBar, 101.0), bar(k1515, 102.0)};
}

} // namespace

class TstCandleAggregator : public QObject {
    Q_OBJECT
  private slots:
    void ticks_close_a_bar_at_the_period_boundary();
    void a_tick_in_seconds_does_not_open_a_bar_in_1970();
    void a_late_print_before_the_warm_history_ends_is_not_a_bar();
    void without_history_the_first_tick_opens_its_bar();
};

void TstCandleAggregator::ticks_close_a_bar_at_the_period_boundary() {
    CandleAggregator agg("TSLA", Timeframe::M15);
    agg.warm_from(history());
    QSignalSpy closed(&agg, &CandleAggregator::candle_closed);

    const int64_t t1530 = k1515 + kBar;
    agg.on_tick(360.0, 10, t1530 + 12 * kMin); // 15:42
    agg.on_tick(361.0, 5, t1530 + 13 * kMin);
    agg.on_tick(359.5, 5, t1530 + 14 * kMin);
    QCOMPARE(closed.count(), 0);

    agg.on_tick(360.5, 1, t1530 + kBar + 1000); // 15:45:01 closes 15:30-15:45
    QCOMPARE(closed.count(), 1);
    const auto c = closed.at(0).at(0).value<OhlcvCandle>();
    QCOMPARE(c.open_time, t1530);
    QCOMPARE(c.open, 360.0);
    QCOMPARE(c.high, 361.0);
    QCOMPARE(c.low, 359.5);
    QCOMPARE(c.close, 359.5);
    QCOMPARE(c.volume, 20.0);

    const auto bars = agg.closed_candles();
    QCOMPARE(bars.size(), 4);
    QCOMPARE(bars.constLast().open_time, t1530);
    QCOMPARE(agg.current_candle().open_time, t1530 + kBar);
}

void TstCandleAggregator::a_tick_in_seconds_does_not_open_a_bar_in_1970() {
    CandleAggregator agg("TSLA", Timeframe::M15);
    agg.warm_from(history());
    QSignalSpy closed(&agg, &CandleAggregator::candle_closed);

    const int64_t t1542 = k1515 + kBar + 12 * kMin;
    agg.on_tick(360.3, 0, t1542 / 1000); // the REST quote: seconds where ms are meant
    agg.on_tick(360.2, 10, t1542 + 500); // the first live tick
    agg.on_tick(360.4, 10, t1542 + 900);
    QCOMPARE(closed.count(), 0); // before the fix: the 1970 bar, closed into the series

    const auto bars = agg.closed_candles();
    QCOMPARE(bars.size(), 3);
    for (const auto& c : bars)
        QVERIFY(c.open_time >= k1515 - 2 * kBar);
    QCOMPARE(agg.current_candle().open_time, k1515 + kBar);
    QCOMPARE(agg.current_candle().open, 360.2);
    QCOMPARE(agg.current_candle().close, 360.4);
}

void TstCandleAggregator::a_late_print_before_the_warm_history_ends_is_not_a_bar() {
    CandleAggregator agg("TSLA", Timeframe::M15);
    agg.warm_from(history());
    QSignalSpy closed(&agg, &CandleAggregator::candle_closed);

    agg.on_tick(101.5, 1, k1515 + kBar - 1);  // 15:29:59.999, inside the last warm bar
    agg.on_tick(103.0, 1, k1515 + kBar);      // 15:30:00.000, the first bar after it
    QCOMPARE(closed.count(), 0);
    QCOMPARE(agg.closed_candles().size(), 3);
    QCOMPARE(agg.current_candle().open_time, k1515 + kBar);
    QCOMPARE(agg.current_candle().open, 103.0);
}

void TstCandleAggregator::without_history_the_first_tick_opens_its_bar() {
    // DeploymentRunner may run before (or without) a warm-up: unchanged there.
    CandleAggregator agg("TSLA", Timeframe::M15);
    QSignalSpy closed(&agg, &CandleAggregator::candle_closed);
    agg.on_tick(50.0, 1, k1515 + 3 * kMin);
    QCOMPARE(agg.current_candle().open_time, k1515);
    agg.on_tick(51.0, 1, k1515 + kBar);
    QCOMPARE(closed.count(), 1);
    QCOMPARE(agg.closed_candles().size(), 1);
}

QTEST_GUILESS_MAIN(TstCandleAggregator)
#include "tst_candle_aggregator.moc"
