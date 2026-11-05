// Unit tests for src/algo_engine/GatewayHistory.{h,cpp}: the terminal's side of
// the gateway's HISTORY command. The reply parser, and above all the rule that
// decides which symbols the gateway's history may replace the broker's for: a
// wrong "yes" warms a scan on a gap and every indicator is silently off.
//
// Qt Core + Test only.

#include "algo_engine/GatewayHistory.h"

#include <QTest>

using namespace fincept::algo::gateway_history;
using fincept::algo::OhlcvCandle;

namespace {

constexpr qint64 kT = 1'793'818'800; // 2026-11-04 19:00:00 UTC
constexpr qint64 k15 = 15 * 60 * 1000;

QByteArray reply(qint64 complete_to, const QList<QByteArray>& bars) {
    QByteArray r = "BARS " + QByteArray::number(bars.size()) + ' ' + QByteArray::number(complete_to) + '\n';
    for (const auto& b : bars)
        r += b + '\n';
    return r + "END\n";
}

QByteArray bar(const char* sym, qint64 start_s, double close = 100) {
    return QByteArray(sym) + ' ' + QByteArray::number(start_s) + " 99 101 98 " + QByteArray::number(close) + " 1000";
}

} // namespace

class TstGatewayHistory : public QObject {
    Q_OBJECT

  private slots:
    void serves_intraday_timeframes_only();
    void the_window_starts_at_utc_midnight_like_the_broker_path();
    void builds_the_request_line();
    void parses_a_reply();
    void rejects_malformed_replies();
    void accepts_covered_symbols_and_sends_the_rest_to_the_broker();
    void a_stale_or_absent_sync_is_not_trusted();
    void drops_periods_the_sync_has_not_finished();
    void a_symbol_with_too_little_history_goes_to_the_broker();
};

void TstGatewayHistory::serves_intraday_timeframes_only() {
    for (const char* tf : {"1m", "3m", "5m", "10m", "15m", "30m", "1h", "4h"})
        QCOMPARE(gateway_timeframe(tf), QString(tf));
    QVERIFY(gateway_timeframe("1d").isEmpty()); // daily bars need the exchange calendar
    QVERIFY(gateway_timeframe("live").isEmpty());
}

void TstGatewayHistory::the_window_starts_at_utc_midnight_like_the_broker_path() {
    // 55 days before 2026-11-05 01:33:05 UTC is 2026-09-11 01:33:05; the broker path
    // asks from that date, so from 2026-09-11 00:00:00 UTC. (Values from `date -u`.)
    QCOMPARE(window_start_ms(1'793'842'385'000, 55), 1'789'084'800'000LL);
    QCOMPARE(window_start_ms(1'789'084'800'000, 0), 1'789'084'800'000LL); // already midnight
}

void TstGatewayHistory::builds_the_request_line() {
    QCOMPARE(request_line("15m", kT, kT + 3600, {"AAPL", "MSFT"}),
             QByteArray("HISTORY 15m 1793818800 1793822400 AAPL MSFT\n"));
}

void TstGatewayHistory::parses_a_reply() {
    const Reply r = parse_reply(reply(kT + 1800, {bar("AAPL", kT, 150.25), bar("AAPL", kT + 900), bar("MSFT", kT)}), k15);
    QVERIFY2(r.ok, qPrintable(r.error));
    QCOMPARE(r.complete_to_s, kT + 1800);
    QCOMPARE(r.bars.size(), 2);
    const auto& aapl = r.bars["AAPL"];
    QCOMPARE(aapl.size(), 2);
    QCOMPARE(aapl[0].open_time, kT * 1000);
    QCOMPARE(aapl[0].close_time, kT * 1000 + k15);
    QCOMPARE(aapl[0].open, 99.0);
    QCOMPARE(aapl[0].close, 150.25);
    QCOMPARE(aapl[0].volume, 1000.0);
    QVERIFY(aapl[0].is_closed);

    const Reply empty = parse_reply("BARS 0 1793820600\nEND\n", k15);
    QVERIFY(empty.ok);
    QVERIFY(empty.bars.isEmpty());
}

void TstGatewayHistory::rejects_malformed_replies() {
    QVERIFY(!parse_reply("", k15).ok);
    QCOMPARE(parse_reply("ERR timeframe must be one of ...\n", k15).error, QString("ERR timeframe must be one of ..."));
    QVERIFY(!parse_reply("BARS two 5\nEND\n", k15).ok);
    QVERIFY(!parse_reply("BARS 2 5\n" + bar("AAPL", kT) + "\nEND\n", k15).ok); // one bar short
    QVERIFY(!parse_reply("BARS 1 5\n" + bar("AAPL", kT) + "\n", k15).ok);       // no END: cut off
    QVERIFY(!parse_reply("BARS 1 5\nAAPL 1793818800 x 101 98 100 1000\nEND\n", k15).ok);
    QVERIFY(!parse_reply("BARS 1 5\nAAPL 1793818800 99\nEND\n", k15).ok);
}

void TstGatewayHistory::accepts_covered_symbols_and_sends_the_rest_to_the_broker() {
    const qint64 now = (kT + 3000) * 1000; // 19:50
    const Reply r = parse_reply(reply(kT + 2940, {bar("AAPL", kT), bar("MSFT", kT)}), k15); // complete to 19:49
    const Accepted a = accept(r, {"AAPL", "MSFT", "NVDA"}, kT * 1000, now, k15);
    QCOMPARE(a.data.keys().size(), 2);
    QVERIFY(a.data.contains("AAPL") && a.data.contains("MSFT"));
    QCOMPARE(a.missing, QStringList{"NVDA"}); // no bars: not synced, or unknown
}

void TstGatewayHistory::a_stale_or_absent_sync_is_not_trusted() {
    const Reply r = parse_reply(reply(kT, {bar("AAPL", kT - 900)}), k15);
    // complete_to six minutes behind now: the sync is not keeping up.
    Accepted a = accept(r, {"AAPL"}, (kT - 86400) * 1000, (kT + 360) * 1000, k15);
    QVERIFY(a.data.isEmpty());
    QCOMPARE(a.missing, QStringList{"AAPL"});
    QVERIFY(a.note.contains("complete only to"));
    // complete_to 0: no sync at all, the stored bars may have gaps.
    a = accept(parse_reply(reply(0, {bar("AAPL", kT - 900)}), k15), {"AAPL"}, (kT - 86400) * 1000, kT * 1000, k15);
    QVERIFY(a.data.isEmpty());
    QVERIFY(a.note.contains("no bar sync"));
    // A failed request: everything to the broker, with the reason.
    a = accept(parse_reply("ERR boom\n", k15), {"AAPL", "MSFT"}, 0, kT * 1000, k15);
    QCOMPARE(a.missing, (QStringList{"AAPL", "MSFT"}));
    QCOMPARE(a.note, QString("ERR boom"));
}

void TstGatewayHistory::drops_periods_the_sync_has_not_finished() {
    // Complete to 19:44: the 19:30 period runs to 19:45, so its last minute may be missing.
    const Reply r = parse_reply(reply(kT + 2640, {bar("AAPL", kT), bar("AAPL", kT + 900), bar("AAPL", kT + 1800)}), k15);
    const Accepted a = accept(r, {"AAPL"}, kT * 1000, (kT + 2650) * 1000, k15);
    const auto bars = a.data.value("AAPL");
    QCOMPARE(bars.size(), 2);
    QCOMPARE(bars.last().open_time, (kT + 900) * 1000);
}

void TstGatewayHistory::a_symbol_with_too_little_history_goes_to_the_broker() {
    // Asked for 55 days; AAPL's stored history starts 10 days ago (synced recently,
    // or kept for fewer days than asked): the scan would warm on a short history.
    const qint64 now = kT * 1000;
    const qint64 from = now - 55LL * 86'400'000;
    const Reply r = parse_reply(reply(kT, {bar("AAPL", kT - 10 * 86400), bar("MSFT", kT - 54 * 86400)}), k15);
    const Accepted a = accept(r, {"AAPL", "MSFT"}, from, now, k15);
    QCOMPARE(a.missing, QStringList{"AAPL"});
    QVERIFY(a.data.contains("MSFT")); // within the weekend-and-holiday slack
    QVERIFY(a.note.contains("1 with too little history"));
}

QTEST_GUILESS_MAIN(TstGatewayHistory)
#include "tst_gateway_history.moc"
