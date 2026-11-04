// Unit tests for src/trading/brokers/alpaca/AlpacaBarsPaging.h
//
// AlpacaBroker::get_history once sent a single limit=500&sort=asc request and got
// the OLDEST 500 bars of the range; realtime scans then warmed on weeks-old
// history. The fix follows next_page_token. Live, every range the app asks for
// fits one 10,000-bar page, so the paging path itself never ran against the real
// API — these tests drive it with scripted responses instead: page order and
// concatenation, token encoding, all-or-nothing failure, the page cap. Plus the
// time-parameter normalisation the same fix introduced.
//
// Header-only code under test — Qt Core + Test.

#include "trading/brokers/alpaca/AlpacaBarsPaging.h"

#include <QDateTime>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QString>
#include <QStringList>
#include <QTest>
#include <QTimeZone>

#include <functional>

using fincept::trading::alpaca::BarsPage;
using fincept::trading::alpaca::BarsResult;
using fincept::trading::alpaca::bars_time_param;
using fincept::trading::alpaca::fetch_all_bar_pages;

namespace {

const QString kBase = QStringLiteral("https://data.example/v2/stocks/AAPL/bars?timeframe=15Min&limit=3");

// One response body: `n` 15-minute bars from `first` (minutes after 13:30Z), and
// the token for the next page (null when empty, as Alpaca sends it).
QString page_body(int first, int n, const QString& next_token) {
    QJsonArray bars;
    for (int i = 0; i < n; ++i) {
        const int k = first + i;
        const QDateTime t = QDateTime(QDate(2026, 11, 3), QTime(13, 30), QTimeZone::UTC).addSecs(k * 900);
        bars.append(QJsonObject{{"t", t.toString(Qt::ISODate)},
                                {"o", 100.0 + k},
                                {"h", 101.0 + k},
                                {"l", 99.0 + k},
                                {"c", 100.5 + k},
                                {"v", 1000 + k}});
    }
    QJsonObject o{{"bars", bars}, {"symbol", "AAPL"}};
    o.insert("next_page_token", next_token.isEmpty() ? QJsonValue(QJsonValue::Null) : QJsonValue(next_token));
    return QString::fromUtf8(QJsonDocument(o).toJson(QJsonDocument::Compact));
}

// Serves scripted pages in order and records every URL requested.
struct Script {
    QVector<BarsPage> pages;
    QStringList urls;
    BarsPage operator()(const QString& url) {
        urls << url;
        return urls.size() <= pages.size() ? pages[urls.size() - 1] : BarsPage{false, {}, "unexpected request"};
    }
};

BarsPage ok(const QString& body) {
    return {true, body, {}};
}

} // namespace

class TstAlpacaBarsPaging : public QObject {
    Q_OBJECT

  private slots:
    void single_page_is_one_request();
    void pages_are_followed_and_concatenated_in_order();
    void page_token_is_percent_encoded();
    void a_failed_page_fails_the_whole_call();
    void an_unparsable_page_fails_the_whole_call();
    void paging_stops_at_the_cap();
    void empty_range_is_ok();
    void bars_carry_epoch_milliseconds();

    void time_param_bare_dates();
    void time_param_end_is_capped_at_now();
    void time_param_local_minutes_and_iso();
    void time_param_empty_and_unknown();
};

void TstAlpacaBarsPaging::single_page_is_one_request() {
    Script s{{ok(page_body(0, 3, {}))}, {}};
    const BarsResult r = fetch_all_bar_pages(kBase, std::ref(s), 50);
    QVERIFY(r.ok);
    QCOMPARE(int(r.candles.size()), 3);
    QCOMPARE(r.pages, 1);
    QCOMPARE(s.urls, QStringList{kBase});
}

void TstAlpacaBarsPaging::pages_are_followed_and_concatenated_in_order() {
    Script s{{ok(page_body(0, 3, "p2")), ok(page_body(3, 3, "p3")), ok(page_body(6, 2, {}))}, {}};
    const BarsResult r = fetch_all_bar_pages(kBase, std::ref(s), 50);
    QVERIFY2(r.ok, qPrintable(r.error));
    QCOMPARE(r.pages, 3);
    QCOMPARE(s.urls,
             (QStringList{kBase, kBase + QStringLiteral("&page_token=p2"), kBase + QStringLiteral("&page_token=p3")}));
    // Oldest first, no gaps, no repeats: bar k opens at 100 + k.
    QCOMPARE(int(r.candles.size()), 8);
    for (int k = 0; k < r.candles.size(); ++k)
        QCOMPARE(r.candles[k].open, 100.0 + k);
}

void TstAlpacaBarsPaging::page_token_is_percent_encoded() {
    // Tokens are opaque base64-like strings; '+', '/' and '=' must survive the query.
    const QString token = QStringLiteral("QUFQTHxNfDE3NjI=+/x");
    Script s{{ok(page_body(0, 3, token)), ok(page_body(3, 1, {}))}, {}};
    QVERIFY(fetch_all_bar_pages(kBase, std::ref(s), 50).ok);
    QCOMPARE(int(s.urls.size()), 2);
    QCOMPARE(s.urls[1], kBase + QStringLiteral("&page_token=QUFQTHxNfDE3NjI%3D%2B%2Fx"));
}

void TstAlpacaBarsPaging::a_failed_page_fails_the_whole_call() {
    // Returning pages 1-2 would hand the caller the oldest part of the range as if
    // it were all of it: the bug this code replaced.
    Script s{{ok(page_body(0, 3, "p2")), ok(page_body(3, 3, "p3")), {false, {}, "HTTP 429: rate limited"}}, {}};
    const BarsResult r = fetch_all_bar_pages(kBase, std::ref(s), 50);
    QVERIFY(!r.ok);
    QVERIFY(r.candles.isEmpty());
    QCOMPARE(r.error, QStringLiteral("HTTP 429: rate limited"));
    QCOMPARE(r.pages, 3);
}

void TstAlpacaBarsPaging::an_unparsable_page_fails_the_whole_call() {
    for (const QString& bad : {QStringLiteral("<html>gateway timeout</html>"), QStringLiteral("[1,2,3]")}) {
        Script s{{ok(page_body(0, 3, "p2")), ok(bad)}, {}};
        const BarsResult r = fetch_all_bar_pages(kBase, std::ref(s), 50);
        QVERIFY(!r.ok);
        QVERIFY(r.candles.isEmpty());
        QVERIFY2(r.error.startsWith(QStringLiteral("JSON parse error on page 2")), qPrintable(r.error));
    }
}

void TstAlpacaBarsPaging::paging_stops_at_the_cap() {
    // A server that never stops handing out tokens must not loop forever.
    int requests = 0;
    const BarsResult r = fetch_all_bar_pages(
        kBase,
        [&](const QString&) {
            ++requests;
            return ok(page_body(3 * requests, 3, QStringLiteral("again")));
        },
        5);
    QVERIFY(!r.ok);
    QCOMPARE(requests, 5);
    QVERIFY(r.candles.isEmpty());
    QVERIFY2(r.error.contains(QStringLiteral("after 5 pages (15 bars)")), qPrintable(r.error));
}

void TstAlpacaBarsPaging::empty_range_is_ok() {
    // A weekend range: "bars": null, no token. Not an error, just nothing.
    Script s{{ok(QStringLiteral(R"({"bars":null,"symbol":"AAPL","next_page_token":null})"))}, {}};
    const BarsResult r = fetch_all_bar_pages(kBase, std::ref(s), 50);
    QVERIFY(r.ok);
    QVERIFY(r.candles.isEmpty());
}

void TstAlpacaBarsPaging::bars_carry_epoch_milliseconds() {
    Script s{{ok(page_body(0, 2, {}))}, {}};
    const BarsResult r = fetch_all_bar_pages(kBase, std::ref(s), 50);
    QVERIFY(r.ok);
    const qint64 first = QDateTime(QDate(2026, 11, 3), QTime(13, 30), QTimeZone::UTC).toMSecsSinceEpoch();
    QCOMPARE(r.candles[0].timestamp, first);
    QCOMPARE(r.candles[1].timestamp - r.candles[0].timestamp, qint64(15 * 60 * 1000));
    QCOMPARE(r.candles[1].volume, 1001.0);
}

// ── time parameters ──────────────────────────────────────────────────────────

namespace {
const QDateTime kNow = QDateTime(QDate(2026, 11, 4), QTime(19, 15, 2), QTimeZone::UTC);
}

void TstAlpacaBarsPaging::time_param_bare_dates() {
    QCOMPARE(bars_time_param("2026-09-10", false, kNow), QStringLiteral("2026-09-10T00:00:00Z"));
    // An end date means "through that day": the next midnight.
    QCOMPARE(bars_time_param("2026-11-03", true, kNow), QStringLiteral("2026-11-04T00:00:00Z"));
}

void TstAlpacaBarsPaging::time_param_end_is_capped_at_now() {
    // Today's end date would be tomorrow's midnight, in the future: capped at now
    // (and read literally it was today's midnight, which dropped today's bars).
    QCOMPARE(bars_time_param("2026-11-04", true, kNow), QStringLiteral("2026-11-04T19:15:02Z"));
    QCOMPARE(bars_time_param("2026-12-01T00:00:00Z", true, kNow), QStringLiteral("2026-11-04T19:15:02Z"));
    // A start is never capped.
    QCOMPARE(bars_time_param("2026-12-01", false, kNow), QStringLiteral("2026-12-01T00:00:00Z"));
}

void TstAlpacaBarsPaging::time_param_local_minutes_and_iso() {
    // Historify's "yyyy-MM-dd HH:mm" is local time; Alpaca rejected it verbatim.
    const QDateTime local = QDateTime(QDate(2026, 11, 3), QTime(9, 30));
    QCOMPARE(bars_time_param("2026-11-03 09:30", false, kNow), local.toUTC().toString(Qt::ISODate));
    // ISO with an offset becomes UTC.
    QCOMPARE(bars_time_param("2026-11-03T09:30:00-05:00", false, kNow), QStringLiteral("2026-11-03T14:30:00Z"));
    QCOMPARE(bars_time_param(" 2026-11-03T14:30:00Z ", false, kNow), QStringLiteral("2026-11-03T14:30:00Z"));
}

void TstAlpacaBarsPaging::time_param_empty_and_unknown() {
    QCOMPARE(bars_time_param("", false, kNow), QString());
    QCOMPARE(bars_time_param("  ", true, kNow), QStringLiteral("2026-11-04T19:15:02Z"));
    // Unknown formats pass through for Alpaca to reject with its own message.
    QCOMPARE(bars_time_param("yesterday", false, kNow), QStringLiteral("yesterday"));
}

QTEST_GUILESS_MAIN(TstAlpacaBarsPaging)
#include "tst_alpaca_bars_paging.moc"
