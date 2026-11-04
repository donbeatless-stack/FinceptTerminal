// src/trading/brokers/alpaca/AlpacaBarsPaging.h
#pragma once
// The network-free half of AlpacaBroker::get_history: time-parameter normalisation,
// bar parsing and the next_page_token loop. The HTTP call is passed in, so the
// paging rules are unit-tested (tst_alpaca_bars_paging) without a live account;
// with real data every range a scan asks for fits one 10,000-bar page, so the
// multi-page path never runs live.
#include "trading/TradingTypes.h" // BrokerCandle

#include <QDate>
#include <QDateTime>
#include <QJsonArray>
#include <QJsonDocument>
#include <QJsonObject>
#include <QString>
#include <QTime>
#include <QTimeZone>
#include <QUrl>
#include <QVector>

namespace fincept::trading::alpaca {

/// The bars endpoint wants RFC-3339 times; a bare date is read as midnight UTC.
/// Callers pass "yyyy-MM-dd", local "yyyy-MM-dd HH:mm" (Historify — not RFC-3339,
/// so Alpaca rejected it) or ISO. Normalise all of them to RFC-3339 UTC. A bare END
/// date means "through that day": read literally it cut off the whole day — today's
/// bars included — so it becomes the next midnight, capped at `now_utc`. Empty →
/// empty for a start, now for an end. An unknown format passes through for Alpaca
/// to report.
inline QString bars_time_param(const QString& value, bool is_end, const QDateTime& now_utc) {
    const QString v = value.trimmed();
    QDateTime t;
    if (v.isEmpty()) {
        if (!is_end)
            return {};
        t = now_utc;
    } else if (const QDate d = QDate::fromString(v, "yyyy-MM-dd"); d.isValid()) {
        t = QDateTime(is_end ? d.addDays(1) : d, QTime(0, 0), QTimeZone::UTC);
    } else {
        t = QDateTime::fromString(v, "yyyy-MM-dd HH:mm"); // local time
        if (!t.isValid())
            t = QDateTime::fromString(v, Qt::ISODateWithMs);
        if (!t.isValid())
            return v;
        t = t.toUTC();
    }
    if (is_end && t > now_utc)
        t = now_utc;
    return t.toUTC().toString(Qt::ISODate); // e.g. 2026-11-04T19:15:02Z
}

/// One bars-response "bars" array → candles. "t" is RFC-3339; BrokerCandle wants
/// epoch MILLISECONDS (seconds render the chart in January 1970).
inline QVector<BrokerCandle> parse_bars(const QJsonArray& bars) {
    QVector<BrokerCandle> out;
    out.reserve(bars.size());
    for (const auto& v : bars) {
        const QJsonObject b = v.toObject();
        BrokerCandle c;
        c.timestamp = QDateTime::fromString(b.value("t").toString(), Qt::ISODateWithMs).toMSecsSinceEpoch();
        c.open = b.value("o").toDouble();
        c.high = b.value("h").toDouble();
        c.low = b.value("l").toDouble();
        c.close = b.value("c").toDouble();
        c.volume = b.value("v").toDouble();
        out.append(c);
    }
    return out;
}

/// What one HTTP GET produced, as far as paging cares.
struct BarsPage {
    bool ok = false;
    QString body;  // response body when ok
    QString error; // caller-formatted message when not
};

struct BarsResult {
    bool ok = false;
    QVector<BrokerCandle> candles; // whole range, oldest first; empty unless ok
    QString error;
    int pages = 0; // requests made
};

/// Requests `base_url`, then `base_url&page_token=<token>` while the response
/// carries a next_page_token. `get(url) -> BarsPage` performs one request.
///
/// A single limit=500&sort=asc request used to return only the OLDEST 500 bars of
/// the range, and realtime scans warmed their indicators on weeks-old history. So
/// all or nothing: a failed or unparsable page, or still paging after `max_pages`,
/// fails the whole call — the pages already read are the oldest part of the range,
/// exactly the stale prefix this loop exists to avoid.
template <class Get>
BarsResult fetch_all_bar_pages(const QString& base_url, Get&& get, int max_pages) {
    BarsResult r;
    QVector<BrokerCandle> candles;
    QString token;
    for (int page = 0; page < max_pages; ++page) {
        QString url = base_url;
        if (!token.isEmpty())
            url += QStringLiteral("&page_token=") + QString::fromLatin1(QUrl::toPercentEncoding(token));
        const BarsPage resp = get(url);
        r.pages = page + 1;
        if (!resp.ok) {
            r.error = resp.error;
            return r;
        }
        QJsonParseError err;
        const QJsonDocument doc = QJsonDocument::fromJson(resp.body.toUtf8(), &err);
        if (err.error != QJsonParseError::NoError || !doc.isObject()) {
            r.error = QStringLiteral("JSON parse error on page %1: %2")
                          .arg(page + 1)
                          .arg(err.error != QJsonParseError::NoError ? err.errorString()
                                                                     : QStringLiteral("not an object"));
            return r;
        }
        const QJsonObject obj = doc.object();
        candles += parse_bars(obj.value("bars").toArray()); // "bars" is null on an empty range
        token = obj.value("next_page_token").toString();    // null on the last page
        if (token.isEmpty()) {
            r.ok = true;
            r.candles = std::move(candles);
            return r;
        }
    }
    r.error = QStringLiteral("history range too large: still paging after %1 pages (%2 bars)")
                  .arg(max_pages)
                  .arg(candles.size());
    return r;
}

} // namespace fincept::trading::alpaca
