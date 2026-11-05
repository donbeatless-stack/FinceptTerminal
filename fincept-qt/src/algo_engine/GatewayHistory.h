// src/algo_engine/GatewayHistory.h
#pragma once
// The terminal's side of the market-data gateway's HISTORY command
// (gateway/include/fmdg/history.h): the request line, the reply parser, and the
// rule for which symbols the gateway's answer may stand in for the broker's.
// Pure functions (Qt Core only), unit-tested in tests/tst_gateway_history.cpp;
// CandleDataFetcher does the socket work.
//
// The gateway answers from one-minute bars it keeps complete through a sync
// with Alpaca's REST API, and says up to when ("complete_to"). Trust follows
// from that, never from the bars alone:
//   - complete_to more than `max_staleness` behind now: the sync is not running
//     or is behind, so nothing is accepted;
//   - a bar whose period ends after complete_to may be missing minutes: dropped
//     (the realtime scan rebuilds the current period from live ticks anyway);
//   - a symbol with no bars, or whose first bar starts well after the requested
//     start (the gateway keeps less history than asked for), is left to the broker.

#include "algo_engine/AlgoEngineTypes.h"

#include <QByteArray>
#include <QHash>
#include <QString>
#include <QStringList>
#include <QVector>

namespace fincept::algo::gateway_history {

/// The gateway's name for `timeframe` ("15m", "1h", ...), or empty when it
/// cannot serve it: daily bars need the exchange calendar, not UTC midnights.
QString gateway_timeframe(const QString& timeframe);

/// Where a lookback of `lookback_days` starts: UTC midnight of that day, as the
/// broker path asks (a bare start date), so both paths cover the same bars.
qint64 window_start_ms(qint64 now_ms, int lookback_days);

/// Symbols per request: keeps the request line well under the gateway's 4 KiB.
inline constexpr int kSymbolsPerRequest = 200;

/// "HISTORY <tf> <from> <to> <symbols>\n".
QByteArray request_line(const QString& gateway_tf, qint64 from_s, qint64 to_s, const QStringList& symbols);

struct Reply {
    bool ok = false;
    QString error;
    qint64 complete_to_s = 0;
    QHash<QString, QVector<OhlcvCandle>> bars; ///< oldest first, as the gateway sends them
};

/// Parses "BARS <n> <complete_to>", n bar lines, "END" (or "ERR ...").
Reply parse_reply(const QByteArray& reply, qint64 period_ms);

struct Accepted {
    QHash<QString, QVector<OhlcvCandle>> data; ///< what the gateway covers
    QStringList missing;                        ///< to be fetched from the broker
    QString note;                               ///< why anything went to `missing`
};

inline constexpr qint64 kMaxStalenessMs = 5LL * 60 * 1000;
inline constexpr qint64 kCoverageSlackMs = 4LL * 86400 * 1000; ///< a weekend plus a holiday

Accepted accept(const Reply& reply, const QStringList& requested, qint64 from_ms, qint64 now_ms, qint64 period_ms,
                qint64 max_staleness_ms = kMaxStalenessMs, qint64 coverage_slack_ms = kCoverageSlackMs);

} // namespace fincept::algo::gateway_history
