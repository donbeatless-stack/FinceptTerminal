// src/algo_engine/GatewayHistory.cpp
#include "algo_engine/GatewayHistory.h"

#include <QDateTime>
#include <QList>

namespace fincept::algo::gateway_history {

QString gateway_timeframe(const QString& tf) {
    static const QStringList kServed = {"1m", "3m", "5m", "10m", "15m", "30m", "1h", "4h"};
    return kServed.contains(tf) ? tf : QString();
}

qint64 window_start_ms(qint64 now_ms, int lookback_days) {
    constexpr qint64 kDay = 86'400'000;
    const qint64 t = now_ms - static_cast<qint64>(lookback_days) * kDay;
    return t - (t % kDay + kDay) % kDay; // floor to UTC midnight, negatives included
}

QByteArray request_line(const QString& gateway_tf, qint64 from_s, qint64 to_s, const QStringList& symbols) {
    QByteArray line = "HISTORY " + gateway_tf.toLatin1() + ' ' + QByteArray::number(from_s) + ' ' +
                      QByteArray::number(to_s);
    for (const QString& s : symbols)
        line += ' ' + s.toLatin1();
    line += '\n';
    return line;
}

Reply parse_reply(const QByteArray& text, qint64 period_ms) {
    Reply r;
    const QList<QByteArray> lines = text.split('\n');
    if (lines.isEmpty() || lines.first().isEmpty()) {
        r.error = QStringLiteral("empty reply");
        return r;
    }
    const QByteArray head = lines.first().trimmed();
    if (head.startsWith("ERR")) {
        r.error = QString::fromUtf8(head);
        return r;
    }
    const QList<QByteArray> h = head.split(' ');
    bool n_ok = false, c_ok = false;
    const qsizetype n = h.size() == 3 && h[0] == "BARS" ? h[1].toLongLong(&n_ok) : -1;
    r.complete_to_s = h.size() == 3 ? h[2].toLongLong(&c_ok) : 0;
    if (!n_ok || !c_ok || n < 0) {
        r.error = QStringLiteral("bad header: ") + QString::fromUtf8(head.left(60));
        return r;
    }
    if (lines.size() < n + 2 || lines[n + 1].trimmed() != "END") {
        r.error = QStringLiteral("truncated reply");
        return r;
    }
    for (qsizetype i = 1; i <= n; ++i) {
        const QList<QByteArray> f = lines[i].trimmed().split(' ');
        bool ok[6] = {};
        if (f.size() != 7) {
            r.error = QStringLiteral("bad bar line %1").arg(i);
            return r;
        }
        OhlcvCandle c;
        c.open_time = f[1].toLongLong(&ok[0]) * 1000;
        c.close_time = c.open_time + period_ms;
        c.open = f[2].toDouble(&ok[1]);
        c.high = f[3].toDouble(&ok[2]);
        c.low = f[4].toDouble(&ok[3]);
        c.close = f[5].toDouble(&ok[4]);
        c.volume = f[6].toDouble(&ok[5]);
        c.is_closed = true;
        for (bool b : ok)
            if (!b) {
                r.error = QStringLiteral("bad number in bar line %1").arg(i);
                return r;
            }
        r.bars[QString::fromLatin1(f[0])].append(c);
    }
    r.ok = true;
    return r;
}

Accepted accept(const Reply& reply, const QStringList& requested, qint64 from_ms, qint64 now_ms, qint64 period_ms,
                qint64 max_staleness_ms, qint64 coverage_slack_ms) {
    Accepted a;
    if (!reply.ok) {
        a.missing = requested;
        a.note = reply.error;
        return a;
    }
    const qint64 complete_ms = reply.complete_to_s * 1000;
    if (reply.complete_to_s == 0 || now_ms - complete_ms > max_staleness_ms) {
        a.missing = requested;
        a.note = reply.complete_to_s == 0
                     ? QStringLiteral("the gateway runs no bar sync")
                     : QStringLiteral("the gateway's bars are complete only to %1")
                           .arg(QDateTime::fromMSecsSinceEpoch(complete_ms).toString("yyyy-MM-dd HH:mm"));
        return a;
    }
    int shallow = 0;
    for (const QString& sym : requested) {
        QVector<OhlcvCandle> bars = reply.bars.value(sym);
        // A period ending after complete_to may lack minutes the sync has not stored yet.
        while (!bars.isEmpty() && bars.constLast().open_time + period_ms > complete_ms)
            bars.removeLast();
        if (bars.isEmpty() || bars.constFirst().open_time > from_ms + coverage_slack_ms) {
            if (!bars.isEmpty())
                ++shallow;
            a.missing.append(sym);
            continue;
        }
        a.data.insert(sym, bars);
    }
    if (!a.missing.isEmpty())
        a.note = QStringLiteral("%1 symbol(s) not covered by the gateway (%2 with too little history)")
                     .arg(a.missing.size())
                     .arg(shallow);
    return a;
}

} // namespace fincept::algo::gateway_history
