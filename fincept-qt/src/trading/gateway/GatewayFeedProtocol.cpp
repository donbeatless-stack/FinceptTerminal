// src/trading/gateway/GatewayFeedProtocol.cpp
#include "trading/gateway/GatewayFeedProtocol.h"

#include <QHash>

#include <algorithm>
#include <bit>
#include <cstring>

namespace fincept::trading::gateway_feed {

namespace {

void put_u16(QByteArray& out, quint16 v) {
    out.append(static_cast<char>(v & 0xFF));
    out.append(static_cast<char>(v >> 8));
}

void put_u32(QByteArray& out, quint32 v) {
    for (int i = 0; i < 4; ++i)
        out.append(static_cast<char>((v >> (8 * i)) & 0xFF));
}

void put_header(QByteArray& out, MsgType type, quint32 body_len, quint16 flags = 0) {
    put_u32(out, body_len);
    put_u16(out, static_cast<quint16>(type));
    put_u16(out, flags);
}

/// Bounds-checked reader over an untrusted body; a failed read poisons it.
class Cursor {
  public:
    explicit Cursor(const QByteArray& b) : b_(b) {}
    bool ok() const { return ok_; }
    bool at_end() const { return ok_ && pos_ == b_.size(); }
    quint64 uint(int bytes) {
        if (!take(bytes))
            return 0;
        quint64 v = 0;
        for (int i = 0; i < bytes; ++i)
            v |= static_cast<quint64>(static_cast<quint8>(b_[pos_ - bytes + i])) << (8 * i);
        return v;
    }
    double f64() { return std::bit_cast<double>(uint(8)); }
    qint64 i64() { return static_cast<qint64>(uint(8)); }
    QString latin1(int n) {
        if (!take(n))
            return {};
        return QString::fromLatin1(b_.constData() + pos_ - n, n);
    }

  private:
    bool take(qsizetype n) {
        if (!ok_ || b_.size() - pos_ < n) {
            ok_ = false;
            return false;
        }
        pos_ += n;
        return true;
    }
    const QByteArray& b_;
    qsizetype pos_ = 0;
    bool ok_ = true;
};

/// "key=value" words after the first two.
QHash<QString, QString> key_values(const QStringList& words) {
    QHash<QString, QString> kv;
    for (const QString& w : words) {
        const qsizetype eq = w.indexOf(QLatin1Char('='));
        if (eq > 0)
            kv.insert(w.left(eq), w.mid(eq + 1));
    }
    return kv;
}

bool parse_host_port(const QString& text, QString& host, quint16& port) {
    const qsizetype colon = text.lastIndexOf(QLatin1Char(':'));
    if (colon < 0) {
        host = text;
        return !host.isEmpty();
    }
    bool ok = false;
    const uint p = text.mid(colon + 1).toUInt(&ok);
    if (!ok || p == 0 || p > 65535)
        return false;
    if (colon > 0)
        host = text.left(colon);
    port = static_cast<quint16>(p);
    return !host.isEmpty();
}

} // namespace

bool valid_symbol(const QString& s) {
    if (s.isEmpty() || s.size() > kMaxSymbolLen)
        return false;
    for (const QChar c : s)
        if (c.unicode() <= 0x20 || c.unicode() >= 0x7f)
            return false;
    return true;
}

QByteArray encode_symbols(MsgType type, const QStringList& symbols) {
    if (type != MsgType::Subscribe && type != MsgType::Unsubscribe)
        return {};
    QStringList ok;
    for (const QString& s : symbols)
        if (valid_symbol(s))
            ok.append(s);
    QByteArray out;
    for (qsizetype start = 0; start < ok.size(); start += kMaxSymbolsPerRequest) {
        const qsizetype n = std::min<qsizetype>(kMaxSymbolsPerRequest, ok.size() - start);
        quint32 body = 2;
        for (qsizetype i = start; i < start + n; ++i)
            body += 1 + static_cast<quint32>(ok[i].size());
        put_header(out, type, body);
        put_u16(out, static_cast<quint16>(n));
        for (qsizetype i = start; i < start + n; ++i) {
            out.append(static_cast<char>(ok[i].size()));
            out.append(ok[i].toLatin1());
        }
    }
    return out;
}

QByteArray encode_heartbeat() {
    QByteArray out;
    put_header(out, MsgType::Heartbeat, 0);
    return out;
}

void FrameReader::feed(const QByteArray& bytes) {
    // Drop consumed bytes once they are most of the buffer: no unbounded growth,
    // no shift on every call.
    if (pos_ > 0 && pos_ >= buf_.size() / 2) {
        buf_.remove(0, pos_);
        pos_ = 0;
    }
    buf_.append(bytes);
}

FrameReader::Result FrameReader::next(Frame& out) {
    if (buffered() < kHeaderSize)
        return Result::NeedMore;
    const auto* p = reinterpret_cast<const quint8*>(buf_.constData() + pos_);
    const quint32 len = quint32(p[0]) | (quint32(p[1]) << 8) | (quint32(p[2]) << 16) | (quint32(p[3]) << 24);
    const quint16 type = static_cast<quint16>(p[4] | (p[5] << 8));
    if (len > kMaxBody || type < static_cast<quint16>(MsgType::Subscribe) || type > static_cast<quint16>(MsgType::Error))
        return Result::Bad;
    if (buffered() < kHeaderSize + static_cast<qsizetype>(len))
        return Result::NeedMore;
    out.type = static_cast<MsgType>(type);
    out.flags = static_cast<quint16>(p[6] | (p[7] << 8));
    out.body = buf_.mid(pos_ + kHeaderSize, len);
    pos_ += kHeaderSize + len;
    return Result::Frame;
}

void FrameReader::clear() {
    buf_.clear();
    pos_ = 0;
}

std::optional<QVector<QPair<quint32, QString>>> parse_subscribed(const QByteArray& body) {
    Cursor c(body);
    const auto n = static_cast<int>(c.uint(2));
    if (!c.ok())
        return std::nullopt;
    QVector<QPair<quint32, QString>> out;
    out.reserve(std::min(n, kMaxSymbolsPerRequest));
    for (int i = 0; i < n; ++i) {
        const auto id = static_cast<quint32>(c.uint(4));
        const auto len = static_cast<int>(c.uint(1));
        QString name = c.latin1(len);
        if (!c.ok())
            return std::nullopt;
        out.append({id, name});
    }
    if (!c.at_end())
        return std::nullopt;
    return out;
}

std::optional<TickMsg> parse_tick(const QByteArray& body) {
    if (body.size() != kTickBodySize)
        return std::nullopt;
    Cursor c(body);
    TickMsg t;
    t.id = static_cast<quint32>(c.uint(4));
    t.sequence = c.uint(8);
    t.price = c.f64();
    t.size = c.f64();
    t.exchange_ts_ns = c.i64();
    t.ingest_ts_ns = c.i64();
    t.send_ts_ns = c.i64();
    if (!c.at_end())
        return std::nullopt;
    return t;
}

std::optional<ErrorMsg> parse_error(const QByteArray& body) {
    Cursor c(body);
    ErrorMsg m;
    m.code = static_cast<quint16>(c.uint(2));
    const auto len = static_cast<int>(c.uint(1));
    m.message = c.latin1(len);
    if (!c.at_end())
        return std::nullopt;
    return m;
}

FeedStatus parse_feed_status(const QByteArray& reply) {
    FeedStatus st;
    const QString line = QString::fromLatin1(reply).section(QLatin1Char('\n'), 0, 0).trimmed();
    const QStringList words = line.split(QLatin1Char(' '), Qt::SkipEmptyParts);
    if (words.isEmpty() || words[0] != QLatin1String("FEED")) {
        st.reason = line.isEmpty() ? QStringLiteral("no reply to FEED") : QStringLiteral("gateway answered: ") + line;
        return st;
    }
    const QString kind = words.value(1);
    if (kind != QLatin1String("alpaca")) {
        st.reason = QStringLiteral("gateway has no live Alpaca feed (FEED %1)").arg(kind.isEmpty() ? "?" : kind);
        return st;
    }
    const auto kv = key_values(words.mid(2));
    st.stream = kv.value(QStringLiteral("stream"));
    st.state = kv.value(QStringLiteral("state"));
    st.on_demand = kv.value(QStringLiteral("on_demand")) == QLatin1String("1");
    if (st.stream != QLatin1String("iex") && st.stream != QLatin1String("sip")) {
        // test: made-up symbols on a separate endpoint; delayed_sip: 15 minutes old.
        st.reason = QStringLiteral("gateway streams Alpaca's '%1' feed, not live iex/sip").arg(st.stream);
        return st;
    }
    if (st.state.isEmpty() || st.state == QLatin1String("stopped")) {
        st.reason = QStringLiteral("gateway's Alpaca feed has stopped");
        return st;
    }
    st.usable = true;
    return st;
}

bool merge_trade(BrokerQuote& q, const Trade& t) {
    if (t.snapshot && q.ltp > 0 && q.timestamp >= t.exchange_ts_ms)
        return false;
    q.symbol = t.symbol;
    q.ltp = t.price;
    q.timestamp = t.exchange_ts_ms;
    q.bid = q.ask = q.bid_size = q.ask_size = 0;
    return true;
}

Endpoints endpoints(const QString& gateway_env, const QString& feed_env) {
    Endpoints e;
    const QString g = gateway_env.trimmed();
    const QString f = feed_env.trimmed();
    if (g.compare(QLatin1String("off"), Qt::CaseInsensitive) == 0) {
        e.reason = QStringLiteral("FINCEPT_GATEWAY=off");
        return e;
    }
    if (f.compare(QLatin1String("off"), Qt::CaseInsensitive) == 0) {
        e.reason = QStringLiteral("FINCEPT_GATEWAY_FEED=off");
        return e;
    }
    e.host = QStringLiteral("127.0.0.1");
    if (!g.isEmpty() && !parse_host_port(g, e.host, e.request_port)) {
        e.reason = QStringLiteral("FINCEPT_GATEWAY is not host:port: ") + g;
        return e;
    }
    e.push_host = e.host;
    if (!f.isEmpty() && !parse_host_port(f, e.push_host, e.push_port)) {
        e.reason = QStringLiteral("FINCEPT_GATEWAY_FEED is not host:port: ") + f;
        return e;
    }
    e.enabled = true;
    return e;
}

} // namespace fincept::trading::gateway_feed
