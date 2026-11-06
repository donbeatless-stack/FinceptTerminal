// src/trading/gateway/GatewayFeedProtocol.h
#pragma once
// The terminal's side of the market-data gateway's push port
// (gateway/include/fmdg/protocol.h) and of its FEED request line: the frame
// codec, the FEED reply, and where the gateway is. Pure functions (Qt Core
// only), unit-tested in tests/tst_gateway_feed.cpp; GatewayFeed does the
// socket work.
//
// Push port, every frame: u32 body length, u16 type, u16 flags, then the body;
// little-endian throughout. The client sends Subscribe / Unsubscribe
// (u16 n, n × (u8 len, symbol)) and Heartbeat (empty, echoed). The gateway sends
// Subscribed (u16 n, n × (u32 id, u8 len, symbol)) — the ids its Ticks carry —
// then each symbol's last value flagged as a snapshot, then every trade:
// Tick = u32 id, u64 sequence, f64 price, f64 size, i64 exchange ts (Unix ns),
// i64 ingest ts, i64 send ts. Error = u16 code, u8 len, text; the gateway then
// closes. Sequence numbers are gateway-wide and only grow within one connection;
// a tick can arrive twice (a snapshot racing a live tick), never older after newer.

#include "trading/TradingTypes.h"

#include <QByteArray>
#include <QMetaType>
#include <QPair>
#include <QString>
#include <QStringList>
#include <QVector>

#include <optional>

namespace fincept::trading::gateway_feed {

enum class MsgType : quint16 {
    Subscribe = 1,
    Unsubscribe = 2,
    Subscribed = 3,
    Tick = 4,
    Heartbeat = 5,
    Error = 6,
};

inline constexpr int kHeaderSize = 8;
inline constexpr quint32 kMaxBody = 64 * 1024;
inline constexpr int kMaxSymbolLen = 32;
inline constexpr int kMaxSymbolsPerRequest = 1024;
inline constexpr int kTickBodySize = 52;
inline constexpr quint16 kFlagSnapshot = 1;

/// 1..32 printable ASCII characters, no spaces: what the gateway accepts. It
/// closes the connection on anything else, so the client never sends one.
bool valid_symbol(const QString& s);

/// Subscribe or Unsubscribe frames for `symbols`, at most 1024 per frame, one
/// after another. Invalid symbols are left out; none left → empty.
QByteArray encode_symbols(MsgType type, const QStringList& symbols);
QByteArray encode_heartbeat();

struct Frame {
    MsgType type = MsgType::Heartbeat;
    quint16 flags = 0;
    QByteArray body;
};

/// Splits the byte stream into frames, whatever the chunking. Bad = a header
/// with an unknown type or an oversized body: the stream cannot be resynced, so
/// the connection must be dropped.
class FrameReader {
  public:
    enum class Result { Frame, NeedMore, Bad };
    void feed(const QByteArray& bytes);
    Result next(Frame& out);
    qsizetype buffered() const { return buf_.size() - pos_; }
    void clear();

  private:
    QByteArray buf_;
    qsizetype pos_ = 0;
};

struct TickMsg {
    quint32 id = 0;
    quint64 sequence = 0;
    double price = 0;
    double size = 0;
    qint64 exchange_ts_ns = 0; ///< the exchange's timestamp, Unix epoch ns
    qint64 ingest_ts_ns = 0;   ///< the gateway's steady clock
    qint64 send_ts_ns = 0;
};

struct ErrorMsg {
    quint16 code = 0;
    QString message;
};

/// nullopt on a malformed body (short, trailing bytes, wrong size).
std::optional<QVector<QPair<quint32, QString>>> parse_subscribed(const QByteArray& body);
std::optional<TickMsg> parse_tick(const QByteArray& body);
std::optional<ErrorMsg> parse_error(const QByteArray& body);

/// One trade as GatewayFeed hands it on: the symbol resolved from its id.
struct Trade {
    QString symbol;
    double price = 0;
    double size = 0;
    qint64 exchange_ts_ms = 0;
    quint64 sequence = 0;
    bool snapshot = false; ///< the symbol's last trade, sent on subscribe — possibly old
};

/// What the request port's FEED line says. `usable`: the gateway streams live
/// US-equity trades from Alpaca (iex or sip, not the test or delayed stream)
/// and has not given up (state != stopped) — so it holds, or is about to hold,
/// the account's one stream connection, and the terminal should take its
/// trades instead of opening its own. An older gateway answers "ERR unknown
/// command"; a history-only one "FEED none": both are not usable.
struct FeedStatus {
    bool usable = false;
    bool on_demand = false; ///< false: only the gateway's fixed symbols stream
    QString stream;
    QString state;
    QString reason; ///< why not usable
};
FeedStatus parse_feed_status(const QByteArray& reply);

/// Applies a gateway trade to the quote the terminal shows for its symbol.
/// False, and `q` untouched, for a snapshot no newer than what `q` shows (the
/// same last trade again after a reconnect). Otherwise sets the last price and
/// time and clears bid/ask: the gateway carries trades, not quotes, so a
/// bid/ask fetched earlier would only go stale — and the paper order matcher
/// prices on bid/ask whenever they are set.
bool merge_trade(BrokerQuote& q, const Trade& t);

/// Where the gateway is. FINCEPT_GATEWAY names the request port ("host:port",
/// default 127.0.0.1:8588; "off" disables the gateway altogether, history and
/// live feed). FINCEPT_GATEWAY_FEED names the push port ("host:port", default
/// the same host, 8589; "off" keeps the live feed on the broker while history
/// still comes from the gateway).
struct Endpoints {
    bool enabled = false;
    QString host;
    quint16 request_port = 8588;
    QString push_host;
    quint16 push_port = 8589;
    QString reason; ///< why disabled
};
Endpoints endpoints(const QString& gateway_env, const QString& feed_env);

} // namespace fincept::trading::gateway_feed

Q_DECLARE_METATYPE(fincept::trading::gateway_feed::Trade)
