// tests/tst_gateway_feed.cpp — the terminal's client of the market-data
// gateway's push port (src/trading/gateway/GatewayFeed*): the codec as pure
// functions, then GatewayFeed against a fake gateway on loopback (a request
// port answering FEED, a push port that acks subscriptions and sends scripted
// frames) — the first attempt's fallback cases, snapshot-then-live, subscription
// differences, duplicates, malformed frames, a link dropped mid-frame,
// resubscription after a reconnect, a dead link, and giving up.

#include "trading/gateway/GatewayFeed.h"
#include "trading/gateway/GatewayFeedProtocol.h"

#include <QElapsedTimer>
#include <QPointer>
#include <QSignalSpy>
#include <QTcpServer>
#include <QTcpSocket>
#include <QTest>

#include <bit>
#include <memory>

using namespace fincept::trading;
using namespace fincept::trading::gateway_feed;

namespace {

// ── wire helpers (the gateway's side of the protocol) ──────────────────────

void put_u16(QByteArray& out, quint16 v) {
    out.append(char(v & 0xFF));
    out.append(char(v >> 8));
}
void put_u32(QByteArray& out, quint32 v) {
    for (int i = 0; i < 4; ++i)
        out.append(char((v >> (8 * i)) & 0xFF));
}
void put_u64(QByteArray& out, quint64 v) {
    for (int i = 0; i < 8; ++i)
        out.append(char((v >> (8 * i)) & 0xFF));
}
QByteArray header(quint16 type, quint32 len, quint16 flags = 0) {
    QByteArray h;
    put_u32(h, len);
    put_u16(h, type);
    put_u16(h, flags);
    return h;
}
QByteArray tick_body(quint32 id, quint64 seq, double price, double size = 1, qint64 ts_ns = 0) {
    QByteArray b;
    put_u32(b, id);
    put_u64(b, seq);
    put_u64(b, std::bit_cast<quint64>(price));
    put_u64(b, std::bit_cast<quint64>(size));
    put_u64(b, quint64(ts_ns));
    put_u64(b, 0);
    put_u64(b, 0);
    return b;
}
QByteArray tick_frame(quint32 id, quint64 seq, double price, bool snapshot = false, qint64 ts_ns = 0) {
    const QByteArray body = tick_body(id, seq, price, 1, ts_ns);
    return header(4, quint32(body.size()), snapshot ? kFlagSnapshot : 0) + body;
}
QByteArray subscribed_frame(const QVector<QPair<quint32, QString>>& ids) {
    QByteArray b;
    put_u16(b, quint16(ids.size()));
    for (const auto& [id, name] : ids) {
        put_u32(b, id);
        b.append(char(name.size()));
        b.append(name.toLatin1());
    }
    return header(3, quint32(b.size())) + b;
}
QByteArray error_frame(quint16 code, const QByteArray& text) {
    QByteArray b;
    put_u16(b, code);
    b.append(char(text.size()));
    b.append(text);
    return header(6, quint32(b.size())) + b;
}

/// A gateway on loopback: FEED on one port, the push protocol on the other.
class FakeGateway : public QObject {
  public:
    QByteArray feed_reply = "FEED alpaca stream=iex state=streaming on_demand=1 upstream=0 cap=30\n";
    bool answer_feed = true;
    bool echo_heartbeats = true;
    bool ack_subscriptions = true;

    FakeGateway() {
        QVERIFY(request_.listen(QHostAddress::LocalHost, 0));
        QVERIFY(push_.listen(QHostAddress::LocalHost, 0));
        connect(&request_, &QTcpServer::newConnection, this, &FakeGateway::on_request);
        connect(&push_, &QTcpServer::newConnection, this, &FakeGateway::on_push);
    }

    Endpoints endpoints() const {
        Endpoints e;
        e.enabled = true;
        e.host = e.push_host = QStringLiteral("127.0.0.1");
        e.request_port = request_.serverPort();
        e.push_port = push_.serverPort();
        return e;
    }

    int push_connections() const { return push_connections_; }
    int feed_requests() const { return feed_requests_; }
    bool client_connected() const { return client_ && client_->state() == QAbstractSocket::ConnectedState; }

    /// Client → gateway frames on all connections, as "SUB A,B" / "UNSUB A" / "HB".
    QStringList log() const { return log_; }
    void clear_log() { log_.clear(); }
    quint32 id_of(const QString& sym) {
        if (!ids_.contains(sym))
            ids_.insert(sym, quint32(ids_.size() + 7)); // not 0-based: ids are opaque
        return ids_.value(sym);
    }

    void send(const QByteArray& bytes) {
        QVERIFY(client_);
        client_->write(bytes);
        client_->flush();
    }
    void drop_client() {
        if (client_)
            client_->abort();
    }
    /// Both ports stop accepting: the gateway is gone.
    void shut_down() {
        request_.close();
        push_.close();
        drop_client();
    }

  private:
    void on_request() {
        while (QTcpSocket* s = request_.nextPendingConnection()) {
            connect(s, &QTcpSocket::readyRead, s, [this, s] {
                if (!s->canReadLine())
                    return;
                ++feed_requests_;
                const QByteArray line = s->readLine().trimmed();
                if (!answer_feed)
                    return; // say nothing, keep the connection open
                s->write(line == "FEED" ? feed_reply : QByteArray("ERR unknown command\n"));
                s->disconnectFromHost();
            });
            connect(s, &QTcpSocket::disconnected, s, &QObject::deleteLater);
        }
    }

    void on_push() {
        while (QTcpSocket* s = push_.nextPendingConnection()) {
            ++push_connections_;
            client_ = s;
            auto reader = std::make_shared<FrameReader>();
            connect(s, &QTcpSocket::readyRead, s, [this, s, reader] {
                reader->feed(s->readAll());
                Frame f;
                while (reader->next(f) == FrameReader::Result::Frame) {
                    if (f.type == MsgType::Heartbeat) {
                        log_ << QStringLiteral("HB");
                        if (echo_heartbeats)
                            s->write(header(5, 0));
                        continue;
                    }
                    // Subscribe/Unsubscribe bodies are u16 n + n × (u8 len, symbol), like Subscribed without ids.
                    QStringList syms;
                    const QByteArray& b = f.body;
                    int pos = 2;
                    const int n = quint8(b[0]) | (quint8(b[1]) << 8);
                    for (int i = 0; i < n; ++i) {
                        const int len = quint8(b[pos]);
                        syms << QString::fromLatin1(b.mid(pos + 1, len));
                        pos += 1 + len;
                    }
                    const bool sub = f.type == MsgType::Subscribe;
                    log_ << (sub ? QStringLiteral("SUB ") : QStringLiteral("UNSUB ")) + syms.join(',');
                    if (sub && ack_subscriptions) {
                        QVector<QPair<quint32, QString>> acks;
                        for (const QString& sym : syms)
                            acks.append({id_of(sym), sym});
                        s->write(subscribed_frame(acks));
                    }
                }
            });
        }
    }

    QTcpServer request_;
    QTcpServer push_;
    QPointer<QTcpSocket> client_;
    QStringList log_;
    QHash<QString, quint32> ids_;
    int push_connections_ = 0;
    int feed_requests_ = 0;
};

GatewayFeed::Options fast() {
    GatewayFeed::Options o;
    o.connect_timeout_ms = 500;
    o.heartbeat_ms = 100;
    o.backoff_initial_ms = 20;
    o.backoff_max_ms = 80;
    o.give_up_after_ms = 1500;
    return o;
}

/// Everything a GatewayFeed emitted, in order.
struct Recorder {
    explicit Recorder(GatewayFeed& f)
        : trades(&f, &GatewayFeed::trade_received), connected(&f, &GatewayFeed::connected),
          disconnected(&f, &GatewayFeed::disconnected), unavailable(&f, &GatewayFeed::unavailable),
          errors(&f, &GatewayFeed::error_occurred) {}
    Trade trade(int i) const { return trades.at(i).at(0).value<Trade>(); }
    QSignalSpy trades, connected, disconnected, unavailable, errors;
};

} // namespace

class TstGatewayFeed : public QObject {
    Q_OBJECT
  private slots:
    void initTestCase() { qRegisterMetaType<Trade>(); }

    // ── codec ────────────────────────────────────────────────────────────────

    void subscribe_frames_match_the_gateway_layout() {
        const QByteArray f = encode_symbols(MsgType::Subscribe, {"AAPL", "X"});
        QByteArray want = header(1, 2 + 5 + 2);
        want += QByteArray::fromHex("0200") + "\x04" "AAPL" + "\x01" "X";
        QCOMPARE(f, want);
        QCOMPARE(encode_symbols(MsgType::Unsubscribe, {"SPY"}).mid(4, 2), QByteArray::fromHex("0200"));
        QCOMPARE(encode_heartbeat(), header(5, 0));
        // The gateway closes the connection on a bad symbol: such symbols never go out.
        QCOMPARE(encode_symbols(MsgType::Subscribe, {"", "A B", QString(33, 'A'), "é"}), QByteArray());
        QCOMPARE(encode_symbols(MsgType::Subscribe, {"", "SPY"}), encode_symbols(MsgType::Subscribe, {"SPY"}));
        QCOMPARE(encode_symbols(MsgType::Tick, {"SPY"}), QByteArray());
    }

    void more_than_1024_symbols_go_out_in_several_frames() {
        QStringList syms;
        for (int i = 0; i < 1500; ++i)
            syms << QStringLiteral("S%1").arg(i);
        FrameReader r;
        r.feed(encode_symbols(MsgType::Subscribe, syms));
        Frame f;
        QCOMPARE(r.next(f), FrameReader::Result::Frame);
        QCOMPARE(quint8(f.body[0]) | (quint8(f.body[1]) << 8), 1024);
        QCOMPARE(r.next(f), FrameReader::Result::Frame);
        QCOMPARE(quint8(f.body[0]) | (quint8(f.body[1]) << 8), 476);
        QCOMPARE(r.next(f), FrameReader::Result::NeedMore);
    }

    void frames_survive_any_split() {
        const QByteArray stream = subscribed_frame({{7, "AAPL"}, {8, "MSFT"}}) + tick_frame(7, 1, 150.25, true) +
                                  header(5, 0) + tick_frame(8, 2, 400.5);
        for (int step = 1; step <= stream.size(); ++step) {
            FrameReader r;
            QVector<MsgType> types;
            for (int i = 0; i < stream.size(); i += step) {
                r.feed(stream.mid(i, step));
                Frame f;
                while (r.next(f) == FrameReader::Result::Frame)
                    types << f.type;
            }
            QCOMPARE(types, (QVector<MsgType>{MsgType::Subscribed, MsgType::Tick, MsgType::Heartbeat, MsgType::Tick}));
            QCOMPARE(r.buffered(), 0);
        }
    }

    void bad_headers_are_reported_not_skipped() {
        for (const QByteArray& bad : {header(0, 0), header(7, 0), header(99, 4), header(4, kMaxBody + 1)}) {
            FrameReader r;
            r.feed(bad + tick_frame(1, 1, 1.0));
            Frame f;
            QCOMPARE(r.next(f), FrameReader::Result::Bad);
        }
        FrameReader r;
        r.feed(header(4, 52).left(5));
        Frame f;
        QCOMPARE(r.next(f), FrameReader::Result::NeedMore);
    }

    void bodies_are_parsed_strictly() {
        const auto t = parse_tick(tick_body(9, 42, 101.5, 3, 1793819702123456789LL));
        QVERIFY(t);
        QCOMPARE(t->id, 9u);
        QCOMPARE(t->sequence, 42u);
        QCOMPARE(t->price, 101.5);
        QCOMPARE(t->size, 3.0);
        QCOMPARE(t->exchange_ts_ns, 1793819702123456789LL);
        QVERIFY(!parse_tick(tick_body(9, 42, 1).left(51)));
        QVERIFY(!parse_tick(tick_body(9, 42, 1) + "x"));

        const QByteArray sub = subscribed_frame({{7, "AAPL"}, {8, "BRK.B"}}).mid(kHeaderSize);
        const auto acks = parse_subscribed(sub);
        QVERIFY(acks);
        QCOMPARE(acks->size(), 2);
        QCOMPARE(acks->at(1).first, 8u);
        QCOMPARE(acks->at(1).second, QStringLiteral("BRK.B"));
        QVERIFY(!parse_subscribed(sub.left(sub.size() - 1))); // a name cut short
        QVERIFY(!parse_subscribed(sub + "z"));                 // trailing bytes
        QVERIFY(!parse_subscribed(QByteArray("\x01", 1)));

        const auto e = parse_error(error_frame(2, "invalid symbol").mid(kHeaderSize));
        QVERIFY(e);
        QCOMPARE(e->code, 2);
        QCOMPARE(e->message, QStringLiteral("invalid symbol"));
        QVERIFY(!parse_error(QByteArray::fromHex("0200ff41")));
    }

    void feed_status_decides_whether_to_use_the_gateway() {
        auto st = parse_feed_status("FEED alpaca stream=iex state=streaming on_demand=1 upstream=6 cap=30\n");
        QVERIFY(st.usable);
        QVERIFY(st.on_demand);
        QCOMPARE(st.stream, QStringLiteral("iex"));
        // Still connecting or backing off (e.g. waiting out a 406): it will hold the stream.
        QVERIFY(parse_feed_status("FEED alpaca stream=sip state=backoff on_demand=0 upstream=2 cap=0\n").usable);
        QVERIFY(!parse_feed_status("FEED alpaca stream=sip state=backoff on_demand=0\n").on_demand);
        for (const char* no : {"FEED alpaca stream=test state=streaming on_demand=1 upstream=1 cap=0\n",
                               "FEED alpaca stream=delayed_sip state=streaming on_demand=1 upstream=1 cap=0\n",
                               "FEED alpaca stream=iex state=stopped on_demand=1 upstream=0 cap=30\n",
                               "FEED alpaca stream=iex\n", "FEED none\n", "FEED synthetic\n",
                               "ERR unknown command\n", "", "PONG\n"}) {
            st = parse_feed_status(no);
            QVERIFY2(!st.usable, no);
            QVERIFY2(!st.reason.isEmpty(), no);
        }
    }

    void endpoints_come_from_the_environment() {
        auto e = endpoints({}, {});
        QVERIFY(e.enabled);
        QCOMPARE(e.host, QStringLiteral("127.0.0.1"));
        QCOMPARE(e.request_port, 8588);
        QCOMPARE(e.push_host, QStringLiteral("127.0.0.1"));
        QCOMPARE(e.push_port, 8589);
        e = endpoints("10.0.0.5:9588", {});
        QCOMPARE(e.host, QStringLiteral("10.0.0.5"));
        QCOMPARE(e.request_port, 9588);
        QCOMPARE(e.push_host, QStringLiteral("10.0.0.5")); // the push port follows the host
        QCOMPARE(e.push_port, 8589);
        e = endpoints("gw:9588", ":9589");
        QCOMPARE(e.push_host, QStringLiteral("gw"));
        QCOMPARE(e.push_port, 9589);
        QVERIFY(!endpoints("off", {}).enabled);
        QVERIFY(!endpoints("OFF", "10.0.0.1:1").enabled);
        QVERIFY(!endpoints({}, "off").enabled);
        QVERIFY(!endpoints("gw:0", {}).enabled);
        QVERIFY(!endpoints("gw:99999", {}).enabled);
        QVERIFY(!endpoints({}, "gw:x").enabled);
    }

    void merging_a_trade_into_the_shown_quote() {
        BrokerQuote q;
        q.symbol = "AAPL";
        q.ltp = 150;
        q.timestamp = 2000;
        q.bid = 149.9;
        q.ask = 150.1;
        q.bid_size = q.ask_size = 100;
        q.volume = 1234; // left alone: not the gateway's to say
        Trade t{"AAPL", 151, 10, 1000, 5, true};
        QVERIFY(!merge_trade(q, t)); // an older snapshot changes nothing
        QCOMPARE(q.ltp, 150.0);
        QCOMPARE(q.bid, 149.9);
        t.exchange_ts_ms = 2000;
        QVERIFY(!merge_trade(q, t)); // the same last trade again
        t.snapshot = false;
        QVERIFY(merge_trade(q, t)); // a live trade always counts, even out of time order
        QCOMPARE(q.ltp, 151.0);
        QCOMPARE(q.bid, 0.0);
        QCOMPARE(q.ask, 0.0);
        QCOMPARE(q.ask_size, 0.0);
        QCOMPARE(q.volume, 1234.0);
        BrokerQuote fresh;
        t.snapshot = true;
        t.exchange_ts_ms = 1;
        QVERIFY(merge_trade(fresh, t)); // nothing shown yet: even an old snapshot is the best there is
        QCOMPARE(fresh.symbol, QStringLiteral("AAPL"));
        QCOMPARE(fresh.timestamp, 1);
    }

    // ── first attempt: no usable gateway means fall back at once ─────────────

    void nothing_listening_falls_back_at_once() {
        quint16 dead_port = 0;
        {
            QTcpServer s;
            QVERIFY(s.listen(QHostAddress::LocalHost, 0));
            dead_port = s.serverPort();
        }
        Endpoints e = endpoints({}, {});
        e.request_port = dead_port;
        e.push_port = dead_port;
        GatewayFeed feed(e, fast());
        Recorder r(feed);
        QElapsedTimer t;
        t.start();
        feed.open();
        QTRY_COMPARE_WITH_TIMEOUT(r.unavailable.count(), 1, 3000);
        const qint64 waited = t.elapsed();
        qInfo("no gateway: fell back after %lld ms", waited);
        // One attempt, bounded by connect_timeout_ms (500 here) — not the retry
        // budget (give_up_after_ms, 1500), which is only for a link that was up.
        QVERIFY2(waited < 1200, qPrintable(QString::number(waited)));
        QVERIFY(feed.is_unavailable());
        QCOMPARE(r.connected.count(), 0);
        QTest::qWait(200); // no retry, no second verdict
        QCOMPARE(r.unavailable.count(), 1);
    }

    void a_gateway_without_a_live_feed_is_not_used_data() {
        QTest::addColumn<QByteArray>("reply");
        QTest::addColumn<QString>("reason");
        QTest::newRow("history only") << QByteArray("FEED none\n") << "no live Alpaca feed";
        QTest::newRow("older gateway") << QByteArray("ERR unknown command\n") << "ERR unknown command";
        QTest::newRow("test stream") << QByteArray("FEED alpaca stream=test state=streaming on_demand=1\n")
                                     << "'test' feed";
        QTest::newRow("feed stopped") << QByteArray("FEED alpaca stream=iex state=stopped on_demand=1\n")
                                      << "stopped";
    }
    void a_gateway_without_a_live_feed_is_not_used() {
        QFETCH(QByteArray, reply);
        QFETCH(QString, reason);
        FakeGateway gw;
        gw.feed_reply = reply;
        GatewayFeed feed(gw.endpoints(), fast());
        Recorder r(feed);
        feed.open();
        QTRY_COMPARE(r.unavailable.count(), 1);
        QVERIFY2(r.unavailable.at(0).at(0).toString().contains(reason), qPrintable(r.unavailable.at(0).at(0).toString()));
        QCOMPARE(gw.push_connections(), 0);
        QCOMPARE(gw.feed_requests(), 1); // asked once, not retried
    }

    void a_silent_request_port_times_out() {
        FakeGateway gw;
        gw.answer_feed = false;
        GatewayFeed feed(gw.endpoints(), fast());
        Recorder r(feed);
        QElapsedTimer t;
        t.start();
        feed.open();
        QTRY_COMPARE_WITH_TIMEOUT(r.unavailable.count(), 1, 3000);
        QVERIFY(t.elapsed() >= 450);
        QVERIFY(r.unavailable.at(0).at(0).toString().contains("no FEED reply from"));
        QCOMPARE(gw.push_connections(), 0);
    }

    // ── a live gateway ───────────────────────────────────────────────────────

    void subscribes_then_gets_the_snapshot_then_live_trades() {
        FakeGateway gw;
        GatewayFeed feed(gw.endpoints(), fast());
        Recorder r(feed);
        feed.set_subscriptions({"AAPL", "MSFT", "AAPL", ""}); // deduped, the empty one dropped
        feed.open();
        QTRY_COMPARE(r.connected.count(), 1);
        QVERIFY(feed.is_connected());
        QTRY_VERIFY(gw.log().contains("SUB AAPL,MSFT"));
        QCOMPARE(gw.feed_requests(), 1);

        const qint64 ts = 1793819702123456789LL;
        gw.send(tick_frame(gw.id_of("AAPL"), 10, 150.25, true, ts) + tick_frame(gw.id_of("AAPL"), 11, 150.30, false, ts + 1'000'000));
        QTRY_COMPARE(r.trades.count(), 2);
        QCOMPARE(r.trade(0).symbol, QStringLiteral("AAPL"));
        QVERIFY(r.trade(0).snapshot);
        QCOMPARE(r.trade(0).price, 150.25);
        QCOMPARE(r.trade(0).exchange_ts_ms, ts / 1'000'000);
        QVERIFY(!r.trade(1).snapshot);
        QCOMPARE(r.trade(1).price, 150.30);
        QCOMPARE(feed.stats().snapshots, 1u);
    }

    void subscription_changes_go_out_as_differences() {
        FakeGateway gw;
        GatewayFeed feed(gw.endpoints(), fast());
        Recorder r(feed);
        feed.set_subscriptions({"AAPL", "MSFT"});
        feed.open();
        QTRY_VERIFY(gw.log().contains("SUB AAPL,MSFT"));
        gw.clear_log();
        feed.set_subscriptions({"MSFT", "TSLA"});
        QTRY_VERIFY(gw.log().contains("UNSUB AAPL"));
        QTRY_VERIFY(gw.log().contains("SUB TSLA"));
        feed.subscribe({"TSLA", "NVDA"});
        QTRY_VERIFY(gw.log().contains("SUB NVDA"));
        feed.unsubscribe({"NVDA", "ZZZ"});
        QTRY_VERIFY(gw.log().contains("UNSUB NVDA"));
        for (const QString& line : gw.log())
            QVERIFY2(!line.contains("MSFT"), qPrintable(line)); // never re-sent
        QCOMPARE(feed.subscriptions(), (QStringList{"MSFT", "TSLA"}));

        // A trade for AAPL still in flight after the unsubscribe is not passed on.
        gw.send(tick_frame(gw.id_of("AAPL"), 20, 150) + tick_frame(gw.id_of("TSLA"), 21, 300));
        QTRY_COMPARE(r.trades.count(), 1);
        QTest::qWait(50);
        QCOMPARE(r.trades.count(), 1);
        QCOMPARE(r.trade(0).symbol, QStringLiteral("TSLA"));
    }

    void repeats_unknown_ids_and_bad_prices_are_dropped() {
        FakeGateway gw;
        GatewayFeed feed(gw.endpoints(), fast());
        Recorder r(feed);
        feed.set_subscriptions({"AAPL", "MSFT"});
        feed.open();
        QTRY_VERIFY(gw.log().contains("SUB AAPL,MSFT"));
        const quint32 a = gw.id_of("AAPL"), m = gw.id_of("MSFT");
        gw.send(tick_frame(a, 5, 100, true)   // snapshot
                + tick_frame(a, 5, 100)       // the same tick live: a repeat
                + tick_frame(m, 4, 200)       // MSFT has its own sequence history
                + tick_frame(a, 3, 99)        // older than seen: never goes backwards
                + tick_frame(999, 6, 1)       // an id never acked
                + tick_frame(a, 7, 0)         // placeholder price
                + tick_frame(a, 8, 101));
        QTRY_COMPARE(r.trades.count(), 3);
        QTest::qWait(50);
        QCOMPARE(r.trades.count(), 3);
        QCOMPARE(r.trade(0).sequence, 5u);
        QCOMPARE(r.trade(1).symbol, QStringLiteral("MSFT"));
        QCOMPARE(r.trade(2).sequence, 8u);
        QCOMPARE(feed.stats().duplicates, 2u);
        QCOMPARE(feed.stats().unknown_ids, 1u);
    }

    // ── broken links ─────────────────────────────────────────────────────────

    void a_malformed_frame_drops_the_link_and_it_reconnects_data() {
        QTest::addColumn<QByteArray>("bad");
        QTest::newRow("unknown type") << header(99, 0);
        QTest::newRow("oversized body") << header(4, kMaxBody + 1);
        QTest::newRow("short tick") << header(4, 51) + tick_body(7, 1, 1).left(51);
        QTest::newRow("truncated ack") << header(3, 6) + QByteArray::fromHex("0100070000") + "\x05";
        QTest::newRow("client-only type") << header(1, 2) + QByteArray::fromHex("0000");
    }
    void a_malformed_frame_drops_the_link_and_it_reconnects() {
        QFETCH(QByteArray, bad);
        FakeGateway gw;
        GatewayFeed feed(gw.endpoints(), fast());
        Recorder r(feed);
        feed.set_subscriptions({"AAPL"});
        feed.open();
        QTRY_COMPARE(r.connected.count(), 1);
        QTRY_VERIFY(gw.log().contains("SUB AAPL"));
        gw.send(bad + tick_frame(gw.id_of("AAPL"), 1, 150)); // nothing after the bad frame may be trusted
        QTRY_COMPARE(r.disconnected.count(), 1);
        QTRY_COMPARE(r.connected.count(), 2);
        QTRY_COMPARE(gw.push_connections(), 2); // the server side may see it a moment later
        QCOMPARE(r.trades.count(), 0);
        QCOMPARE(r.unavailable.count(), 0);
    }

    void a_link_dropped_mid_frame_resubscribes_everything_on_reconnect() {
        FakeGateway gw;
        GatewayFeed feed(gw.endpoints(), fast());
        Recorder r(feed);
        feed.set_subscriptions({"AAPL", "MSFT"});
        // Changed while down — inside the disconnect itself, so it is certainly
        // before the reconnect: the next connection must carry the new set.
        connect(&feed, &GatewayFeed::disconnected, &feed, [&feed] { feed.subscribe({"TSLA"}); });
        feed.open();
        QTRY_VERIFY(gw.log().contains("SUB AAPL,MSFT"));
        gw.send(tick_frame(gw.id_of("AAPL"), 100, 150));
        QTRY_COMPARE(r.trades.count(), 1);

        // Half a frame, then the connection dies.
        gw.send(tick_frame(gw.id_of("MSFT"), 101, 400).left(30));
        QTest::qWait(30);
        gw.drop_client();
        QTRY_COMPARE(r.disconnected.count(), 1);
        QTRY_COMPARE(r.connected.count(), 2);
        QTRY_VERIFY(gw.log().contains("SUB AAPL,MSFT,TSLA"));

        // A restarted gateway counts from 1 again: AAPL's seq 1 after the old
        // connection's seq 100 is a new trade, not a repeat.
        gw.send(tick_frame(gw.id_of("AAPL"), 1, 151));
        QTRY_COMPARE(r.trades.count(), 2);
        QCOMPARE(r.trade(1).symbol, QStringLiteral("AAPL"));
        QCOMPARE(r.trade(1).price, 151.0);
        // The half MSFT frame was never emitted, before or after.
        for (int i = 0; i < r.trades.count(); ++i)
            QVERIFY(r.trade(i).symbol != QStringLiteral("MSFT"));
        QCOMPARE(feed.stats().connects, 2u);
    }

    void heartbeats_go_out_and_a_silent_link_is_dropped() {
        FakeGateway gw;
        GatewayFeed feed(gw.endpoints(), fast());
        Recorder r(feed);
        feed.open();
        QTRY_COMPARE(r.connected.count(), 1);
        QTRY_VERIFY(gw.log().count("HB") >= 3);
        QCOMPARE(r.disconnected.count(), 0); // echoed heartbeats keep a quiet link up
        gw.echo_heartbeats = false;          // now the link goes silent but stays open
        QTRY_COMPARE_WITH_TIMEOUT(r.disconnected.count(), 1, 2000);
        QTRY_COMPARE(r.connected.count(), 2);
    }

    void an_error_frame_is_reported_and_the_link_reconnects() {
        FakeGateway gw;
        GatewayFeed feed(gw.endpoints(), fast());
        Recorder r(feed);
        feed.open();
        QTRY_COMPARE(r.connected.count(), 1);
        gw.send(error_frame(4, "gateway symbol table is full"));
        QTRY_COMPARE(r.errors.count(), 1);
        QVERIFY(r.errors.at(0).at(0).toString().contains("symbol table is full"));
        QTRY_COMPARE(r.connected.count(), 2);
    }

    void gives_up_after_a_long_outage() {
        FakeGateway gw;
        GatewayFeed feed(gw.endpoints(), fast());
        Recorder r(feed);
        feed.open();
        QTRY_COMPARE(r.connected.count(), 1);
        QElapsedTimer t;
        t.start();
        gw.shut_down();
        QTRY_COMPARE(r.disconnected.count(), 1);
        QTRY_COMPARE_WITH_TIMEOUT(r.unavailable.count(), 1, 5000);
        const qint64 waited = t.elapsed();
        qInfo("gave up after %lld ms (give_up_after_ms 1500)", waited);
        QVERIFY(waited >= 1400);
        QVERIFY(r.unavailable.at(0).at(0).toString().contains("no gateway connection"));
        QCOMPARE(r.connected.count(), 1);
    }

    void close_is_final_and_quiet() {
        FakeGateway gw;
        GatewayFeed feed(gw.endpoints(), fast());
        Recorder r(feed);
        feed.open();
        QTRY_COMPARE(r.connected.count(), 1);
        feed.close();
        QCOMPARE(r.disconnected.count(), 1);
        feed.close();
        feed.open(); // final: no new attempt
        QTest::qWait(150);
        QCOMPARE(gw.push_connections(), 1);
        QCOMPARE(r.unavailable.count(), 0);
        QVERIFY(!feed.is_connected());
    }
};

QTEST_GUILESS_MAIN(TstGatewayFeed)
#include "tst_gateway_feed.moc"
