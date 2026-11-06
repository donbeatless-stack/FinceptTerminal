// src/trading/gateway/GatewayFeed.h
#pragma once
// Live US-equity trades from the market-data gateway's push port
// (gateway/, protocol in GatewayFeedProtocol.h) instead of the terminal's own
// Alpaca WebSocket. Alpaca allows one stream connection per account: when a
// gateway holds it, the terminal must take its trades from the gateway, and the
// gateway subscribes upstream to whatever the terminal asks for.
//
// Life cycle (all on the owner's thread, no blocking):
//   open() → ask the request port "FEED" → if it streams live Alpaca trades,
//   connect the push port → subscribe everything wanted → Subscribed (ids),
//   a snapshot per symbol, then live trades.
//   - The first attempt failing in any way (nothing listening, a history-only
//     gateway, an old gateway without FEED, a timeout) emits unavailable() at
//     once: with no usable gateway the caller goes on exactly as before.
//   - Once connected, a drop is retried with backoff (doubling, 50-100 %
//     jitter); after give_up_after_ms without a connection, unavailable().
//   - Liveness: a heartbeat every heartbeat_ms, which the gateway echoes; three
//     intervals with no byte at all mean a dead link.
//   - Subscriptions are a set: changes go out as Subscribe/Unsubscribe
//     differences; a new connection subscribes the whole set again.
//   - A trade seen twice (snapshot racing a live tick, sequence not newer) is
//     dropped, as are trades for symbols no longer wanted and non-positive prices.

#include "trading/gateway/GatewayFeedProtocol.h"

#include <QElapsedTimer>
#include <QHash>
#include <QObject>

#include <QStringList>

class QTcpSocket;
class QTimer;

namespace fincept::trading {

class GatewayFeed : public QObject {
    Q_OBJECT
  public:
    struct Options {
        /// Per step (request-port connect + FEED reply, then push connect). With no
        /// gateway this is how long the fallback takes: on Windows a refused
        /// loopback connect is only reported after ~2 s. Short enough not to hold
        /// the broker stream up, long enough for a busy UI thread at start-up.
        int connect_timeout_ms = 1000;
        int heartbeat_ms = 5000;
        int backoff_initial_ms = 500;
        int backoff_max_ms = 10000;
        int give_up_after_ms = 30000; ///< after a drop, keep trying this long
    };

    struct Stats {
        quint64 connects = 0;
        quint64 drops = 0;
        quint64 trades = 0;     ///< emitted, snapshots included
        quint64 snapshots = 0;  ///< of those, snapshots
        quint64 duplicates = 0; ///< dropped: sequence not newer than the symbol's last
        quint64 unknown_ids = 0;
        quint64 bad_frames = 0;
        QString last_feed_line;
    };

    explicit GatewayFeed(gateway_feed::Endpoints where, QObject* parent = nullptr);
    GatewayFeed(gateway_feed::Endpoints where, Options options, QObject* parent = nullptr);
    ~GatewayFeed() override;

    void open();
    /// Idempotent; final. Invokable by name, like the broker sockets'.
    Q_INVOKABLE void close();

    bool is_connected() const { return state_ == State::Live; }
    bool is_unavailable() const { return state_ == State::Unavailable; }

    void subscribe(const QStringList& symbols);
    void unsubscribe(const QStringList& symbols);
    void set_subscriptions(const QStringList& symbols);
    QStringList subscriptions() const { return wanted_; }

    Stats stats() const { return stats_; }
    QString endpoint_text() const;

  signals:
    void trade_received(const fincept::trading::gateway_feed::Trade& trade);
    void connected();
    void disconnected();
    /// Once, final: the gateway cannot (or can no longer) serve live data.
    void unavailable(const QString& reason);
    void error_occurred(const QString& error);
    /// One line per event worth a log entry; the owner logs it.
    void note(const QString& line);

  private:
    enum class State { Idle, Probing, Connecting, Live, Backoff, Unavailable, Closed };

    void attempt();
    void on_probe_ready();
    void on_push_connected();
    void on_push_ready();
    void on_heartbeat();
    void handle_frame(const gateway_feed::Frame& f);
    void handle_tick(const gateway_feed::TickMsg& t, bool snapshot);
    void attempt_failed(const QString& why);
    void drop(const QString& why);
    void retry_or_give_up(const QString& why);
    void give_up(const QString& why);
    void release_sockets();
    void send(const QByteArray& bytes);

    const gateway_feed::Endpoints where_;
    const Options opt_;
    State state_ = State::Idle;

    QTcpSocket* probe_ = nullptr;
    QTcpSocket* push_ = nullptr;
    QByteArray probe_reply_;
    gateway_feed::FrameReader reader_;
    QTimer* step_timer_ = nullptr;  ///< deadline of the current probe/connect step
    QTimer* retry_timer_ = nullptr;
    QTimer* heartbeat_timer_ = nullptr;

    QElapsedTimer clock_;
    qint64 last_rx_ms_ = 0;
    qint64 down_since_ms_ = -1; ///< when the last live connection dropped; -1 = never live
    int backoff_ms_ = 0;

    QStringList wanted_;
    QHash<quint32, QString> names_;     ///< ids from Subscribed, this connection
    QHash<QString, quint64> last_seq_;  ///< per symbol, this connection
    Stats stats_;
};

} // namespace fincept::trading
