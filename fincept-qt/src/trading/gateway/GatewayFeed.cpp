// src/trading/gateway/GatewayFeed.cpp
#include "trading/gateway/GatewayFeed.h"

#include <QRandomGenerator>
#include <QTcpSocket>
#include <QTimer>

#include <algorithm>

namespace fincept::trading {

using namespace gateway_feed;

GatewayFeed::GatewayFeed(Endpoints where, QObject* parent) : GatewayFeed(std::move(where), Options{}, parent) {}

GatewayFeed::GatewayFeed(Endpoints where, Options options, QObject* parent)
    : QObject(parent), where_(std::move(where)), opt_(options) {
    clock_.start();
    step_timer_ = new QTimer(this);
    step_timer_->setSingleShot(true);
    connect(step_timer_, &QTimer::timeout, this, [this] {
        // On Windows a port nobody listens on is refused only after ~2 s of SYN
        // retries, so "nothing there" usually ends here, not in errorOccurred.
        QString why;
        if (state_ == State::Probing && probe_ && probe_->state() == QAbstractSocket::ConnectedState)
            why = QStringLiteral("no FEED reply from %1:%2").arg(where_.host).arg(where_.request_port);
        else if (state_ == State::Probing)
            why = QStringLiteral("nothing accepted on %1:%2 within %3 ms")
                      .arg(where_.host)
                      .arg(where_.request_port)
                      .arg(opt_.connect_timeout_ms);
        else
            why = QStringLiteral("push port %1 did not accept within %2 ms").arg(endpoint_text()).arg(opt_.connect_timeout_ms);
        attempt_failed(why);
    });
    retry_timer_ = new QTimer(this);
    retry_timer_->setSingleShot(true);
    connect(retry_timer_, &QTimer::timeout, this, &GatewayFeed::attempt);
    heartbeat_timer_ = new QTimer(this);
    heartbeat_timer_->setInterval(opt_.heartbeat_ms);
    connect(heartbeat_timer_, &QTimer::timeout, this, &GatewayFeed::on_heartbeat);
}

GatewayFeed::~GatewayFeed() {
    // No signals from here on: the owner may be half destroyed.
    blockSignals(true);
    close();
}

QString GatewayFeed::endpoint_text() const {
    return QStringLiteral("%1:%2").arg(where_.push_host).arg(where_.push_port);
}

// ── public API ──────────────────────────────────────────────────────────────

void GatewayFeed::open() {
    if (state_ != State::Idle)
        return;
    attempt();
}

void GatewayFeed::close() {
    if (state_ == State::Closed)
        return;
    const bool was_live = state_ == State::Live;
    state_ = State::Closed;
    step_timer_->stop();
    retry_timer_->stop();
    heartbeat_timer_->stop();
    release_sockets();
    if (was_live)
        emit disconnected();
}

void GatewayFeed::subscribe(const QStringList& symbols) {
    QStringList all = wanted_;
    for (const QString& s : symbols)
        if (!all.contains(s))
            all.append(s);
    set_subscriptions(all);
}

void GatewayFeed::unsubscribe(const QStringList& symbols) {
    QStringList all = wanted_;
    for (const QString& s : symbols)
        all.removeAll(s);
    set_subscriptions(all);
}

void GatewayFeed::set_subscriptions(const QStringList& symbols) {
    QStringList next;
    for (const QString& s : symbols)
        if (valid_symbol(s) && !next.contains(s))
            next.append(s);
    QStringList add, remove;
    for (const QString& s : next)
        if (!wanted_.contains(s))
            add.append(s);
    for (const QString& s : wanted_)
        if (!next.contains(s))
            remove.append(s);
    wanted_ = next;
    if (state_ != State::Live)
        return; // a new connection subscribes the whole set
    if (!remove.isEmpty())
        send(encode_symbols(MsgType::Unsubscribe, remove));
    if (!add.isEmpty())
        send(encode_symbols(MsgType::Subscribe, add));
}

// ── one attempt: FEED on the request port, then the push port ───────────────

void GatewayFeed::attempt() {
    if (state_ == State::Closed || state_ == State::Unavailable)
        return;
    release_sockets();
    state_ = State::Probing;
    probe_reply_.clear();
    probe_ = new QTcpSocket(this);
    connect(probe_, &QTcpSocket::connected, this, [this] { probe_->write("FEED\n"); });
    connect(probe_, &QTcpSocket::readyRead, this, &GatewayFeed::on_probe_ready);
    // The gateway half-closes after its reply: the line may complete only at EOF.
    connect(probe_, &QTcpSocket::disconnected, this, &GatewayFeed::on_probe_ready);
    connect(probe_, &QTcpSocket::errorOccurred, this, [this](QAbstractSocket::SocketError e) {
        if (e == QAbstractSocket::RemoteHostClosedError)
            return; // the normal end of a reply; disconnected() handles it
        attempt_failed(QStringLiteral("request port %1:%2: %3")
                           .arg(where_.host)
                           .arg(where_.request_port)
                           .arg(probe_ ? probe_->errorString() : QString()));
    });
    step_timer_->start(opt_.connect_timeout_ms);
    probe_->connectToHost(where_.host, where_.request_port);
}

void GatewayFeed::on_probe_ready() {
    if (state_ != State::Probing || !probe_)
        return;
    probe_reply_.append(probe_->readAll());
    const bool at_eof = probe_->state() != QAbstractSocket::ConnectedState;
    if (!probe_reply_.contains('\n') && !at_eof) {
        if (probe_reply_.size() > 4096)
            attempt_failed(QStringLiteral("FEED reply too long"));
        return;
    }
    step_timer_->stop();
    const FeedStatus st = parse_feed_status(probe_reply_);
    stats_.last_feed_line = QString::fromLatin1(probe_reply_).section(QLatin1Char('\n'), 0, 0).trimmed();
    QTcpSocket* done = probe_;
    probe_ = nullptr;
    done->disconnect(this);
    done->abort();
    done->deleteLater();
    if (!st.usable) {
        attempt_failed(st.reason);
        return;
    }
    if (!st.on_demand && down_since_ms_ < 0)
        emit note(QStringLiteral("gateway streams only its own fixed symbols (on_demand=0): %1")
                      .arg(stats_.last_feed_line));

    state_ = State::Connecting;
    push_ = new QTcpSocket(this);
    connect(push_, &QTcpSocket::connected, this, &GatewayFeed::on_push_connected);
    connect(push_, &QTcpSocket::readyRead, this, &GatewayFeed::on_push_ready);
    connect(push_, &QTcpSocket::disconnected, this, [this] {
        if (state_ == State::Live)
            drop(QStringLiteral("gateway closed the push connection"));
    });
    connect(push_, &QTcpSocket::errorOccurred, this, [this](QAbstractSocket::SocketError) {
        const QString why = QStringLiteral("push port %1: %2").arg(endpoint_text(), push_ ? push_->errorString() : "");
        if (state_ == State::Live)
            drop(why);
        else if (state_ == State::Connecting)
            attempt_failed(why);
    });
    step_timer_->start(opt_.connect_timeout_ms);
    push_->connectToHost(where_.push_host, where_.push_port);
}

void GatewayFeed::on_push_connected() {
    if (state_ != State::Connecting)
        return;
    step_timer_->stop();
    push_->setSocketOption(QAbstractSocket::LowDelayOption, 1);
    state_ = State::Live;
    reader_.clear();
    names_.clear();
    last_seq_.clear(); // a restarted gateway starts its sequence again
    last_rx_ms_ = clock_.elapsed();
    backoff_ms_ = 0;
    down_since_ms_ = -1;
    ++stats_.connects;
    if (!wanted_.isEmpty())
        send(encode_symbols(MsgType::Subscribe, wanted_));
    heartbeat_timer_->start();
    emit note(QStringLiteral("live trades from gateway %1 (%2), %3 symbol(s)")
                  .arg(endpoint_text(), stats_.last_feed_line)
                  .arg(wanted_.size()));
    emit connected();
}

// ── the push connection ─────────────────────────────────────────────────────

void GatewayFeed::on_push_ready() {
    if (state_ != State::Live || !push_)
        return;
    last_rx_ms_ = clock_.elapsed();
    reader_.feed(push_->readAll());
    Frame f;
    for (;;) {
        const auto r = reader_.next(f);
        if (r == FrameReader::Result::NeedMore)
            return;
        if (r == FrameReader::Result::Bad) {
            ++stats_.bad_frames;
            drop(QStringLiteral("malformed frame header from the gateway"));
            return;
        }
        handle_frame(f);
        if (state_ != State::Live) // a slot closed us, or the frame was fatal
            return;
    }
}

void GatewayFeed::handle_frame(const Frame& f) {
    switch (f.type) {
        case MsgType::Subscribed: {
            const auto acks = parse_subscribed(f.body);
            if (!acks) {
                ++stats_.bad_frames;
                drop(QStringLiteral("malformed Subscribed frame"));
                return;
            }
            for (const auto& [id, name] : *acks)
                names_.insert(id, name);
            return;
        }
        case MsgType::Tick: {
            const auto t = parse_tick(f.body);
            if (!t) {
                ++stats_.bad_frames;
                drop(QStringLiteral("malformed Tick frame"));
                return;
            }
            handle_tick(*t, (f.flags & kFlagSnapshot) != 0);
            return;
        }
        case MsgType::Heartbeat:
            return; // the echo; last_rx_ms_ already moved
        case MsgType::Error: {
            const auto e = parse_error(f.body);
            const QString why = e ? QStringLiteral("gateway error %1: %2").arg(e->code).arg(e->message)
                                  : QStringLiteral("malformed Error frame");
            emit error_occurred(why);
            if (state_ == State::Live)
                drop(why); // the gateway closes after an error anyway
            return;
        }
        case MsgType::Subscribe:
        case MsgType::Unsubscribe:
            ++stats_.bad_frames;
            drop(QStringLiteral("gateway sent a client-only frame"));
            return;
    }
}

void GatewayFeed::handle_tick(const TickMsg& t, bool snapshot) {
    const auto name = names_.constFind(t.id);
    if (name == names_.cend()) {
        ++stats_.unknown_ids;
        return;
    }
    const QString& sym = name.value();
    if (!wanted_.contains(sym) || !(t.price > 0))
        return; // unsubscribed while in flight, or a placeholder price
    auto last = last_seq_.find(sym);
    if (last != last_seq_.end() && t.sequence <= last.value()) {
        ++stats_.duplicates;
        return;
    }
    if (last == last_seq_.end())
        last_seq_.insert(sym, t.sequence);
    else
        last.value() = t.sequence;
    gateway_feed::Trade trade;
    trade.symbol = sym;
    trade.price = t.price;
    trade.size = t.size;
    trade.exchange_ts_ms = t.exchange_ts_ns / 1'000'000;
    trade.sequence = t.sequence;
    trade.snapshot = snapshot;
    ++stats_.trades;
    if (snapshot)
        ++stats_.snapshots;
    emit trade_received(trade);
}

void GatewayFeed::on_heartbeat() {
    if (state_ != State::Live)
        return;
    const qint64 silent = clock_.elapsed() - last_rx_ms_;
    if (silent >= 3LL * opt_.heartbeat_ms) {
        drop(QStringLiteral("no data from the gateway for %1 ms, not even a heartbeat echo").arg(silent));
        return;
    }
    send(encode_heartbeat());
}

void GatewayFeed::send(const QByteArray& bytes) {
    if (push_ && !bytes.isEmpty())
        push_->write(bytes);
}

// ── failures ────────────────────────────────────────────────────────────────

void GatewayFeed::attempt_failed(const QString& why) {
    if (state_ != State::Probing && state_ != State::Connecting)
        return;
    step_timer_->stop();
    release_sockets();
    if (down_since_ms_ < 0) {
        give_up(why); // never connected: no usable gateway, the caller goes on without one
        return;
    }
    retry_or_give_up(why);
}

void GatewayFeed::drop(const QString& why) {
    if (state_ != State::Live)
        return;
    ++stats_.drops;
    heartbeat_timer_->stop();
    release_sockets();
    down_since_ms_ = clock_.elapsed();
    state_ = State::Backoff;
    emit note(QStringLiteral("gateway connection lost: %1").arg(why));
    emit disconnected();
    if (state_ == State::Backoff) // a slot may have closed us
        retry_or_give_up(why);
}

void GatewayFeed::retry_or_give_up(const QString& why) {
    if (clock_.elapsed() - down_since_ms_ >= opt_.give_up_after_ms) {
        give_up(QStringLiteral("no gateway connection for %1 ms: %2").arg(clock_.elapsed() - down_since_ms_).arg(why));
        return;
    }
    backoff_ms_ = backoff_ms_ == 0 ? opt_.backoff_initial_ms : std::min(backoff_ms_ * 2, opt_.backoff_max_ms);
    // 50-100 %: clients dropped together do not come back together.
    const int wait = backoff_ms_ / 2 + static_cast<int>(QRandomGenerator::global()->bounded(backoff_ms_ / 2 + 1));
    state_ = State::Backoff;
    retry_timer_->start(wait);
}

void GatewayFeed::give_up(const QString& why) {
    if (state_ == State::Unavailable || state_ == State::Closed)
        return;
    state_ = State::Unavailable;
    step_timer_->stop();
    retry_timer_->stop();
    heartbeat_timer_->stop();
    release_sockets();
    emit unavailable(why);
}

void GatewayFeed::release_sockets() {
    for (QTcpSocket** s : {&probe_, &push_}) {
        if (!*s)
            continue;
        (*s)->disconnect(this);
        (*s)->abort();
        (*s)->deleteLater();
        *s = nullptr;
    }
}

} // namespace fincept::trading
