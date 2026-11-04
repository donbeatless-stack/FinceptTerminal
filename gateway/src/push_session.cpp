// gateway/src/push_session.cpp
#include "push_session.h"

#include <string>
#include <utility>

namespace fmdg::detail {

PushSession::PushSession(asio::ip::tcp::socket socket, SessionContext& ctx) : socket_(std::move(socket)), ctx_(ctx) {}

void PushSession::start() {
    ctx_.counters.clients.fetch_add(1, std::memory_order_relaxed);
    asio::post(socket_.get_executor(), [self = shared_from_this()] { self->read_header(); });
}

void PushSession::deliver(const Tick& t) {
    if (closed_.load(std::memory_order_acquire))
        return;
    queue_.push(t);
    wake();
}

void PushSession::wake() {
    if (!writing_.exchange(true, std::memory_order_acq_rel))
        asio::post(socket_.get_executor(), [self = shared_from_this()] { self->write_next(); });
}

void PushSession::write_next() {
    if (closed_.load(std::memory_order_acquire))
        return;
    out_.clear();
    out_.swap(control_); // replies first, then ticks; control_ keeps out_'s old capacity
    batch_.clear();
    queue_.drain(batch_, ctx_.max_batch);
    if (!batch_.empty()) {
        const std::int64_t send_ts = now_ns();
        for (const Tick& t : batch_) {
            proto::append_tick(out_, t, send_ts);
            if (!(t.flags & kTickSnapshot)) // a snapshot's age is not latency
                ctx_.latency.record(send_ts - t.ingest_ts_ns);
        }
        ctx_.counters.ticks_out.fetch_add(batch_.size(), std::memory_order_relaxed);
    }
    if (out_.empty()) {
        if (closing_) {
            close();
            return;
        }
        writing_.store(false, std::memory_order_release);
        // A deliver() between drain() and the store above saw writing_ == true and
        // left its tick to us: look once more before going idle.
        if (queue_.size() > 0)
            wake();
        return;
    }
    asio::async_write(socket_, asio::buffer(out_),
                      [self = shared_from_this()](const asio::error_code& ec, std::size_t n) {
                          if (ec) {
                              self->close();
                              return;
                          }
                          self->ctx_.counters.bytes_out.fetch_add(n, std::memory_order_relaxed);
                          self->write_next();
                      });
}

void PushSession::read_header() {
    asio::async_read(socket_, asio::buffer(header_), [self = shared_from_this()](const asio::error_code& ec, std::size_t) {
        if (ec) {
            self->close();
            return;
        }
        proto::Header h;
        if (!proto::parse_header(self->header_.data(), h)) {
            self->fail(proto::ErrorCode::BadFrame, "bad frame header");
            return;
        }
        self->read_body(h);
    });
}

void PushSession::read_body(proto::Header h) {
    body_.resize(h.body_len);
    if (h.body_len == 0) {
        handle(h.type);
        return;
    }
    asio::async_read(socket_, asio::buffer(body_),
                     [self = shared_from_this(), type = h.type](const asio::error_code& ec, std::size_t) {
                         if (ec) {
                             self->close();
                             return;
                         }
                         self->handle(type);
                     });
}

void PushSession::handle(proto::MsgType type) {
    switch (type) {
        case proto::MsgType::Subscribe:
        case proto::MsgType::Unsubscribe:
            on_symbols(type);
            break;
        case proto::MsgType::Heartbeat:
            proto::append_heartbeat(control_);
            wake();
            break;
        default:
            fail(proto::ErrorCode::BadFrame, "message type not accepted from clients");
            break;
    }
    if (!closing_ && !closed_.load(std::memory_order_acquire))
        read_header();
}

void PushSession::on_symbols(proto::MsgType type) {
    // Tell "too many" apart from "malformed": the count is the first u16.
    if (body_.size() >= 2) {
        const std::size_t n = static_cast<std::size_t>(body_[0]) | (static_cast<std::size_t>(body_[1]) << 8);
        if (n > proto::kMaxSymbolsPerRequest) {
            fail(proto::ErrorCode::TooManySymbols, "more than 1024 symbols in one request");
            return;
        }
    }
    const auto names = proto::parse_symbols(body_);
    if (!names) {
        fail(proto::ErrorCode::BadFrame, "malformed symbol list");
        return;
    }
    for (const auto& name : *names) {
        if (!proto::valid_symbol(name)) {
            fail(proto::ErrorCode::BadSymbol, "invalid symbol");
            return;
        }
    }

    std::vector<SymbolId> ids;
    ids.reserve(names->size());
    if (type == proto::MsgType::Subscribe) {
        std::vector<std::pair<SymbolId, std::string>> acks;
        acks.reserve(names->size());
        for (const auto& name : *names) {
            const auto id = ctx_.symbols.intern(name);
            if (!id) {
                fail(proto::ErrorCode::SymbolLimit, "gateway symbol table is full");
                return;
            }
            ids.push_back(*id);
            acks.emplace_back(*id, name);
        }
        // The ack goes into control_, which write_next() sends ahead of any tick,
        // so the client learns the ids before the first snapshot arrives.
        proto::append_subscribed(control_, acks);
        wake();
        ctx_.distributor.subscribe(shared_from_this(), ids);
    } else {
        for (const auto& name : *names)
            if (const auto id = ctx_.symbols.find(name))
                ids.push_back(*id);
        ctx_.distributor.unsubscribe(this, ids);
    }
}

void PushSession::fail(proto::ErrorCode code, std::string_view why) {
    ctx_.counters.protocol_errors.fetch_add(1, std::memory_order_relaxed);
    ctx_.distributor.remove(this); // no new ticks: the queue drains and the error goes out
    proto::append_error(control_, code, why);
    closing_ = true;
    wake();
}

void PushSession::close() {
    if (closed_.exchange(true, std::memory_order_acq_rel))
        return;
    ctx_.distributor.remove(this);
    ctx_.registry.remove(this);
    ctx_.counters.clients.fetch_sub(1, std::memory_order_relaxed);
    ctx_.counters.conflated_closed.fetch_add(queue_.conflated(), std::memory_order_relaxed);
    asio::post(socket_.get_executor(), [self = shared_from_this()] {
        asio::error_code ignored;
        self->socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
        self->socket_.close(ignored);
    });
}

} // namespace fmdg::detail
