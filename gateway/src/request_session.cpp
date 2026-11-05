// gateway/src/request_session.cpp
#include "request_session.h"

#include <utility>

namespace fmdg::detail {

RequestSession::RequestSession(asio::ip::tcp::socket socket, Handler handler, Registry<RequestSession>& registry,
                               std::chrono::milliseconds command_timeout)
    : socket_(std::move(socket)), timer_(socket_.get_executor()), handler_(std::move(handler)), registry_(registry),
      command_timeout_(command_timeout) {}

// (Re)starts the session deadline. Re-arming cancels the previous wait, whose
// handler then sees operation_aborted and does nothing.
void RequestSession::arm(std::chrono::milliseconds timeout, bool reply_on_expiry) {
    timer_.expires_after(timeout);
    timer_.async_wait([self = shared_from_this(), reply_on_expiry](const asio::error_code& ec) {
        if (ec)
            return; // re-armed or cancelled: the session moved on
        if (reply_on_expiry)
            self->reply("ERR timed out");
        else
            self->close();
    });
}

void RequestSession::start() {
    auto self = shared_from_this();
    asio::post(socket_.get_executor(), [self] {
        self->arm(kTimeout, false);
        asio::async_read_until(self->socket_, asio::dynamic_buffer(self->in_, kMaxLine), '\n',
                               [self](const asio::error_code& ec, std::size_t n) {
                                   if (ec == asio::error::not_found) {
                                       self->reply("ERR request line longer than 4096 bytes");
                                       return;
                                   }
                                   if (ec == asio::error::eof && !self->in_.empty())
                                       n = self->in_.size(); // last line without '\n'
                                   else if (ec) {
                                       self->close();
                                       return;
                                   }
                                   std::string_view line(self->in_.data(), n);
                                   while (!line.empty() && (line.back() == '\n' || line.back() == '\r'))
                                       line.remove_suffix(1);
                                   // The handler may answer from another thread: hop
                                   // back onto the strand before touching the session.
                                   self->arm(self->command_timeout_, true);
                                   self->handler_(line, [self](std::string text) {
                                       asio::post(self->socket_.get_executor(),
                                                  [self, text = std::move(text)]() mutable {
                                                      self->reply(std::move(text));
                                                  });
                                   });
                               });
    });
}

void RequestSession::reply(std::string text) {
    if (replied_ || closed_.load(std::memory_order_acquire))
        return; // a late reply after a timeout, or a second call: ignored
    replied_ = true;
    arm(kTimeout, false); // the reply must drain, and the client close, in time
    out_ = std::move(text);
    if (out_.empty() || out_.back() != '\n')
        out_ += '\n';
    asio::async_write(socket_, asio::buffer(out_), [self = shared_from_this()](const asio::error_code& ec, std::size_t) {
        if (ec) {
            self->close();
            return;
        }
        asio::error_code ignored;
        self->socket_.shutdown(asio::ip::tcp::socket::shutdown_send, ignored); // FIN after the reply
        self->linger();
    });
}

// Closing a socket that still has unread input makes the OS send a reset, and a
// reset can destroy the reply before the client has read it (a client that sent
// an over-long line is the usual case). So after the reply, read and discard
// until the client closes its side; the session timer bounds the wait.
void RequestSession::linger() {
    socket_.async_read_some(asio::buffer(discard_), [self = shared_from_this()](const asio::error_code& ec, std::size_t) {
        if (ec)
            self->close();
        else
            self->linger();
    });
}

void RequestSession::close() {
    if (closed_.exchange(true, std::memory_order_acq_rel))
        return;
    registry_.remove(this);
    asio::post(socket_.get_executor(), [self = shared_from_this()] {
        self->timer_.cancel();
        asio::error_code ignored;
        self->socket_.shutdown(asio::ip::tcp::socket::shutdown_both, ignored);
        self->socket_.close(ignored);
    });
}

} // namespace fmdg::detail
