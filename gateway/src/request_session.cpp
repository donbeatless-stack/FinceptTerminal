// gateway/src/request_session.cpp
#include "request_session.h"

#include <utility>

namespace fmdg::detail {

RequestSession::RequestSession(asio::ip::tcp::socket socket, Handler handler, Registry<RequestSession>& registry)
    : socket_(std::move(socket)), timer_(socket_.get_executor()), handler_(std::move(handler)), registry_(registry) {}

void RequestSession::start() {
    auto self = shared_from_this();
    asio::post(socket_.get_executor(), [self] {
        self->timer_.expires_after(kTimeout);
        self->timer_.async_wait([self](const asio::error_code& ec) {
            if (!ec)
                self->close(); // timed out; a cancelled wait means we finished first
        });
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
                                   self->reply(self->handler_(line));
                               });
    });
}

void RequestSession::reply(std::string text) {
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
