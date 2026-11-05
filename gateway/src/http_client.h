// gateway/src/http_client.h — internal: GET over one persistent connection.
#pragma once

#include "fmdg/http.h"
#include "net.h"

#include <asio/experimental/awaitable_operators.hpp>

#include <array>
#include <chrono>
#include <memory>
#include <string>

namespace fmdg::net {

/// GETs against one host over a kept-alive connection (TLS or plain). A request
/// on a reused connection that fails before any response byte is retried once
/// on a fresh connection: the server may have closed an idle keep-alive
/// connection in the meantime. Each request, connect included, has a deadline.
/// One request at a time (it is used from a single coroutine).
class HttpClient {
  public:
    HttpClient(asio::ssl::context& ssl, std::string host, std::string port, bool tls)
        : ssl_(ssl), host_(std::move(host)), port_(std::move(port)), tls_(tls) {}

    asio::awaitable<http::Response> get(const std::string& target, const http::Headers& headers,
                                        std::chrono::milliseconds timeout) {
        using namespace asio::experimental::awaitable_operators;
        const auto ex = co_await asio::this_coro::executor;
        for (int attempt = 0;; ++attempt) {
            const bool reused = conn_ != nullptr;
            bool got_bytes = false;
            http::Response response;
            std::exception_ptr first;
            bool done = false;
            asio::steady_timer timer(ex);
            auto exchange = [&]() -> asio::awaitable<void> {
                if (!conn_) {
                    conn_ = std::make_unique<Connection>(ex, ssl_, tls_);
                    co_await conn_->open(host_, port_);
                }
                co_await conn_->write(http::get_request(host_header(), target, headers));
                http::ResponseParser parser;
                std::array<char, 16 * 1024> buf;
                for (;;) {
                    asio::error_code ec;
                    const std::size_t n = co_await conn_->read_some(asio::buffer(buf), ec);
                    if (ec) {
                        // A server closing without TLS close_notify shows up as stream_truncated.
                        if (ec != asio::error::eof && ec != asio::ssl::error::stream_truncated)
                            throw std::system_error(ec, "http read");
                        parser.finish_at_eof();
                        break;
                    }
                    got_bytes = true;
                    if (parser.feed(std::string_view(buf.data(), n)) != http::ResponseParser::State::Incomplete)
                        break;
                }
                if (parser.state() != http::ResponseParser::State::Complete)
                    throw std::runtime_error("http: " + parser.error());
                response = parser.take();
                done = true;
                timer.cancel();
            };
            bool failed = false;
            try {
                co_await (note_first_error(exchange(), first) &&
                          note_first_error(deadline(timer, timeout, done, "http request to " + host_), first));
            } catch (...) {
                failed = true;
                if (!first)
                    first = std::current_exception();
            }
            if (!failed) {
                if (!response.keep_alive)
                    conn_.reset();
                co_return response;
            }
            conn_.reset();
            if (!reused || got_bytes || attempt > 0)
                std::rethrow_exception(first);
            // A reused connection died before answering: one more try, fresh.
        }
    }

  private:
    std::string host_header() const {
        const bool default_port = port_ == (tls_ ? "443" : "80");
        return default_port ? host_ : host_ + ":" + port_;
    }

    asio::ssl::context& ssl_;
    const std::string host_, port_;
    const bool tls_;
    std::unique_ptr<Connection> conn_;
};

} // namespace fmdg::net
