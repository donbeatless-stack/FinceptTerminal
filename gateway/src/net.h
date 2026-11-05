// gateway/src/net.h — internal: the outbound-connection pieces the Alpaca feed
// (WebSocket) and the REST client (HTTP) share.
#pragma once

#include "tls_roots.h"

#include <asio.hpp>
#include <asio/ssl.hpp>

#include <chrono>
#include <exception>
#include <optional>
#include <string>

namespace fmdg::net {

/// Verification on; trusts `ca_file` if given, else the system roots.
inline void configure_tls(asio::ssl::context& ssl, const std::string& ca_file) {
    ssl.set_verify_mode(asio::ssl::verify_peer);
    if (!ca_file.empty())
        ssl.load_verify_file(ca_file);
    else
        load_system_roots(ssl.native_handle());
}

/// TCP, or TLS over TCP, behind one read/write interface.
class Connection {
  public:
    Connection(const asio::any_io_executor& ex, asio::ssl::context& ssl, bool tls) : plain_(ex) {
        if (tls)
            tls_.emplace(ex, ssl);
    }

    asio::ip::tcp::socket& socket() { return tls_ ? tls_->next_layer() : plain_; }
    asio::ssl::stream<asio::ip::tcp::socket>* tls() { return tls_ ? &*tls_ : nullptr; }

    /// Resolve, connect, and (for TLS) handshake with SNI and the hostname checked.
    asio::awaitable<void> open(const std::string& host, const std::string& port) {
        const auto ex = co_await asio::this_coro::executor;
        asio::ip::tcp::resolver resolver(ex);
        const auto endpoints = co_await resolver.async_resolve(host, port, asio::use_awaitable);
        co_await asio::async_connect(socket(), endpoints, asio::use_awaitable);
        socket().set_option(asio::ip::tcp::no_delay(true));
        if (tls_) {
            SSL_set_tlsext_host_name(tls_->native_handle(), host.c_str());
            tls_->set_verify_callback(asio::ssl::host_name_verification(host));
            co_await tls_->async_handshake(asio::ssl::stream_base::client, asio::use_awaitable);
        }
    }

    asio::awaitable<std::size_t> read_some(asio::mutable_buffer b, asio::error_code& ec) {
        if (tls_)
            co_return co_await tls_->async_read_some(b, asio::redirect_error(asio::use_awaitable, ec));
        co_return co_await plain_.async_read_some(b, asio::redirect_error(asio::use_awaitable, ec));
    }

    asio::awaitable<void> write(const std::string& data) {
        if (tls_)
            co_await asio::async_write(*tls_, asio::buffer(data), asio::use_awaitable);
        else
            co_await asio::async_write(plain_, asio::buffer(data), asio::use_awaitable);
    }

  private:
    asio::ip::tcp::socket plain_;
    std::optional<asio::ssl::stream<asio::ip::tcp::socket>> tls_;
};

/// Runs `a`, noting the first exception any such wrapper sees. `x && y` cancels
/// the survivor when one fails; the survivor then throws operation_aborted, and
/// with two exceptions `&&` reports only asio::multiple_exceptions. Callers
/// rethrow the one that came first: the actual cause.
inline asio::awaitable<void> note_first_error(asio::awaitable<void> a, std::exception_ptr& first) {
    try {
        co_await std::move(a);
    } catch (...) {
        if (!first)
            first = std::current_exception();
        throw;
    }
}

/// Fails with `what` + " timed out" after `timeout`, unless `done` is set and
/// the timer cancelled first. Pair with the guarded operation through `&&`.
inline asio::awaitable<void> deadline(asio::steady_timer& timer, std::chrono::milliseconds timeout, const bool& done,
                                      std::string what) {
    timer.expires_after(timeout);
    asio::error_code ec;
    if (!done)
        co_await timer.async_wait(asio::redirect_error(asio::use_awaitable, ec));
    if (!ec && !done)
        throw std::runtime_error(what + " timed out after " + std::to_string(timeout.count()) + " ms");
}

} // namespace fmdg::net
