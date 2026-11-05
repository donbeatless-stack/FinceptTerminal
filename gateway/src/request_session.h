// gateway/src/request_session.h — internal
#pragma once

#include "registry.h"

#include <asio.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

namespace fmdg::detail {

/// One short connection on the request port: read one line, hand it to the
/// handler, write the reply, close. Everything runs on the socket's strand.
///
/// Three deadlines, one timer: kTimeout to send the line (idle or hostile
/// sockets cannot pile up); `command_timeout` for the handler's reply, which may
/// come later from a worker thread (a database query); kTimeout again for the
/// reply to drain and the client to close.
class RequestSession final : public std::enable_shared_from_this<RequestSession> {
  public:
    using Reply = std::function<void(std::string)>;
    /// Called once per session with the request line; calls `reply` once, now
    /// or later, from any thread. Later calls are ignored.
    using Handler = std::function<void(std::string_view line, Reply reply)>;
    static constexpr std::size_t kMaxLine = 4096;
    static constexpr std::chrono::seconds kTimeout{5};

    RequestSession(asio::ip::tcp::socket socket, Handler handler, Registry<RequestSession>& registry,
                   std::chrono::milliseconds command_timeout);

    void start();
    /// Any thread, idempotent.
    void close();

  private:
    void arm(std::chrono::milliseconds timeout, bool reply_on_expiry);
    void reply(std::string text); // strand only
    void linger();

    asio::ip::tcp::socket socket_;
    asio::steady_timer timer_;
    Handler handler_;
    Registry<RequestSession>& registry_;
    const std::chrono::milliseconds command_timeout_;
    std::atomic<bool> closed_{false};
    bool replied_ = false; ///< strand only
    std::string in_;
    std::string out_;
    std::array<char, 1024> discard_{};
};

} // namespace fmdg::detail
