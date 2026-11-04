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

/// One short connection on the request port: read one line, write the reply,
/// close. A line longer than kMaxLine is refused, and a client that has not
/// finished its line within kTimeout is dropped, so idle or hostile sockets
/// cannot pile up. Like PushSession, everything runs on the socket's strand.
class RequestSession final : public std::enable_shared_from_this<RequestSession> {
  public:
    using Handler = std::function<std::string(std::string_view)>;
    static constexpr std::size_t kMaxLine = 4096;
    static constexpr std::chrono::seconds kTimeout{5};

    RequestSession(asio::ip::tcp::socket socket, Handler handler, Registry<RequestSession>& registry);

    void start();
    /// Any thread, idempotent.
    void close();

  private:
    void reply(std::string text);
    void linger();

    asio::ip::tcp::socket socket_;
    asio::steady_timer timer_;
    Handler handler_;
    Registry<RequestSession>& registry_;
    std::atomic<bool> closed_{false};
    std::string in_;
    std::string out_;
    std::array<char, 1024> discard_{};
};

} // namespace fmdg::detail
