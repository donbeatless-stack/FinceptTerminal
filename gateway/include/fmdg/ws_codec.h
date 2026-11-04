// gateway/include/fmdg/ws_codec.h
#pragma once
// RFC 6455 (WebSocket), client side: the opening-handshake helpers and the frame
// codec. Pure functions over bytes, no sockets, so every rule is unit-tested
// (tests/test_ws_codec.cpp); the transport that drives them is alpaca_feed.cpp.

#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace fmdg::ws {

enum class Opcode : std::uint8_t {
    Continuation = 0x0,
    Text = 0x1,
    Binary = 0x2,
    Close = 0x8,
    Ping = 0x9,
    Pong = 0xA,
};

/// Sec-WebSocket-Key for a 16-byte random nonce: its base64 (§4.1).
std::string client_key(const std::array<std::uint8_t, 16>& nonce);

/// The Sec-WebSocket-Accept a server must answer to `client_key`:
/// base64(SHA-1(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")) (§4.2.2).
std::string accept_key(std::string_view client_key);

/// The opening-handshake GET. `host` is the Host header value (add ":port" when
/// it is not the scheme's default). No extensions or subprotocols are offered.
std::string upgrade_request(std::string_view host, std::string_view path, std::string_view client_key);

/// Checks the server's handshake response head (status line and headers, up to
/// the blank line). Empty when the upgrade is accepted, otherwise why not: a
/// status other than 101 (the status line is quoted), a missing Upgrade or
/// Connection token, a wrong Sec-WebSocket-Accept, or an extension/subprotocol
/// we never offered.
std::string check_upgrade_response(std::string_view head, std::string_view client_key);

/// One client-to-server frame with FIN set. Client frames are always masked
/// (§5.3); `mask` should come from a strong random source. Control frames
/// (Close/Ping/Pong) must carry at most 125 bytes; the caller keeps to that.
std::string encode_frame(Opcode op, std::string_view payload, const std::array<std::uint8_t, 4>& mask);

/// A Close frame's payload: the status code, big-endian, then the reason.
std::string close_payload(std::uint16_t code, std::string_view reason = {});

/// One decoded server-to-client unit: a whole data message (fragments joined)
/// or a control frame.
struct Event {
    enum class Kind { Text, Binary, Ping, Pong, Close };
    Kind kind = Kind::Text;
    std::string payload;          ///< the message, the ping/pong data, or the close reason
    std::uint16_t close_code = 0; ///< Close only; 1005 ("no status") when the frame had none
};

/// Server-to-client frame decoder. Bytes go in as they arrive, split anywhere
/// (one byte at a time works); whole messages and control frames come out.
/// Enforces what a client must: server frames unmasked, RSV bits clear (no
/// extension was negotiated), known opcodes, control frames unfragmented and at
/// most 125 bytes, continuation frames only inside a fragmented message, and a
/// cap on message size checked against the header, before the payload arrives,
/// so a hostile length cannot make it buffer without bound. Text payloads are
/// not UTF-8 checked here; the JSON parser downstream rejects invalid UTF-8.
/// After an error the parser stays failed.
class FrameParser {
  public:
    explicit FrameParser(std::size_t max_message = std::size_t{16} << 20);

    /// Appends `bytes`, pushes every completed Event onto `out`. False on a
    /// protocol violation (error() says which); events decoded before it stay in `out`.
    bool feed(std::string_view bytes, std::vector<Event>& out);

    const std::string& error() const { return error_; }
    std::size_t buffered() const { return buf_.size() - pos_; }

  private:
    bool fail(std::string why);

    std::string buf_;      ///< received bytes; [pos_, size) not yet decoded
    std::size_t pos_ = 0;
    std::string message_;  ///< fragments of the data message in progress
    bool in_message_ = false;
    Opcode message_op_ = Opcode::Text;
    std::size_t max_message_;
    std::string error_;
};

} // namespace fmdg::ws
