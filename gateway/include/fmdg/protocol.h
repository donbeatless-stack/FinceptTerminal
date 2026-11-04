// gateway/include/fmdg/protocol.h
#pragma once
// Binary protocol of the push port (8589). Every frame is an 8-byte header —
// u32 body length, u16 message type, u16 flags — followed by the body.
// All integers are little-endian, written byte by byte so the encoding does not
// depend on the host. Bodies:
//
//   Subscribe / Unsubscribe   client → gateway   u16 n, then n × (u8 len, len bytes)
//   Subscribed                gateway → client   u16 n, then n × (u32 id, u8 len, len bytes)
//   Tick                      gateway → client   u32 symbol id, u64 sequence, f64 price,
//                                                f64 size, i64 exchange ts, i64 ingest ts,
//                                                i64 send ts  (52 bytes)
//   Heartbeat                 either way         empty (the gateway echoes it)
//   Error                     gateway → client   u16 code, u8 len, len bytes of text
//
// Header flags: bit 0 on a Tick frame marks a snapshot (the symbol's last value,
// sent on subscribe, not a new update). Other bits are 0.
//
// Every parser treats its input as untrusted: lengths are checked against the
// buffer before anything is read, and a malformed body yields std::nullopt.

#include "fmdg/tick.h"

#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace fmdg::proto {

enum class MsgType : std::uint16_t {
    Subscribe = 1,
    Unsubscribe = 2,
    Subscribed = 3,
    Tick = 4,
    Heartbeat = 5,
    Error = 6,
};

enum class ErrorCode : std::uint16_t {
    BadFrame = 1,       ///< unparseable header or body, or a type clients may not send
    BadSymbol = 2,      ///< empty, too long, or not printable ASCII
    TooManySymbols = 3, ///< more than kMaxSymbolsPerRequest in one request
    SymbolLimit = 4,    ///< the gateway's symbol table is full
};

inline constexpr std::size_t kHeaderSize = 8;
inline constexpr std::uint32_t kMaxBody = 64 * 1024;
inline constexpr std::size_t kMaxSymbolLen = 32;
inline constexpr std::size_t kMaxSymbolsPerRequest = 1024;
inline constexpr std::size_t kTickBodySize = 4 + 8 + 8 + 8 + 8 + 8 + 8;

using Bytes = std::vector<std::uint8_t>;

inline constexpr std::uint16_t kFlagSnapshot = 1;

struct Header {
    std::uint32_t body_len = 0;
    MsgType type = MsgType::Heartbeat;
    std::uint16_t flags = 0;
};

struct TickMsg {
    Tick tick; ///< tick.flags is not on the wire in the body: see Header::flags
    std::int64_t send_ts_ns = 0; ///< now_ns() when the gateway handed the frame to the socket
};

struct ErrorMsg {
    ErrorCode code = ErrorCode::BadFrame;
    std::string message;
};

/// 1..kMaxSymbolLen characters of printable ASCII, no spaces.
bool valid_symbol(std::string_view s);

// ── Encoding: each appends one whole frame to `out` ─────────────────────────
/// Throws std::invalid_argument on an invalid symbol or more than
/// kMaxSymbolsPerRequest of them: a client bug, not wire input.
void append_symbols(Bytes& out, MsgType type, const std::vector<std::string>& symbols);
void append_subscribed(Bytes& out, const std::vector<std::pair<SymbolId, std::string>>& ids);
void append_tick(Bytes& out, const Tick& tick, std::int64_t send_ts_ns);
void append_heartbeat(Bytes& out);
/// The message is cut to 255 bytes.
void append_error(Bytes& out, ErrorCode code, std::string_view message);

// ── Decoding ────────────────────────────────────────────────────────────────
/// False on an unknown type or a body longer than kMaxBody.
bool parse_header(const std::uint8_t* p, Header& h);
std::optional<std::vector<std::string>> parse_symbols(std::span<const std::uint8_t> body);
std::optional<std::vector<std::pair<SymbolId, std::string>>> parse_subscribed(std::span<const std::uint8_t> body);
std::optional<TickMsg> parse_tick(std::span<const std::uint8_t> body);
std::optional<ErrorMsg> parse_error(std::span<const std::uint8_t> body);

/// Splits a byte stream into frames for a reader that receives arbitrary chunks
/// (one recv() can hold half a frame or a hundred). Feed bytes in, then call
/// next() until it stops returning Frame; the returned body stays valid until the
/// next feed() or next().
class FrameReader {
  public:
    enum class Result { Frame, NeedMore, Bad };

    void feed(const std::uint8_t* data, std::size_t n);
    Result next(Header& h, std::span<const std::uint8_t>& body);
    std::size_t buffered() const { return buf_.size() - pos_; }

  private:
    Bytes buf_;
    std::size_t pos_ = 0;
};

} // namespace fmdg::proto
