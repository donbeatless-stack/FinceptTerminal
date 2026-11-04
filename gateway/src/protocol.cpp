// gateway/src/protocol.cpp
#include "fmdg/protocol.h"

#include <algorithm>
#include <bit>
#include <stdexcept>

namespace fmdg::proto {

namespace {

void put_u8(Bytes& out, std::uint8_t v) {
    out.push_back(v);
}

void put_u16(Bytes& out, std::uint16_t v) {
    out.push_back(static_cast<std::uint8_t>(v));
    out.push_back(static_cast<std::uint8_t>(v >> 8));
}

void put_u32(Bytes& out, std::uint32_t v) {
    for (int i = 0; i < 4; ++i)
        out.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}

void put_u64(Bytes& out, std::uint64_t v) {
    for (int i = 0; i < 8; ++i)
        out.push_back(static_cast<std::uint8_t>(v >> (8 * i)));
}

void put_i64(Bytes& out, std::int64_t v) {
    put_u64(out, static_cast<std::uint64_t>(v));
}

void put_f64(Bytes& out, double v) {
    put_u64(out, std::bit_cast<std::uint64_t>(v));
}

void put_header(Bytes& out, MsgType type, std::size_t body_len, std::uint16_t flags = 0) {
    put_u32(out, static_cast<std::uint32_t>(body_len));
    put_u16(out, static_cast<std::uint16_t>(type));
    put_u16(out, flags);
}

/// Bounds-checked cursor over an untrusted body: every read first checks that
/// the bytes are there, and a failed read poisons the cursor.
class Cursor {
  public:
    explicit Cursor(std::span<const std::uint8_t> s) : s_(s) {}

    bool ok() const { return ok_; }
    bool at_end() const { return ok_ && pos_ == s_.size(); }

    std::uint64_t uint(int bytes) {
        if (!take(static_cast<std::size_t>(bytes)))
            return 0;
        std::uint64_t v = 0;
        for (int i = 0; i < bytes; ++i)
            v |= static_cast<std::uint64_t>(s_[pos_ - static_cast<std::size_t>(bytes) + static_cast<std::size_t>(i)])
                 << (8 * i);
        return v;
    }
    double f64() { return std::bit_cast<double>(uint(8)); }
    std::int64_t i64() { return static_cast<std::int64_t>(uint(8)); }

    std::string str(std::size_t n) {
        if (!take(n))
            return {};
        return std::string(reinterpret_cast<const char*>(s_.data() + pos_ - n), n);
    }

  private:
    bool take(std::size_t n) {
        if (!ok_ || s_.size() - pos_ < n) {
            ok_ = false;
            return false;
        }
        pos_ += n;
        return true;
    }

    std::span<const std::uint8_t> s_;
    std::size_t pos_ = 0;
    bool ok_ = true;
};

} // namespace

bool valid_symbol(std::string_view s) {
    return !s.empty() && s.size() <= kMaxSymbolLen &&
           std::all_of(s.begin(), s.end(), [](char c) { return c > 0x20 && c < 0x7f; });
}

void append_symbols(Bytes& out, MsgType type, const std::vector<std::string>& symbols) {
    if (type != MsgType::Subscribe && type != MsgType::Unsubscribe)
        throw std::invalid_argument("append_symbols: type must be Subscribe or Unsubscribe");
    if (symbols.size() > kMaxSymbolsPerRequest)
        throw std::invalid_argument("append_symbols: too many symbols");
    std::size_t body = 2;
    for (const auto& s : symbols) {
        if (!valid_symbol(s))
            throw std::invalid_argument("append_symbols: invalid symbol '" + s + "'");
        body += 1 + s.size();
    }
    put_header(out, type, body);
    put_u16(out, static_cast<std::uint16_t>(symbols.size()));
    for (const auto& s : symbols) {
        put_u8(out, static_cast<std::uint8_t>(s.size()));
        out.insert(out.end(), s.begin(), s.end());
    }
}

void append_subscribed(Bytes& out, const std::vector<std::pair<SymbolId, std::string>>& ids) {
    std::size_t body = 2;
    for (const auto& [id, name] : ids)
        body += 4 + 1 + name.size();
    put_header(out, MsgType::Subscribed, body);
    put_u16(out, static_cast<std::uint16_t>(ids.size()));
    for (const auto& [id, name] : ids) {
        put_u32(out, id);
        put_u8(out, static_cast<std::uint8_t>(name.size()));
        out.insert(out.end(), name.begin(), name.end());
    }
}

void append_tick(Bytes& out, const Tick& t, std::int64_t send_ts_ns) {
    put_header(out, MsgType::Tick, kTickBodySize, (t.flags & kTickSnapshot) ? kFlagSnapshot : 0);
    put_u32(out, t.symbol);
    put_u64(out, t.sequence);
    put_f64(out, t.price);
    put_f64(out, t.size);
    put_i64(out, t.exchange_ts_ns);
    put_i64(out, t.ingest_ts_ns);
    put_i64(out, send_ts_ns);
}

void append_heartbeat(Bytes& out) {
    put_header(out, MsgType::Heartbeat, 0);
}

void append_error(Bytes& out, ErrorCode code, std::string_view message) {
    const std::string_view text = message.substr(0, 255);
    put_header(out, MsgType::Error, 2 + 1 + text.size());
    put_u16(out, static_cast<std::uint16_t>(code));
    put_u8(out, static_cast<std::uint8_t>(text.size()));
    out.insert(out.end(), text.begin(), text.end());
}

bool parse_header(const std::uint8_t* p, Header& h) {
    const std::uint32_t len = static_cast<std::uint32_t>(p[0]) | (static_cast<std::uint32_t>(p[1]) << 8) |
                              (static_cast<std::uint32_t>(p[2]) << 16) | (static_cast<std::uint32_t>(p[3]) << 24);
    const auto type = static_cast<std::uint16_t>(p[4] | (p[5] << 8));
    if (len > kMaxBody || type < static_cast<std::uint16_t>(MsgType::Subscribe) ||
        type > static_cast<std::uint16_t>(MsgType::Error))
        return false;
    h.body_len = len;
    h.type = static_cast<MsgType>(type);
    h.flags = static_cast<std::uint16_t>(p[6] | (p[7] << 8));
    return true;
}

std::optional<std::vector<std::string>> parse_symbols(std::span<const std::uint8_t> body) {
    Cursor c(body);
    const auto n = static_cast<std::size_t>(c.uint(2));
    if (!c.ok() || n > kMaxSymbolsPerRequest)
        return std::nullopt;
    std::vector<std::string> out;
    out.reserve(n);
    for (std::size_t i = 0; i < n; ++i) {
        const auto len = static_cast<std::size_t>(c.uint(1));
        out.push_back(c.str(len));
        if (!c.ok())
            return std::nullopt;
    }
    if (!c.at_end())
        return std::nullopt; // trailing bytes: not a frame this side wrote
    return out;
}

std::optional<std::vector<std::pair<SymbolId, std::string>>> parse_subscribed(std::span<const std::uint8_t> body) {
    Cursor c(body);
    const auto n = static_cast<std::size_t>(c.uint(2));
    if (!c.ok())
        return std::nullopt;
    std::vector<std::pair<SymbolId, std::string>> out;
    out.reserve(std::min<std::size_t>(n, kMaxSymbolsPerRequest));
    for (std::size_t i = 0; i < n; ++i) {
        const auto id = static_cast<SymbolId>(c.uint(4));
        const auto len = static_cast<std::size_t>(c.uint(1));
        std::string name = c.str(len);
        if (!c.ok())
            return std::nullopt;
        out.emplace_back(id, std::move(name));
    }
    if (!c.at_end())
        return std::nullopt;
    return out;
}

std::optional<TickMsg> parse_tick(std::span<const std::uint8_t> body) {
    if (body.size() != kTickBodySize)
        return std::nullopt;
    Cursor c(body);
    TickMsg m;
    m.tick.symbol = static_cast<SymbolId>(c.uint(4));
    m.tick.sequence = c.uint(8);
    m.tick.price = c.f64();
    m.tick.size = c.f64();
    m.tick.exchange_ts_ns = c.i64();
    m.tick.ingest_ts_ns = c.i64();
    m.send_ts_ns = c.i64();
    if (!c.at_end())
        return std::nullopt;
    return m;
}

std::optional<ErrorMsg> parse_error(std::span<const std::uint8_t> body) {
    Cursor c(body);
    ErrorMsg m;
    m.code = static_cast<ErrorCode>(c.uint(2));
    const auto len = static_cast<std::size_t>(c.uint(1));
    m.message = c.str(len);
    if (!c.at_end())
        return std::nullopt;
    return m;
}

void FrameReader::feed(const std::uint8_t* data, std::size_t n) {
    // Drop consumed bytes once they make up most of the buffer, so it neither
    // grows without bound nor gets shifted on every call.
    if (pos_ > 0 && pos_ >= buf_.size() / 2) {
        buf_.erase(buf_.begin(), buf_.begin() + static_cast<std::ptrdiff_t>(pos_));
        pos_ = 0;
    }
    buf_.insert(buf_.end(), data, data + n);
}

FrameReader::Result FrameReader::next(Header& h, std::span<const std::uint8_t>& body) {
    if (buffered() < kHeaderSize)
        return Result::NeedMore;
    if (!parse_header(buf_.data() + pos_, h))
        return Result::Bad;
    if (buffered() < kHeaderSize + h.body_len)
        return Result::NeedMore;
    body = std::span<const std::uint8_t>(buf_.data() + pos_ + kHeaderSize, h.body_len);
    pos_ += kHeaderSize + h.body_len;
    return Result::Frame;
}

} // namespace fmdg::proto
