// gateway/src/ws_codec.cpp
#include "fmdg/ws_codec.h"

#include <openssl/evp.h>

#include <algorithm>
#include <cctype>
#include <utility>

namespace fmdg::ws {

namespace {

constexpr std::string_view kAcceptGuid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

std::string base64(const unsigned char* data, std::size_t n) {
    std::string out(4 * ((n + 2) / 3), '\0');
    const int written = EVP_EncodeBlock(reinterpret_cast<unsigned char*>(out.data()), data, static_cast<int>(n));
    out.resize(static_cast<std::size_t>(std::max(written, 0)));
    return out;
}

bool iequals(std::string_view a, std::string_view b) {
    return a.size() == b.size() && std::equal(a.begin(), a.end(), b.begin(), [](char x, char y) {
               return std::tolower(static_cast<unsigned char>(x)) == std::tolower(static_cast<unsigned char>(y));
           });
}

std::string_view trim(std::string_view s) {
    while (!s.empty() && (s.front() == ' ' || s.front() == '\t'))
        s.remove_prefix(1);
    while (!s.empty() && (s.back() == ' ' || s.back() == '\t' || s.back() == '\r'))
        s.remove_suffix(1);
    return s;
}

// Does the comma-separated header value contain `token` (case-insensitive)?
bool has_token(std::string_view value, std::string_view token) {
    while (!value.empty()) {
        const std::size_t comma = value.find(',');
        if (iequals(trim(value.substr(0, comma)), token))
            return true;
        if (comma == std::string_view::npos)
            break;
        value.remove_prefix(comma + 1);
    }
    return false;
}

} // namespace

std::string client_key(const std::array<std::uint8_t, 16>& nonce) {
    return base64(nonce.data(), nonce.size());
}

std::string accept_key(std::string_view client_key) {
    std::string input(client_key);
    input += kAcceptGuid;
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int len = 0;
    EVP_Digest(input.data(), input.size(), digest, &len, EVP_sha1(), nullptr);
    return base64(digest, len);
}

std::string upgrade_request(std::string_view host, std::string_view path, std::string_view client_key) {
    std::string r;
    r.reserve(256);
    r.append("GET ").append(path).append(" HTTP/1.1\r\n");
    r.append("Host: ").append(host).append("\r\n");
    r.append("Upgrade: websocket\r\n");
    r.append("Connection: Upgrade\r\n");
    r.append("Sec-WebSocket-Key: ").append(client_key).append("\r\n");
    r.append("Sec-WebSocket-Version: 13\r\n");
    r.append("User-Agent: fmdg/0.1\r\n");
    r.append("\r\n");
    return r;
}

std::string check_upgrade_response(std::string_view head, std::string_view client_key) {
    const std::size_t eol = head.find("\r\n");
    const std::string_view status = trim(head.substr(0, eol));
    // "HTTP/1.1 101 Switching Protocols"
    if (status.size() < 12 || status.substr(0, 9) != "HTTP/1.1 " || status.substr(9, 3) != "101")
        return "handshake refused: " + std::string(status);

    bool upgrade = false, connection = false;
    std::string accept;
    std::string_view rest = eol == std::string_view::npos ? std::string_view{} : head.substr(eol + 2);
    while (!rest.empty()) {
        const std::size_t end = rest.find("\r\n");
        const std::string_view line = rest.substr(0, end);
        rest = end == std::string_view::npos ? std::string_view{} : rest.substr(end + 2);
        if (line.empty())
            break; // the blank line ending the head
        const std::size_t colon = line.find(':');
        if (colon == std::string_view::npos)
            continue;
        const std::string_view name = trim(line.substr(0, colon));
        const std::string_view value = trim(line.substr(colon + 1));
        if (iequals(name, "Upgrade"))
            upgrade = has_token(value, "websocket");
        else if (iequals(name, "Connection"))
            connection = has_token(value, "upgrade");
        else if (iequals(name, "Sec-WebSocket-Accept"))
            accept = std::string(value);
        else if (iequals(name, "Sec-WebSocket-Extensions"))
            return "handshake: server enabled an extension we did not offer: " + std::string(value);
        else if (iequals(name, "Sec-WebSocket-Protocol"))
            return "handshake: server chose a subprotocol we did not offer: " + std::string(value);
    }
    if (!upgrade)
        return "handshake: no 'Upgrade: websocket' header";
    if (!connection)
        return "handshake: no 'Connection: Upgrade' header";
    if (accept != accept_key(client_key))
        return "handshake: Sec-WebSocket-Accept does not match the key we sent";
    return {};
}

std::string encode_frame(Opcode op, std::string_view payload, const std::array<std::uint8_t, 4>& mask) {
    const std::size_t n = payload.size();
    std::string f;
    f.reserve(14 + n);
    f.push_back(static_cast<char>(0x80 | static_cast<std::uint8_t>(op))); // FIN + opcode
    if (n < 126) {
        f.push_back(static_cast<char>(0x80 | n));
    } else if (n <= 0xFFFF) {
        f.push_back(static_cast<char>(0x80 | 126));
        f.push_back(static_cast<char>((n >> 8) & 0xFF));
        f.push_back(static_cast<char>(n & 0xFF));
    } else {
        f.push_back(static_cast<char>(0x80 | 127));
        const auto n64 = static_cast<std::uint64_t>(n);
        for (int shift = 56; shift >= 0; shift -= 8)
            f.push_back(static_cast<char>((n64 >> shift) & 0xFF));
    }
    for (std::uint8_t m : mask)
        f.push_back(static_cast<char>(m));
    for (std::size_t i = 0; i < n; ++i)
        f.push_back(static_cast<char>(static_cast<std::uint8_t>(payload[i]) ^ mask[i % 4]));
    return f;
}

std::string close_payload(std::uint16_t code, std::string_view reason) {
    std::string p;
    p.push_back(static_cast<char>(code >> 8));
    p.push_back(static_cast<char>(code & 0xFF));
    p.append(reason.substr(0, 123)); // 125-byte control frame limit
    return p;
}

FrameParser::FrameParser(std::size_t max_message) : max_message_(max_message) {}

bool FrameParser::fail(std::string why) {
    error_ = std::move(why);
    return false;
}

bool FrameParser::feed(std::string_view bytes, std::vector<Event>& out) {
    if (!error_.empty())
        return false;
    buf_.append(bytes);

    for (;;) {
        const std::size_t avail = buf_.size() - pos_;
        if (avail < 2)
            break;
        const auto byte = [&](std::size_t i) { return static_cast<std::uint8_t>(buf_[pos_ + i]); };
        const bool fin = (byte(0) & 0x80) != 0;
        const std::uint8_t rsv = byte(0) & 0x70;
        const std::uint8_t op = byte(0) & 0x0F;
        const bool masked = (byte(1) & 0x80) != 0;
        std::uint64_t len = byte(1) & 0x7F;
        std::size_t header = 2;
        if (len == 126) {
            if (avail < 4)
                break;
            len = (std::uint64_t{byte(2)} << 8) | byte(3);
            header = 4;
        } else if (len == 127) {
            if (avail < 10)
                break;
            len = 0;
            for (std::size_t i = 2; i < 10; ++i)
                len = (len << 8) | byte(i);
            if (len >> 63)
                return fail("frame length has its most significant bit set");
            header = 10;
        }

        if (rsv != 0)
            return fail("RSV bits set, but no extension was negotiated");
        if (masked)
            return fail("server frame is masked");
        const bool control = (op & 0x08) != 0;
        if (control) {
            if (op != 0x8 && op != 0x9 && op != 0xA)
                return fail("unknown control opcode " + std::to_string(op));
            if (!fin)
                return fail("fragmented control frame");
            if (len > 125)
                return fail("control frame longer than 125 bytes");
        } else {
            if (op > 0x2)
                return fail("unknown data opcode " + std::to_string(op));
            if (op == 0x0 && !in_message_)
                return fail("continuation frame outside a fragmented message");
            if (op != 0x0 && in_message_)
                return fail("new data frame inside a fragmented message");
            // Checked from the header alone: a hostile length fails here instead of
            // making us wait for (and buffer) a payload that size.
            if (len > max_message_ - std::min(message_.size(), max_message_))
                return fail("message larger than " + std::to_string(max_message_) + " bytes");
        }
        if (avail - header < len)
            break; // payload not complete yet

        const std::string_view payload(buf_.data() + pos_ + header, static_cast<std::size_t>(len));
        pos_ += header + static_cast<std::size_t>(len);

        if (control) {
            Event e;
            if (op == 0x9) {
                e.kind = Event::Kind::Ping;
                e.payload = std::string(payload);
            } else if (op == 0xA) {
                e.kind = Event::Kind::Pong;
                e.payload = std::string(payload);
            } else {
                e.kind = Event::Kind::Close;
                if (payload.size() == 1)
                    return fail("close frame with a one-byte payload");
                if (payload.size() >= 2) {
                    e.close_code = static_cast<std::uint16_t>((static_cast<std::uint8_t>(payload[0]) << 8) |
                                                              static_cast<std::uint8_t>(payload[1]));
                    e.payload = std::string(payload.substr(2));
                } else {
                    e.close_code = 1005; // no status code present
                }
            }
            out.push_back(std::move(e));
            continue;
        }

        if (op != 0x0) {
            in_message_ = true;
            message_op_ = static_cast<Opcode>(op);
            message_.clear();
        }
        message_.append(payload);
        if (fin) {
            Event e;
            e.kind = message_op_ == Opcode::Text ? Event::Kind::Text : Event::Kind::Binary;
            e.payload = std::move(message_);
            out.push_back(std::move(e));
            message_.clear();
            in_message_ = false;
        }
    }

    // Drop decoded bytes: all of them when caught up, otherwise once enough has
    // piled up that the move is cheap relative to what it frees.
    if (pos_ == buf_.size()) {
        buf_.clear();
        pos_ = 0;
    } else if (pos_ > (std::size_t{64} << 10)) {
        buf_.erase(0, pos_);
        pos_ = 0;
    }
    return true;
}

} // namespace fmdg::ws
