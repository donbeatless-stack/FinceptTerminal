// gateway/src/http.cpp
#include "fmdg/http.h"

#include <algorithm>
#include <cctype>
#include <charconv>

namespace fmdg::http {

namespace {

constexpr std::size_t kMaxHead = 32 * 1024;
constexpr std::size_t kMaxLine = 8 * 1024; // chunk-size and trailer lines

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

std::string Response::header(std::string_view name) const {
    for (const auto& [k, v] : headers)
        if (iequals(k, name))
            return v;
    return {};
}

std::string get_request(std::string_view host, std::string_view target, const Headers& headers) {
    std::string r;
    r.append("GET ").append(target).append(" HTTP/1.1\r\nHost: ").append(host).append("\r\n");
    for (const auto& [k, v] : headers)
        r.append(k).append(": ").append(v).append("\r\n");
    r.append("\r\n");
    return r;
}

std::string url_encode(std::string_view s) {
    static constexpr char kHex[] = "0123456789ABCDEF";
    std::string out;
    for (char c : s) {
        const auto u = static_cast<unsigned char>(c);
        if (std::isalnum(u) || c == '-' || c == '_' || c == '.' || c == '~') {
            out += c;
        } else {
            out += '%';
            out += kHex[u >> 4];
            out += kHex[u & 0xF];
        }
    }
    return out;
}

ResponseParser::ResponseParser(std::size_t max_body) : max_body_(max_body) {}

ResponseParser::State ResponseParser::fail(std::string why) {
    error_ = std::move(why);
    state_ = State::Error;
    return state_;
}

ResponseParser::State ResponseParser::parse_head(std::string_view head) {
    const std::size_t eol = head.find("\r\n");
    const std::string_view status_line = head.substr(0, eol);
    // "HTTP/1.1 200 OK"
    if (status_line.size() < 12 || status_line.substr(0, 7) != "HTTP/1." || status_line[8] != ' ')
        return fail("bad status line: " + std::string(status_line.substr(0, 80)));
    const bool http10 = status_line[7] == '0';
    int status = 0;
    const auto code = status_line.substr(9, 3);
    if (std::from_chars(code.data(), code.data() + 3, status).ec != std::errc{} || status < 100 || status > 599)
        return fail("bad status code: " + std::string(code));

    Response r;
    r.status = status;
    r.reason = std::string(trim(status_line.size() > 12 ? status_line.substr(13) : std::string_view{}));
    std::string_view rest = eol == std::string_view::npos ? std::string_view{} : head.substr(eol + 2);
    while (!rest.empty()) {
        const std::size_t end = rest.find("\r\n");
        const std::string_view line = rest.substr(0, end);
        rest = end == std::string_view::npos ? std::string_view{} : rest.substr(end + 2);
        const std::size_t colon = line.find(':');
        if (colon == std::string_view::npos || colon == 0)
            return fail("bad header line");
        r.headers.emplace_back(std::string(trim(line.substr(0, colon))), std::string(trim(line.substr(colon + 1))));
    }

    if (status >= 100 && status < 200) {
        if (status == 101)
            return fail("unexpected 101 Switching Protocols");
        phase_ = Phase::Head; // interim response: the real one follows
        return state_;
    }

    const std::string connection = r.header("Connection");
    r.keep_alive = http10 ? has_token(connection, "keep-alive") : !has_token(connection, "close");
    res_ = std::move(r);

    if (status == 204 || status == 304) {
        phase_ = Phase::Done;
        return state_;
    }
    if (const std::string te = res_.header("Transfer-Encoding"); !te.empty()) {
        if (!iequals(trim(te), "chunked"))
            return fail("unsupported transfer coding: " + te);
        phase_ = Phase::ChunkSize;
        return state_;
    }
    if (const std::string cl = res_.header("Content-Length"); !cl.empty()) {
        std::size_t n = 0;
        const auto v = trim(cl);
        const auto rc = std::from_chars(v.data(), v.data() + v.size(), n);
        if (rc.ec != std::errc{} || rc.ptr != v.data() + v.size())
            return fail("bad Content-Length: " + cl);
        if (n > max_body_)
            return fail("body of " + std::to_string(n) + " bytes is over the " + std::to_string(max_body_) +
                        "-byte limit");
        remaining_ = n;
        phase_ = n == 0 ? Phase::Done : Phase::Fixed;
        return state_;
    }
    res_.keep_alive = false; // the end of the connection ends the body
    phase_ = Phase::UntilEof;
    return state_;
}

ResponseParser::State ResponseParser::feed(std::string_view bytes) {
    if (state_ == State::Error)
        return state_;
    if (state_ == State::Complete)
        return bytes.empty() ? state_ : fail("data after the response");
    buf_.append(bytes);

    for (;;) {
        switch (phase_) {
            case Phase::Head: {
                const std::size_t end = buf_.find("\r\n\r\n");
                if (end == std::string::npos)
                    return buf_.size() > kMaxHead ? fail("response head over 32 KiB") : state_;
                const std::string head = buf_.substr(0, end);
                buf_.erase(0, end + 4);
                if (parse_head(head) == State::Error)
                    return state_;
                break;
            }
            case Phase::Fixed:
            case Phase::ChunkData: {
                const std::size_t n = std::min(remaining_, buf_.size());
                res_.body.append(buf_, 0, n);
                buf_.erase(0, n);
                remaining_ -= n;
                if (remaining_ > 0)
                    return state_;
                phase_ = phase_ == Phase::Fixed ? Phase::Done : Phase::ChunkDataEnd;
                break;
            }
            case Phase::ChunkSize: {
                const std::size_t eol = buf_.find("\r\n");
                if (eol == std::string::npos)
                    return buf_.size() > kMaxLine ? fail("chunk-size line too long") : state_;
                std::string_view line(buf_.data(), eol);
                line = trim(line.substr(0, line.find(';'))); // drop chunk extensions
                std::size_t size = 0;
                const auto rc = std::from_chars(line.data(), line.data() + line.size(), size, 16);
                if (line.empty() || rc.ec != std::errc{} || rc.ptr != line.data() + line.size())
                    return fail("bad chunk size: " + std::string(line.substr(0, 20)));
                buf_.erase(0, eol + 2);
                if (size == 0) {
                    phase_ = Phase::Trailers;
                } else {
                    if (size > max_body_ - std::min(res_.body.size(), max_body_))
                        return fail("chunked body over the " + std::to_string(max_body_) + "-byte limit");
                    remaining_ = size;
                    phase_ = Phase::ChunkData;
                }
                break;
            }
            case Phase::ChunkDataEnd:
                if (buf_.size() < 2)
                    return state_;
                if (buf_[0] != '\r' || buf_[1] != '\n')
                    return fail("chunk data not followed by CRLF");
                buf_.erase(0, 2);
                phase_ = Phase::ChunkSize;
                break;
            case Phase::Trailers: {
                const std::size_t eol = buf_.find("\r\n");
                if (eol == std::string::npos)
                    return buf_.size() > kMaxLine ? fail("trailer line too long") : state_;
                const bool last = eol == 0;
                buf_.erase(0, eol + 2);
                if (last)
                    phase_ = Phase::Done;
                break;
            }
            case Phase::UntilEof:
                res_.body += buf_;
                buf_.clear();
                return res_.body.size() > max_body_ ? fail("body over the limit") : state_;
            case Phase::Done:
                if (!buf_.empty())
                    return fail("data after the response");
                state_ = State::Complete;
                return state_;
        }
    }
}

ResponseParser::State ResponseParser::finish_at_eof() {
    if (state_ != State::Incomplete)
        return state_;
    if (phase_ == Phase::UntilEof || phase_ == Phase::Done) {
        state_ = State::Complete;
        return state_;
    }
    return fail("connection closed before the response was complete");
}

} // namespace fmdg::http
