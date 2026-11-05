// gateway/include/fmdg/http.h
#pragma once
// The HTTP/1.1 client side the REST sync needs: a GET request builder and an
// incremental response parser. Pure functions over bytes, unit-tested in
// tests/test_http.cpp; the connection that drives them is http_client.h.

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace fmdg::http {

using Headers = std::vector<std::pair<std::string, std::string>>;

struct Response {
    int status = 0;
    std::string reason;
    Headers headers;
    std::string body;
    bool keep_alive = true; ///< false after "Connection: close", or for HTTP/1.0 without keep-alive

    /// First header with this name (case-insensitive), or "".
    std::string header(std::string_view name) const;
};

/// "GET <target> HTTP/1.1" with Host, the given headers, and no body.
std::string get_request(std::string_view host, std::string_view target, const Headers& headers);

/// Percent-encodes a query-string value (RFC 3986 unreserved characters pass).
std::string url_encode(std::string_view s);

/// One response, fed bytes as they arrive (split anywhere). The body is framed
/// by Transfer-Encoding: chunked, else Content-Length, else the end of the
/// connection (call finish_at_eof()). Interim 1xx responses are skipped;
/// 204/304 have no body. Head and body sizes are capped, so a hostile server
/// cannot make it buffer without bound.
class ResponseParser {
  public:
    enum class State { Incomplete, Complete, Error };

    explicit ResponseParser(std::size_t max_body = std::size_t{64} << 20);

    State feed(std::string_view bytes);
    /// The connection closed: completes a body framed by the connection's end;
    /// any other unfinished response becomes an error.
    State finish_at_eof();

    State state() const { return state_; }
    const std::string& error() const { return error_; }
    const Response& response() const { return res_; }
    Response take() { return std::move(res_); }

  private:
    enum class Phase { Head, Fixed, ChunkSize, ChunkData, ChunkDataEnd, Trailers, UntilEof, Done };
    State fail(std::string why);
    State parse_head(std::string_view head);

    Phase phase_ = Phase::Head;
    State state_ = State::Incomplete;
    std::string buf_;
    std::size_t remaining_ = 0; ///< body bytes left (Fixed) or chunk bytes left (ChunkData)
    std::size_t max_body_;
    Response res_;
    std::string error_;
};

} // namespace fmdg::http
