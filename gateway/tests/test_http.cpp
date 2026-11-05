// gateway/tests/test_http.cpp — the HTTP/1.1 response parser and request
// builder (fmdg/http.h): each body framing, fed whole and split anywhere, and
// the malformed or hostile responses it must refuse.

#include "fmdg/http.h"

#include <gtest/gtest.h>

#include <random>
#include <string>

using namespace fmdg::http;
using State = ResponseParser::State;

namespace {

// Feeds `wire` in random pieces (one byte at a time on trial 0).
Response parse_split(const std::string& wire, bool eof_framed, int trials = 20) {
    std::mt19937 rng(3);
    Response first;
    for (int t = 0; t < trials; ++t) {
        ResponseParser p;
        std::size_t at = 0;
        State s = State::Incomplete;
        while (at < wire.size()) {
            const std::size_t n = t == 0 ? 1 : std::uniform_int_distribution<std::size_t>(1, 40)(rng);
            s = p.feed(std::string_view(wire).substr(at, n));
            EXPECT_NE(s, State::Error) << p.error();
            at += n;
        }
        if (eof_framed)
            s = p.finish_at_eof();
        EXPECT_EQ(s, State::Complete) << "trial " << t;
        Response r = p.take();
        if (t == 0)
            first = r;
        EXPECT_EQ(r.body, first.body) << "trial " << t;
    }
    return first;
}

std::string error_of(const std::string& wire, std::size_t max_body = 1 << 20) {
    ResponseParser p(max_body);
    State s = p.feed(wire);
    if (s == State::Incomplete)
        s = p.finish_at_eof();
    EXPECT_EQ(s, State::Error) << wire.substr(0, 60);
    return p.error();
}

} // namespace

TEST(Http, BuildsAGetRequest) {
    EXPECT_EQ(get_request("data.alpaca.markets", "/v2/stocks/bars?symbols=AAPL",
                          {{"Accept", "application/json"}, {"APCA-API-KEY-ID", "k"}}),
              "GET /v2/stocks/bars?symbols=AAPL HTTP/1.1\r\nHost: data.alpaca.markets\r\n"
              "Accept: application/json\r\nAPCA-API-KEY-ID: k\r\n\r\n");
    EXPECT_EQ(url_encode("AAPL,BRK.B 2026-11-04T19:15:00Z/+="), "AAPL%2CBRK.B%202026-11-04T19%3A15%3A00Z%2F%2B%3D");
}

TEST(Http, ContentLengthBody) {
    const Response r = parse_split("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\ncontent-length: 11\r\n\r\n"
                                   "{\"bars\":{}}",
                                   false);
    EXPECT_EQ(r.status, 200);
    EXPECT_EQ(r.reason, "OK");
    EXPECT_EQ(r.body, "{\"bars\":{}}");
    EXPECT_EQ(r.header("CONTENT-TYPE"), "application/json");
    EXPECT_TRUE(r.keep_alive);
}

TEST(Http, ChunkedBodyWithExtensionsAndTrailers) {
    const Response r = parse_split("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
                                   "4\r\nWiki\r\n5;name=value\r\npedia\r\nE\r\n in\r\n\r\nchunks.\r\n"
                                   "0\r\nX-Trailer: 1\r\n\r\n",
                                   false);
    EXPECT_EQ(r.body, "Wikipedia in\r\n\r\nchunks.");
}

TEST(Http, BodyEndingWithTheConnection) {
    const Response r = parse_split("HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\n\r\nhello, world", true);
    EXPECT_EQ(r.body, "hello, world");
    EXPECT_FALSE(r.keep_alive); // the connection is gone
}

TEST(Http, NoBodyInterimResponsesAndConnectionHeaders) {
    ResponseParser a;
    EXPECT_EQ(a.feed("HTTP/1.1 204 No Content\r\nX: y\r\n\r\n"), State::Complete);
    ResponseParser b;
    EXPECT_EQ(b.feed("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok"), State::Complete);
    EXPECT_EQ(b.response().body, "ok");
    ResponseParser c;
    EXPECT_EQ(c.feed("HTTP/1.1 429 Too Many Requests\r\nConnection: close\r\nRetry-After: 3\r\n"
                     "Content-Length: 0\r\n\r\n"),
              State::Complete);
    EXPECT_EQ(c.response().status, 429);
    EXPECT_FALSE(c.response().keep_alive);
    EXPECT_EQ(c.response().header("retry-after"), "3");
    ResponseParser d;
    EXPECT_EQ(d.feed("HTTP/1.0 200 OK\r\nContent-Length: 0\r\n\r\n"), State::Complete);
    EXPECT_FALSE(d.response().keep_alive); // HTTP/1.0 closes unless asked otherwise
}

TEST(Http, RefusesMalformedOrHostileResponses) {
    EXPECT_NE(error_of("SMTP 220 ready\r\n\r\n").find("bad status line"), std::string::npos);
    EXPECT_NE(error_of("HTTP/1.1 2x0 OK\r\n\r\n").find("bad status code"), std::string::npos);
    EXPECT_NE(error_of("HTTP/1.1 200 OK\r\nno colon here\r\n\r\n").find("bad header"), std::string::npos);
    EXPECT_NE(error_of("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n").find("bad chunk size"),
              std::string::npos);
    EXPECT_NE(error_of("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n2\r\nabXX").find("not followed by CRLF"),
              std::string::npos);
    EXPECT_NE(error_of("HTTP/1.1 200 OK\r\nTransfer-Encoding: gzip\r\n\r\n").find("unsupported transfer coding"),
              std::string::npos);
    // Over the limit is refused from the header alone, before any body arrives.
    EXPECT_NE(error_of("HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n").find("limit"), std::string::npos);
    EXPECT_NE(error_of("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nFFFFFFF\r\n").find("limit"),
              std::string::npos);
    EXPECT_NE(error_of("HTTP/1.1 200 OK\r\n" + std::string(40000, 'x')).find("head over"), std::string::npos);
    // Cut short by the connection closing.
    EXPECT_NE(error_of("HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc").find("closed before"), std::string::npos);
    // We never pipeline, so bytes past the response are an error.
    EXPECT_NE(error_of("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nokEXTRA").find("data after"), std::string::npos);
}
