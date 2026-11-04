// gateway/tests/test_ws_codec.cpp — RFC 6455 client codec (fmdg/ws_codec.h).
// Known-answer cases come straight from the RFC (§1.3 handshake key, §5.7 frame
// examples); the rest pin the rules a client must enforce on server frames.

#include "fmdg/ws_codec.h"

#include <gtest/gtest.h>

#include <random>
#include <string>
#include <vector>

using namespace fmdg::ws;

namespace {

// A server-to-client frame: never masked.
std::string server_frame(std::uint8_t first_byte, std::string_view payload) {
    std::string f(1, static_cast<char>(first_byte));
    const std::size_t n = payload.size();
    if (n < 126) {
        f.push_back(static_cast<char>(n));
    } else if (n <= 0xFFFF) {
        f.push_back(static_cast<char>(126));
        f.push_back(static_cast<char>(n >> 8));
        f.push_back(static_cast<char>(n & 0xFF));
    } else {
        f.push_back(static_cast<char>(127));
        for (int shift = 56; shift >= 0; shift -= 8)
            f.push_back(static_cast<char>((static_cast<std::uint64_t>(n) >> shift) & 0xFF));
    }
    f.append(payload);
    return f;
}

std::string bytes(std::initializer_list<int> b) {
    std::string s;
    for (int x : b)
        s.push_back(static_cast<char>(x));
    return s;
}

std::vector<Event> decode_all(std::string_view wire, FrameParser& p) {
    std::vector<Event> out;
    EXPECT_TRUE(p.feed(wire, out)) << p.error();
    return out;
}

std::string decode_error(std::string_view wire) {
    FrameParser p;
    std::vector<Event> out;
    EXPECT_FALSE(p.feed(wire, out));
    return p.error();
}

const std::string kRfcKey = "dGhlIHNhbXBsZSBub25jZQ=="; // RFC 6455 §1.3

std::string accepted_head(std::string_view accept) {
    return "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
           "Sec-WebSocket-Accept: " +
           std::string(accept) + "\r\n\r\n";
}

} // namespace

// ── handshake ────────────────────────────────────────────────────────────────

TEST(WsHandshake, KeyAndAcceptMatchTheRfcExample) {
    const std::array<std::uint8_t, 16> nonce{'t', 'h', 'e', ' ', 's', 'a', 'm', 'p',
                                             'l', 'e', ' ', 'n', 'o', 'n', 'c', 'e'};
    EXPECT_EQ(client_key(nonce), kRfcKey);
    EXPECT_EQ(accept_key(kRfcKey), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=");
}

TEST(WsHandshake, RequestCarriesTheRequiredHeaders) {
    const std::string r = upgrade_request("stream.data.alpaca.markets", "/v2/iex", kRfcKey);
    EXPECT_EQ(r.rfind("GET /v2/iex HTTP/1.1\r\n", 0), 0u);
    EXPECT_NE(r.find("\r\nHost: stream.data.alpaca.markets\r\n"), std::string::npos);
    EXPECT_NE(r.find("\r\nUpgrade: websocket\r\n"), std::string::npos);
    EXPECT_NE(r.find("\r\nConnection: Upgrade\r\n"), std::string::npos);
    EXPECT_NE(r.find("\r\nSec-WebSocket-Key: " + kRfcKey + "\r\n"), std::string::npos);
    EXPECT_NE(r.find("\r\nSec-WebSocket-Version: 13\r\n"), std::string::npos);
    EXPECT_EQ(r.substr(r.size() - 4), "\r\n\r\n");
}

TEST(WsHandshake, AcceptsAValidResponse) {
    EXPECT_EQ(check_upgrade_response(accepted_head("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="), kRfcKey), "");
    // Header names are case-insensitive; Connection may list several tokens.
    EXPECT_EQ(check_upgrade_response("HTTP/1.1 101 Switching Protocols\r\nupgrade: WebSocket\r\n"
                                     "CONNECTION: keep-alive, Upgrade\r\n"
                                     "sec-websocket-accept:  s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n",
                                     kRfcKey),
              "");
}

TEST(WsHandshake, RejectsWhatItMust) {
    EXPECT_EQ(check_upgrade_response("HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n", kRfcKey),
              "handshake refused: HTTP/1.1 401 Unauthorized");
    EXPECT_NE(check_upgrade_response(accepted_head("AAAAAAAAAAAAAAAAAAAAAAAAAAA="), kRfcKey).find("does not match"),
              std::string::npos);
    EXPECT_NE(check_upgrade_response("HTTP/1.1 101 OK\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                                     "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n",
                                     kRfcKey)
                  .find("Upgrade: websocket"),
              std::string::npos);
    EXPECT_NE(check_upgrade_response("HTTP/1.1 101 OK\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
                                     "Sec-WebSocket-Extensions: permessage-deflate\r\n"
                                     "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n",
                                     kRfcKey)
                  .find("extension"),
              std::string::npos);
}

// ── encoding (client → server) ───────────────────────────────────────────────

TEST(WsEncode, MaskedHelloMatchesTheRfcExample) {
    // §5.7: a single-frame masked text message containing "Hello".
    EXPECT_EQ(encode_frame(Opcode::Text, "Hello", {0x37, 0xfa, 0x21, 0x3d}),
              bytes({0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58}));
}

TEST(WsEncode, UsesTheThreeLengthEncodings) {
    const std::array<std::uint8_t, 4> mask{1, 2, 3, 4};
    struct Case {
        std::size_t n;
        std::size_t header; // bytes before the mask key
        int len7;
    };
    for (const Case c : {Case{0, 2, 0}, Case{125, 2, 125}, Case{126, 4, 126}, Case{65535, 4, 126},
                         Case{65536, 10, 127}}) {
        const std::string payload(c.n, 'x');
        const std::string f = encode_frame(Opcode::Binary, payload, mask);
        ASSERT_EQ(f.size(), c.header + 4 + c.n) << c.n;
        EXPECT_EQ(static_cast<std::uint8_t>(f[0]), 0x82);
        EXPECT_EQ(static_cast<std::uint8_t>(f[1]), 0x80 | c.len7) << c.n;
        // Unmasking gives the payload back.
        for (std::size_t i = 0; i < c.n; ++i)
            ASSERT_EQ(static_cast<char>(static_cast<std::uint8_t>(f[c.header + 4 + i]) ^ mask[i % 4]), 'x');
    }
}

TEST(WsEncode, ClosePayloadIsBigEndianCodeThenReason) {
    EXPECT_EQ(close_payload(1000, "bye"), bytes({0x03, 0xE8, 'b', 'y', 'e'}));
    EXPECT_EQ(close_payload(1001, std::string(200, 'r')).size(), 125u);
}

// ── decoding (server → client) ───────────────────────────────────────────────

TEST(WsDecode, UnmaskedHelloFromTheRfc) {
    FrameParser p;
    const auto ev = decode_all(bytes({0x81, 0x05, 'H', 'e', 'l', 'l', 'o'}), p);
    ASSERT_EQ(ev.size(), 1u);
    EXPECT_EQ(ev[0].kind, Event::Kind::Text);
    EXPECT_EQ(ev[0].payload, "Hello");
    EXPECT_EQ(p.buffered(), 0u);
}

TEST(WsDecode, FragmentsJoinAndControlFramesInterleave) {
    // §5.7: "Hel" + "lo" as two fragments, with a ping between them.
    FrameParser p;
    const auto ev = decode_all(bytes({0x01, 0x03, 'H', 'e', 'l'}) + bytes({0x89, 0x02, 'h', 'i'}) +
                                   bytes({0x80, 0x02, 'l', 'o'}),
                               p);
    ASSERT_EQ(ev.size(), 2u);
    EXPECT_EQ(ev[0].kind, Event::Kind::Ping);
    EXPECT_EQ(ev[0].payload, "hi");
    EXPECT_EQ(ev[1].kind, Event::Kind::Text);
    EXPECT_EQ(ev[1].payload, "Hello");
}

TEST(WsDecode, AnySplitOfTheStreamGivesTheSameEvents) {
    std::string wire = server_frame(0x81, R"([{"T":"success","msg":"connected"}])");
    wire += server_frame(0x82, std::string(300, 'b'));    // 16-bit length
    wire += server_frame(0x81, std::string(70'000, 't')); // 64-bit length
    wire += server_frame(0x8A, "pong");
    wire += server_frame(0x02, "part1-") + server_frame(0x80, "part2");
    wire += server_frame(0x88, bytes({0x03, 0xE8}) + "done");

    FrameParser whole;
    const auto expected = decode_all(wire, whole);
    ASSERT_EQ(expected.size(), 6u);

    std::mt19937 rng(5);
    for (int trial = 0; trial < 20; ++trial) {
        FrameParser p;
        std::vector<Event> got;
        std::size_t at = 0;
        while (at < wire.size()) {
            // Mostly tiny chunks (down to one byte), sometimes big ones.
            const std::size_t chunk =
                trial == 0 ? 1 : std::uniform_int_distribution<std::size_t>(1, trial % 2 ? 7 : 9000)(rng);
            ASSERT_TRUE(p.feed(std::string_view(wire).substr(at, chunk), got)) << p.error();
            at += chunk;
        }
        ASSERT_EQ(got.size(), expected.size()) << "trial " << trial;
        for (std::size_t i = 0; i < got.size(); ++i) {
            EXPECT_EQ(got[i].kind, expected[i].kind);
            EXPECT_EQ(got[i].payload, expected[i].payload);
            EXPECT_EQ(got[i].close_code, expected[i].close_code);
        }
        EXPECT_EQ(p.buffered(), 0u);
    }
    EXPECT_EQ(expected[2].payload.size(), 70'000u);
    EXPECT_EQ(expected[4].payload, "part1-part2");
    EXPECT_EQ(expected[5].close_code, 1000);
    EXPECT_EQ(expected[5].payload, "done");
}

TEST(WsDecode, CloseWithoutStatusIs1005) {
    FrameParser p;
    const auto ev = decode_all(bytes({0x88, 0x00}), p);
    ASSERT_EQ(ev.size(), 1u);
    EXPECT_EQ(ev[0].kind, Event::Kind::Close);
    EXPECT_EQ(ev[0].close_code, 1005);
}

TEST(WsDecode, RejectsProtocolViolations) {
    EXPECT_EQ(decode_error(bytes({0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x7f, 0x9f, 0x4d, 0x51, 0x58})),
              "server frame is masked");
    EXPECT_EQ(decode_error(bytes({0xC1, 0x01, 'x'})), "RSV bits set, but no extension was negotiated");
    EXPECT_EQ(decode_error(bytes({0x83, 0x00})), "unknown data opcode 3");
    EXPECT_EQ(decode_error(bytes({0x8B, 0x00})), "unknown control opcode 11");
    EXPECT_EQ(decode_error(bytes({0x09, 0x00})), "fragmented control frame");
    EXPECT_EQ(decode_error(bytes({0x89, 0x7E, 0x00, 0x7E})), "control frame longer than 125 bytes");
    EXPECT_EQ(decode_error(bytes({0x80, 0x01, 'x'})), "continuation frame outside a fragmented message");
    EXPECT_EQ(decode_error(bytes({0x01, 0x01, 'x', 0x81, 0x01, 'y'})), "new data frame inside a fragmented message");
    EXPECT_EQ(decode_error(bytes({0x88, 0x01, 0x03})), "close frame with a one-byte payload");
    EXPECT_EQ(decode_error(bytes({0x82, 0x7F, 0x80, 0, 0, 0, 0, 0, 0, 0})),
              "frame length has its most significant bit set");
}

TEST(WsDecode, OversizedMessageFailsFromTheHeaderAlone) {
    // A 1 TiB length: refused as soon as the 10 header bytes are in, without
    // waiting for (or buffering) any payload.
    FrameParser p(1 << 20);
    std::vector<Event> out;
    EXPECT_FALSE(p.feed(bytes({0x82, 0x7F, 0, 0, 0x01, 0, 0, 0, 0, 0}), out));
    EXPECT_NE(p.error().find("larger than"), std::string::npos);
    // The cap covers the joined fragments, not just one frame.
    FrameParser q(10);
    EXPECT_FALSE(q.feed(server_frame(0x01, "123456") + server_frame(0x80, "78901"), out));
    EXPECT_NE(q.error().find("larger than"), std::string::npos);
}

TEST(WsDecode, StaysFailedAfterAnError) {
    FrameParser p;
    std::vector<Event> out;
    EXPECT_FALSE(p.feed(bytes({0x83, 0x00}), out));
    EXPECT_FALSE(p.feed(server_frame(0x81, "fine"), out));
    EXPECT_TRUE(out.empty());
}
