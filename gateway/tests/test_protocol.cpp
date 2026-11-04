// gateway/tests/test_protocol.cpp
#include "fmdg/protocol.h"

#include <gtest/gtest.h>

#include <random>
#include <stdexcept>
#include <string>
#include <vector>

using namespace fmdg;
using namespace fmdg::proto;

namespace {

/// Splits a buffer of whole frames back into (header, body) pairs.
std::vector<std::pair<Header, Bytes>> frames_of(const Bytes& wire) {
    FrameReader r;
    r.feed(wire.data(), wire.size());
    std::vector<std::pair<Header, Bytes>> out;
    Header h;
    std::span<const std::uint8_t> body;
    while (r.next(h, body) == FrameReader::Result::Frame)
        out.emplace_back(h, Bytes(body.begin(), body.end()));
    EXPECT_EQ(r.buffered(), 0u);
    return out;
}

} // namespace

TEST(Protocol, SymbolValidity) {
    EXPECT_TRUE(valid_symbol("AAPL"));
    EXPECT_TRUE(valid_symbol("BRK.B"));
    EXPECT_TRUE(valid_symbol("ES=F"));
    EXPECT_TRUE(valid_symbol(std::string(kMaxSymbolLen, 'X')));
    EXPECT_FALSE(valid_symbol(""));
    EXPECT_FALSE(valid_symbol(std::string(kMaxSymbolLen + 1, 'X')));
    EXPECT_FALSE(valid_symbol("A B"));
    EXPECT_FALSE(valid_symbol("A\nB"));
    EXPECT_FALSE(valid_symbol("\x7f"));
    EXPECT_FALSE(valid_symbol("\xc3\xa9")); // non-ASCII
}

TEST(Protocol, SubscribeRoundTrip) {
    Bytes wire;
    append_symbols(wire, MsgType::Subscribe, {"AAPL", "MSFT", "BRK.B"});
    const auto frames = frames_of(wire);
    ASSERT_EQ(frames.size(), 1u);
    EXPECT_EQ(frames[0].first.type, MsgType::Subscribe);
    const auto syms = parse_symbols(frames[0].second);
    ASSERT_TRUE(syms);
    EXPECT_EQ(*syms, (std::vector<std::string>{"AAPL", "MSFT", "BRK.B"}));
}

TEST(Protocol, AppendSymbolsRejectsClientBugs) {
    Bytes wire;
    EXPECT_THROW(append_symbols(wire, MsgType::Subscribe, {"bad symbol"}), std::invalid_argument);
    EXPECT_THROW(append_symbols(wire, MsgType::Tick, {"AAPL"}), std::invalid_argument);
    EXPECT_THROW(append_symbols(wire, MsgType::Subscribe, std::vector<std::string>(kMaxSymbolsPerRequest + 1, "A")),
                 std::invalid_argument);
    EXPECT_TRUE(wire.empty()) << "nothing may be appended when encoding fails";
}

TEST(Protocol, SubscribedRoundTrip) {
    Bytes wire;
    append_subscribed(wire, {{0, "AAPL"}, {7, "MSFT"}, {4'000'000'000u, "X"}});
    const auto frames = frames_of(wire);
    ASSERT_EQ(frames.size(), 1u);
    EXPECT_EQ(frames[0].first.type, MsgType::Subscribed);
    const auto ids = parse_subscribed(frames[0].second);
    ASSERT_TRUE(ids);
    ASSERT_EQ(ids->size(), 3u);
    EXPECT_EQ((*ids)[1], (std::pair<SymbolId, std::string>{7, "MSFT"}));
    EXPECT_EQ((*ids)[2].first, 4'000'000'000u);
}

TEST(Protocol, TickRoundTripIsBitExact) {
    Tick t;
    t.symbol = 123456;
    t.sequence = 0xFEDCBA9876543210ull;
    t.price = 189.2549999999999;
    t.size = 1e-7;
    t.exchange_ts_ns = 1'700'000'000'123'456'789;
    t.ingest_ts_ns = -42; // signedness survives
    Bytes wire;
    append_tick(wire, t, 987654321);
    ASSERT_EQ(wire.size(), kHeaderSize + kTickBodySize);
    const auto frames = frames_of(wire);
    ASSERT_EQ(frames.size(), 1u);
    const auto m = parse_tick(frames[0].second);
    ASSERT_TRUE(m);
    EXPECT_EQ(m->tick.symbol, t.symbol);
    EXPECT_EQ(m->tick.sequence, t.sequence);
    EXPECT_EQ(m->tick.price, t.price);
    EXPECT_EQ(m->tick.size, t.size);
    EXPECT_EQ(m->tick.exchange_ts_ns, t.exchange_ts_ns);
    EXPECT_EQ(m->tick.ingest_ts_ns, t.ingest_ts_ns);
    EXPECT_EQ(m->send_ts_ns, 987654321);
}

TEST(Protocol, SnapshotFlagTravelsInTheHeader) {
    Tick t;
    t.flags = kTickSnapshot;
    Bytes wire;
    append_tick(wire, t, 0);
    t.flags = 0;
    append_tick(wire, t, 0);
    const auto frames = frames_of(wire);
    ASSERT_EQ(frames.size(), 2u);
    EXPECT_EQ(frames[0].first.flags, kFlagSnapshot);
    EXPECT_EQ(frames[1].first.flags, 0);
    EXPECT_EQ(wire[6], 1) << "flags are header bytes 6-7";
}

TEST(Protocol, WireFormatIsLittleEndian) {
    Bytes wire;
    append_heartbeat(wire);
    EXPECT_EQ(wire, (Bytes{0, 0, 0, 0, 5, 0, 0, 0}));
    wire.clear();
    append_symbols(wire, MsgType::Subscribe, {"AB"});
    EXPECT_EQ(wire, (Bytes{5, 0, 0, 0, 1, 0, 0, 0, /*n*/ 1, 0, /*len*/ 2, 'A', 'B'}));
}

TEST(Protocol, ErrorRoundTripAndTruncation) {
    Bytes wire;
    append_error(wire, ErrorCode::BadSymbol, std::string(1000, 'e'));
    const auto frames = frames_of(wire);
    ASSERT_EQ(frames.size(), 1u);
    const auto e = parse_error(frames[0].second);
    ASSERT_TRUE(e);
    EXPECT_EQ(e->code, ErrorCode::BadSymbol);
    EXPECT_EQ(e->message.size(), 255u);
}

TEST(Protocol, HeaderRejectsUnknownTypesAndHugeBodies) {
    Header h;
    const std::uint8_t ok[8] = {0, 0, 0, 0, 5, 0, 0, 0};
    EXPECT_TRUE(parse_header(ok, h));
    const std::uint8_t type0[8] = {0, 0, 0, 0, 0, 0, 0, 0};
    EXPECT_FALSE(parse_header(type0, h));
    const std::uint8_t type7[8] = {0, 0, 0, 0, 7, 0, 0, 0};
    EXPECT_FALSE(parse_header(type7, h));
    const std::uint8_t huge[8] = {1, 0, 1, 0, 1, 0, 0, 0}; // 65537 > kMaxBody
    EXPECT_FALSE(parse_header(huge, h));
    const std::uint8_t max_ok[8] = {0, 0, 1, 0, 1, 0, 0, 0}; // exactly kMaxBody
    EXPECT_TRUE(parse_header(max_ok, h));
}

TEST(Protocol, ParsersRejectMalformedBodies) {
    // Count says 2 symbols, only one present.
    EXPECT_FALSE(parse_symbols(Bytes{2, 0, 1, 'A'}));
    // Length byte runs past the end.
    EXPECT_FALSE(parse_symbols(Bytes{1, 0, 5, 'A', 'B'}));
    // Trailing garbage.
    EXPECT_FALSE(parse_symbols(Bytes{1, 0, 1, 'A', 'Z'}));
    // Too many, even if the bytes would be there.
    EXPECT_FALSE(parse_symbols(Bytes{0x01, 0x04})); // 1025
    EXPECT_FALSE(parse_symbols(Bytes{}));
    EXPECT_TRUE(parse_symbols(Bytes{0, 0})) << "an empty list is well-formed";
    EXPECT_FALSE(parse_tick(Bytes(kTickBodySize - 1, 0)));
    EXPECT_FALSE(parse_tick(Bytes(kTickBodySize + 1, 0)));
    EXPECT_FALSE(parse_error(Bytes{1, 0, 3, 'a'}));
    EXPECT_FALSE(parse_subscribed(Bytes{1, 0, 0, 0, 0}));
}

// A stream reader receives arbitrary chunks. Feeding a multi-frame stream one
// byte at a time must produce exactly the same frames as feeding it whole.
TEST(Protocol, FrameReaderHandlesAnyChunking) {
    Bytes wire;
    append_symbols(wire, MsgType::Subscribe, {"AAPL", "MSFT"});
    for (int i = 0; i < 50; ++i) {
        Tick t;
        t.symbol = static_cast<SymbolId>(i);
        t.sequence = static_cast<std::uint64_t>(i);
        append_tick(wire, t, i);
    }
    append_heartbeat(wire);
    append_error(wire, ErrorCode::BadFrame, "bye");
    const auto whole = frames_of(wire);
    ASSERT_EQ(whole.size(), 53u);

    std::mt19937 rng(3);
    for (int trial = 0; trial < 20; ++trial) {
        FrameReader r;
        std::vector<std::pair<Header, Bytes>> got;
        std::size_t pos = 0;
        while (pos < wire.size()) {
            const std::size_t n = std::min<std::size_t>(wire.size() - pos, trial == 0 ? 1 : 1 + rng() % 97);
            r.feed(wire.data() + pos, n);
            pos += n;
            Header h;
            std::span<const std::uint8_t> body;
            while (r.next(h, body) == FrameReader::Result::Frame)
                got.emplace_back(h, Bytes(body.begin(), body.end()));
        }
        ASSERT_EQ(got.size(), whole.size()) << "trial " << trial;
        for (std::size_t i = 0; i < got.size(); ++i) {
            ASSERT_EQ(got[i].first.type, whole[i].first.type);
            ASSERT_EQ(got[i].second, whole[i].second);
        }
        EXPECT_EQ(r.buffered(), 0u);
    }
}

TEST(Protocol, FrameReaderFlagsABadHeader) {
    FrameReader r;
    const std::uint8_t junk[8] = {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0, 0};
    r.feed(junk, sizeof junk);
    Header h;
    std::span<const std::uint8_t> body;
    EXPECT_EQ(r.next(h, body), FrameReader::Result::Bad);
}

// Random bytes must never crash a parser or make it read out of bounds (run
// under ASan/a debug CRT this also catches over-reads).
TEST(Protocol, FuzzedInputNeverCrashes) {
    std::mt19937 rng(99);
    for (int i = 0; i < 20'000; ++i) {
        Bytes b(rng() % 80);
        for (auto& x : b)
            x = static_cast<std::uint8_t>(rng() % 6 == 0 ? rng() % 4 : rng());
        (void)parse_symbols(b);
        (void)parse_subscribed(b);
        (void)parse_tick(b);
        (void)parse_error(b);
        FrameReader r;
        r.feed(b.data(), b.size());
        Header h;
        std::span<const std::uint8_t> body;
        int guard = 0;
        while (r.next(h, body) == FrameReader::Result::Frame && ++guard < 100) {
        }
    }
    SUCCEED();
}
