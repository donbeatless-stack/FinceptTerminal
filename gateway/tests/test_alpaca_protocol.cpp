// gateway/tests/test_alpaca_protocol.cpp — Alpaca stream messages
// (fmdg/alpaca_protocol.h). Sample messages are the ones in Alpaca's
// "Real-time Stock Pricing Data" docs; timestamps are checked against
// std::chrono's calendar, an independent implementation.

#include "fmdg/alpaca_protocol.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace fmdg::alpaca;
using namespace std::chrono;

namespace {

std::int64_t utc_ns(year_month_day ymd, hours h, minutes m, seconds s, nanoseconds frac = nanoseconds{0}) {
    return duration_cast<nanoseconds>((sys_days{ymd} + h + m + s).time_since_epoch()).count() + frac.count();
}

} // namespace

// ── timestamps ───────────────────────────────────────────────────────────────

TEST(AlpacaTime, ParsesUtcWithNanoseconds) {
    EXPECT_EQ(parse_rfc3339_ns("1970-01-01T00:00:00Z"), 0);
    EXPECT_EQ(parse_rfc3339_ns("2026-11-04T19:15:02.123456789Z"),
              utc_ns(2026y / 11 / 4, 19h, 15min, 2s, 123456789ns));
    EXPECT_EQ(parse_rfc3339_ns("2021-02-22T15:51:44.208Z"), utc_ns(2021y / 2 / 22, 15h, 51min, 44s, 208ms));
    EXPECT_EQ(parse_rfc3339_ns("2026-11-04T19:15:02.5Z"), utc_ns(2026y / 11 / 4, 19h, 15min, 2s, 500ms));
    // Digits past nanoseconds are dropped, not rounded.
    EXPECT_EQ(parse_rfc3339_ns("2026-11-04T19:15:02.1234567899Z"),
              utc_ns(2026y / 11 / 4, 19h, 15min, 2s, 123456789ns));
    EXPECT_EQ(parse_rfc3339_ns("2028-02-29 00:00:00Z"), utc_ns(2028y / 2 / 29, 0h, 0min, 0s));
}

TEST(AlpacaTime, AppliesNumericOffsets) {
    EXPECT_EQ(parse_rfc3339_ns("2026-11-04T14:15:02-05:00"), parse_rfc3339_ns("2026-11-04T19:15:02Z"));
    EXPECT_EQ(parse_rfc3339_ns("2026-11-05T03:45:02+08:30"), parse_rfc3339_ns("2026-11-04T19:15:02Z"));
}

TEST(AlpacaTime, RejectsMalformedInput) {
    for (const char* bad : {"", "2026-11-04", "2026-11-04T19:15:02", "2026-11-04T19:15:02.Z", "2026-13-01T00:00:00Z",
                            "2026-02-29T00:00:00Z", "2026-11-31T00:00:00Z", "2026-11-04T24:00:00Z",
                            "2026-11-04T19:15:02Zjunk", "2026-11-04T19:15:02+0500", "2026/11/04T19:15:02Z",
                            "1600-01-01T00:00:00Z"})
        EXPECT_FALSE(parse_rfc3339_ns(bad).has_value()) << bad;
}

// ── outgoing ─────────────────────────────────────────────────────────────────

TEST(AlpacaOutgoing, AuthIsEscapedJson) {
    EXPECT_EQ(auth_message("KEY", "SECRET"), R"({"action":"auth","key":"KEY","secret":"SECRET"})");
    // Raw strings holding backslashes stay out of macro arguments: MSVC's
    // traditional preprocessor mangles them.
    const std::string escaped = R"({"action":"auth","key":"a\"b\\c","secret":"\u000a"})";
    EXPECT_EQ(auth_message("a\"b\\c", "\n"), escaped);
}

TEST(AlpacaOutgoing, SubscribeListsTradesAndOptionallyBars) {
    EXPECT_EQ(subscribe_message({"AAPL", "MSFT"}, false), R"({"action":"subscribe","trades":["AAPL","MSFT"]})");
    EXPECT_EQ(subscribe_message({"SPY"}, true),
              R"({"action":"subscribe","trades":["SPY"],"bars":["SPY"],"updatedBars":["SPY"]})");
    // A hostile "symbol" stays a string; it cannot add a channel.
    const std::string hostile = R"(X"],"quotes":["*)";
    const std::string contained = R"({"action":"subscribe","trades":["X\"],\"quotes\":[\"*"]})";
    EXPECT_EQ(subscribe_message({hostile}, false), contained);
}

// ── incoming ─────────────────────────────────────────────────────────────────

TEST(AlpacaIncoming, ControlMessages) {
    MessageParser p;
    Batch b;
    ASSERT_TRUE(p.parse(R"([{"T":"success","msg":"connected"}])", b));
    ASSERT_TRUE(p.parse(R"([{"T":"success","msg":"authenticated"}])", b));
    ASSERT_TRUE(p.parse(R"([{"T":"subscription","trades":["AAPL","MSFT"],"quotes":[],"bars":["AAPL"],)"
                        R"("updatedBars":[],"dailyBars":[],"statuses":[],"lulds":[],"corrections":["AAPL"],)"
                        R"("cancelErrors":["AAPL"]}])",
                        b));
    ASSERT_TRUE(p.parse(R"([{"T":"error","code":406,"msg":"connection limit exceeded"}])", b));
    ASSERT_EQ(b.control.size(), 4u);
    EXPECT_EQ(b.control[0].kind, Control::Kind::Connected);
    EXPECT_EQ(b.control[1].kind, Control::Kind::Authenticated);
    EXPECT_EQ(b.control[2].kind, Control::Kind::Subscription);
    EXPECT_EQ(b.control[2].message, "trades=[AAPL,MSFT] bars=[AAPL] corrections=[AAPL] cancelErrors=[AAPL]");
    EXPECT_EQ(b.control[3].kind, Control::Kind::Error);
    EXPECT_EQ(b.control[3].code, 406);
    EXPECT_EQ(b.control[3].message, "connection limit exceeded");
    EXPECT_EQ(b.malformed, 0u);
}

TEST(AlpacaIncoming, TradesAndBarsFromTheDocs) {
    MessageParser p;
    Batch b;
    ASSERT_TRUE(p.parse(
        R"([{"T":"t","i":96921,"S":"AAPL","x":"D","p":126.55,"s":1,"t":"2021-02-22T15:51:44.208Z","c":["@","I"],"z":"C"},)"
        R"({"T":"t","i":96922,"S":"AAPL","x":"D","p":126.56,"s":100,"t":"2021-02-22T15:51:44.209Z","c":["@"],"z":"C"},)"
        R"({"T":"b","S":"SPY","o":388.985,"h":389.13,"l":388.975,"c":389.12,"v":49378,"t":"2021-02-22T19:15:00Z","n":461,"vw":389.062639},)"
        R"({"T":"u","S":"SPY","o":388.985,"h":389.13,"l":388.975,"c":389.13,"v":49478,"t":"2021-02-22T19:15:00Z","n":462,"vw":389.06})"
        R"(])",
        b));
    ASSERT_EQ(b.trades.size(), 2u);
    EXPECT_EQ(b.trades[0].symbol, "AAPL");
    EXPECT_DOUBLE_EQ(b.trades[0].price, 126.55);
    EXPECT_DOUBLE_EQ(b.trades[1].size, 100.0); // JSON integer → double
    EXPECT_EQ(b.trades[0].ts_ns, utc_ns(2021y / 2 / 22, 15h, 51min, 44s, 208ms));
    ASSERT_EQ(b.bars.size(), 2u);
    EXPECT_EQ(b.bars[0].symbol, "SPY");
    EXPECT_FALSE(b.bars[0].updated);
    EXPECT_DOUBLE_EQ(b.bars[0].open, 388.985);
    EXPECT_DOUBLE_EQ(b.bars[0].close, 389.12);
    EXPECT_DOUBLE_EQ(b.bars[0].volume, 49378.0);
    EXPECT_EQ(b.bars[0].start_ns, utc_ns(2021y / 2 / 22, 19h, 15min, 0s));
    EXPECT_TRUE(b.bars[1].updated);
    EXPECT_DOUBLE_EQ(b.bars[1].close, 389.13);
}

TEST(AlpacaIncoming, OneBadItemDoesNotDropItsNeighbours) {
    MessageParser p;
    Batch b;
    ASSERT_TRUE(p.parse(R"([{"T":"q","S":"AMD","bp":87.66,"ap":87.68,"t":"2021-02-22T15:51:45.3352Z"},)"
                        R"({"T":"t","S":"AMD","p":"87.67","s":5,"t":"2021-02-22T15:51:45Z"},)" // price is a string
                        R"({"T":"t","S":"AMD","p":87.67,"s":5,"t":"yesterday"},)"           // bad time
                        R"(42,)"                                                              // not an object
                        R"({"S":"AMD"},)"                                                     // no type
                        R"({"T":"t","S":"AMD","p":87.67,"s":5,"t":"2021-02-22T15:51:45Z"}])",
                        b));
    EXPECT_EQ(b.other, 1u); // the quote
    EXPECT_EQ(b.malformed, 4u);
    ASSERT_EQ(b.trades.size(), 1u);
    EXPECT_DOUBLE_EQ(b.trades[0].price, 87.67);
}

TEST(AlpacaIncoming, RejectsNonArrays) {
    MessageParser p;
    Batch b;
    std::string err;
    EXPECT_FALSE(p.parse("not json", b, &err));
    EXPECT_FALSE(err.empty());
    EXPECT_FALSE(p.parse(R"({"T":"success","msg":"connected"})", b, &err));
    EXPECT_EQ(err, "not a JSON array");
    EXPECT_FALSE(p.parse("[{\"T\":\"t\",\"S\":\"\xff\"}]", b, &err)); // invalid UTF-8
    EXPECT_TRUE(b.control.empty());
    EXPECT_TRUE(b.trades.empty());
}

TEST(AlpacaIncoming, ErrorCodesMapToActions) {
    for (int code : {402, 409, 410})
        EXPECT_EQ(classify_error(code), ErrorAction::Stop) << code;
    EXPECT_EQ(classify_error(406), ErrorAction::ReconnectSlowly);
    for (int code : {400, 403, 405})
        EXPECT_EQ(classify_error(code), ErrorAction::Ignore) << code;
    for (int code : {401, 404, 407, 500, 999})
        EXPECT_EQ(classify_error(code), ErrorAction::Reconnect) << code;
}
