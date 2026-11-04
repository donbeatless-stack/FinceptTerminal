// gateway/include/fmdg/alpaca_protocol.h
#pragma once
// Alpaca's market-data stream messages ("Real-time Stock Pricing Data" in the
// Alpaca docs): the JSON the gateway sends and the JSON it receives, as pure
// functions so they are unit-tested without a connection
// (tests/test_alpaca_protocol.cpp). The connection itself is alpaca_feed.h.

#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace fmdg::alpaca {

/// RFC-3339 → nanoseconds since the Unix epoch. Alpaca sends UTC with up to
/// nine fractional digits ("2026-11-04T19:15:02.123456789Z"); numeric offsets
/// ("-05:00") and 0–9 fractional digits are accepted too. nullopt if malformed.
std::optional<std::int64_t> parse_rfc3339_ns(std::string_view s);

/// {"action":"auth","key":…,"secret":…}. Never log it.
std::string auth_message(std::string_view key, std::string_view secret);

/// {"action":"subscribe","trades":[…]} plus "bars" and "updatedBars" when
/// `bars` is set. Symbols are JSON-escaped, never trusted.
std::string subscribe_message(const std::vector<std::string>& symbols, bool bars);

struct Trade {
    std::string symbol;
    double price = 0.0;
    double size = 0.0;
    std::int64_t ts_ns = 0; ///< exchange timestamp, Unix epoch ns
};

/// A one-minute bar. `start_ns` is the minute it covers ("t"). Alpaca sends it
/// just after the minute ends; `updated` marks a late-trade correction of a bar
/// already sent ("updatedBars").
struct Bar {
    std::string symbol;
    double open = 0.0, high = 0.0, low = 0.0, close = 0.0, volume = 0.0;
    std::int64_t start_ns = 0;
    bool updated = false;
};

struct Control {
    enum class Kind { Connected, Authenticated, Subscription, Error };
    Kind kind = Kind::Connected;
    int code = 0;         ///< Error only
    std::string message;  ///< Error: Alpaca's text; Subscription: "trades=[…] bars=[…]"
};

/// Everything one WebSocket message (a JSON array) carried.
struct Batch {
    std::vector<Control> control;
    std::vector<Trade> trades;
    std::vector<Bar> bars;
    std::size_t other = 0;     ///< well-formed items of types the gateway does not use
    std::size_t malformed = 0; ///< items missing a field or carrying a wrong type: skipped
};

/// What a server error code means for the connection.
enum class ErrorAction {
    Ignore,          ///< logged; the session goes on (e.g. 405: earlier subscriptions remain)
    Reconnect,       ///< drop the connection, reconnect with the normal backoff
    ReconnectSlowly, ///< 406: another session holds the account's one connection
    Stop,            ///< retrying cannot help (bad credentials, plan lacks the feed)
};
ErrorAction classify_error(int code);

/// Parses stream messages. Holds one simdjson parser and reuses its buffers.
class MessageParser {
  public:
    MessageParser();
    ~MessageParser();
    MessageParser(const MessageParser&) = delete;
    MessageParser& operator=(const MessageParser&) = delete;

    /// Appends what `json` carries to `out`. False when `json` is not a JSON
    /// array (`error` says why); a bad item inside a good array is only counted
    /// in Batch::malformed, so one odd message cannot drop its neighbours.
    bool parse(std::string_view json, Batch& out, std::string* error = nullptr);

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace fmdg::alpaca
