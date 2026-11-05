// gateway/include/fmdg/history.h
#pragma once
// HISTORY on the request port: stored bars, aggregated by PostgreSQL.
//
//   HISTORY <timeframe> <from> <to> <SYMBOL>...
//     timeframe   1m 3m 5m 10m 15m 30m 1h 4h
//     from, to    Unix seconds (UTC): bars whose period starts in [from, to)
//
//   reply       BARS <n> <complete_to>
//               <SYMBOL> <start> <open> <high> <low> <close> <volume>   n lines, by symbol then time
//               END
//               or ERR <why>
//
// Only one-minute bars are stored; longer periods are built from them in SQL
// (date_bin + first/max/min/last/sum), aligned to the Unix epoch, which puts
// 15m bars at :00/:15/:30/:45 UTC, as in Alpaca's own bars and the terminal's
// live CandleAggregator. Only whole periods come back: a `from` inside a period
// starts at the next one, so no bar is built from part of its minutes. The last
// period may still be in progress; the caller decides whether to keep it.
//
// complete_to (Unix s) comes from the bar sync: every symbol's minutes before it
// are stored. 0 means no sync is running, so the stored bars may have gaps (the
// live stream alone); a client should not take them as complete history.

#include "fmdg/gateway.h"
#include "fmdg/pg.h"
#include "fmdg/pool.h"

#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace fmdg {

struct HistoryRequest {
    int bucket_seconds = 60;
    std::int64_t from = 0; ///< aligned up to a bucket boundary
    std::int64_t to = 0;   ///< aligned up to a bucket boundary
    std::vector<std::string> symbols;
};

/// Bars a single reply may carry; a larger request gets "ERR ... narrow the range".
inline constexpr std::int64_t kMaxHistoryBars = 2'000'000;

/// Parses one HISTORY line. nullopt, with `error` set, when it is malformed.
std::optional<HistoryRequest> parse_history(std::string_view line, std::string& error);

/// The query for `table`: $1 bucket seconds, $2 symbols (text[]), $3 from, $4 to, $5 row limit.
std::string history_sql(const std::string& table);

/// Runs the request on a pooled connection and formats the reply.
std::string run_history(Pool<pg::Connection>& pool, const std::string& table, const HistoryRequest& request,
                        std::int64_t complete_to = 0);

/// The request-port command (for Gateway::add_command("HISTORY", ...)). `pool`
/// must outlive the gateway's workers. With `synced`, only symbols it accepts
/// are answered: complete_to vouches for the sync's symbols, and a symbol only
/// the live stream writes may have gaps, so it is left out (the client fetches
/// it elsewhere) rather than returned as if complete.
AsyncCommand history_command(Pool<pg::Connection>& pool, std::string table,
                             std::function<std::int64_t()> complete_to = {},
                             std::function<bool(std::string_view)> synced = {});

} // namespace fmdg
