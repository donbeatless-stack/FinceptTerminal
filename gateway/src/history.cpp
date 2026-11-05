// gateway/src/history.cpp
#include "fmdg/history.h"

#include "fmdg/protocol.h"

#include <cctype>
#include <charconv>
#include <format>

namespace fmdg {

namespace {

std::vector<std::string_view> words_of(std::string_view line) {
    std::vector<std::string_view> out;
    std::size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && (line[i] == ' ' || line[i] == '\t'))
            ++i;
        const std::size_t start = i;
        while (i < line.size() && line[i] != ' ' && line[i] != '\t')
            ++i;
        if (i > start)
            out.push_back(line.substr(start, i - start));
    }
    return out;
}

bool iequals(std::string_view a, std::string_view b) {
    if (a.size() != b.size())
        return false;
    for (std::size_t i = 0; i < a.size(); ++i)
        if (std::tolower(static_cast<unsigned char>(a[i])) != std::tolower(static_cast<unsigned char>(b[i])))
            return false;
    return true;
}

std::optional<std::int64_t> to_int(std::string_view s) {
    std::int64_t v = 0;
    const auto r = std::from_chars(s.data(), s.data() + s.size(), v);
    if (r.ec != std::errc{} || r.ptr != s.data() + s.size())
        return std::nullopt;
    return v;
}

std::int64_t align_up(std::int64_t t, std::int64_t step) {
    const std::int64_t r = t % step;
    return r == 0 ? t : t + (r > 0 ? step - r : -r);
}

// A PostgreSQL array literal of quoted strings: {"AAPL","BRK.B"}. Symbols are
// already printable ASCII; quotes and backslashes are escaped anyway.
std::string text_array(const std::vector<std::string>& items) {
    std::string out = "{";
    for (std::size_t i = 0; i < items.size(); ++i) {
        if (i)
            out += ',';
        out += '"';
        for (char c : items[i]) {
            if (c == '"' || c == '\\')
                out += '\\';
            out += c;
        }
        out += '"';
    }
    out += '}';
    return out;
}

} // namespace

std::optional<HistoryRequest> parse_history(std::string_view line, std::string& error) {
    const auto w = words_of(line);
    if (w.size() < 5 || !iequals(w[0], "HISTORY")) {
        error = "usage: HISTORY <1m|3m|5m|10m|15m|30m|1h|4h> <from> <to> <symbol>...";
        return std::nullopt;
    }
    static constexpr std::pair<std::string_view, int> kFrames[] = {{"1m", 60},     {"3m", 180},   {"5m", 300},
                                                                   {"10m", 600},   {"15m", 900},  {"30m", 1800},
                                                                   {"1h", 3600},   {"4h", 14400}};
    HistoryRequest r;
    bool known = false;
    for (const auto& [name, seconds] : kFrames)
        if (iequals(w[1], name)) {
            r.bucket_seconds = seconds;
            known = true;
        }
    if (!known) {
        error = "timeframe must be one of 1m 3m 5m 10m 15m 30m 1h 4h";
        return std::nullopt;
    }
    const auto from = to_int(w[2]), to = to_int(w[3]);
    if (!from || !to || *from < 0 || *to <= *from) {
        error = "from and to must be Unix seconds with from < to";
        return std::nullopt;
    }
    if (*to - *from > 3LL * 366 * 86400) {
        error = "range longer than three years";
        return std::nullopt;
    }
    r.from = align_up(*from, r.bucket_seconds);
    r.to = align_up(*to, r.bucket_seconds);
    if (w.size() - 4 > proto::kMaxSymbolsPerRequest) {
        error = "too many symbols";
        return std::nullopt;
    }
    for (std::size_t i = 4; i < w.size(); ++i) {
        if (!proto::valid_symbol(w[i])) {
            error = "invalid symbol";
            return std::nullopt;
        }
        r.symbols.emplace_back(w[i]);
    }
    return r;
}

std::string history_sql(const std::string& table) {
    // first(open) and last(close) as ordered array_agg: PostgreSQL has no
    // first()/last() aggregates built in.
    return std::format(
        "SELECT symbol, "
        "extract(epoch FROM date_bin(make_interval(secs => $1::int), start_ts, TIMESTAMPTZ '1970-01-01 00:00:00+00'))"
        "::bigint AS bucket, "
        "(array_agg(open ORDER BY start_ts))[1], max(high), min(low), "
        "(array_agg(close ORDER BY start_ts DESC))[1], sum(volume) "
        "FROM {} "
        "WHERE symbol = ANY($2::text[]) AND start_ts >= to_timestamp($3::bigint) AND start_ts < to_timestamp($4::bigint) "
        "GROUP BY symbol, bucket ORDER BY symbol, bucket LIMIT $5::bigint",
        table);
}

std::string run_history(Pool<pg::Connection>& pool, const std::string& table, const HistoryRequest& req,
                        std::int64_t complete_to) {
    if (req.from >= req.to)
        return std::format("BARS 0 {}\nEND\n", complete_to); // the range held no whole period
    auto lease = pool.acquire();
    try {
        const pg::Result rows =
            lease->exec(history_sql(table), {std::to_string(req.bucket_seconds), text_array(req.symbols),
                                             std::to_string(req.from), std::to_string(req.to),
                                             std::to_string(kMaxHistoryBars + 1)});
        const int n = rows.rows();
        if (n > kMaxHistoryBars)
            return std::format("ERR more than {} bars: narrow the range or the symbol list\n", kMaxHistoryBars);
        std::string out;
        out.reserve(static_cast<std::size_t>(n) * 64 + 32);
        out += std::format("BARS {} {}\n", n, complete_to);
        for (int i = 0; i < n; ++i) {
            for (int c = 0; c < 7; ++c) {
                if (c)
                    out += ' ';
                out += rows.text(i, c); // PostgreSQL prints doubles in their shortest exact form
            }
            out += '\n';
        }
        out += "END\n";
        return out;
    } catch (const pg::Error& e) {
        if (e.connection_lost())
            lease.discard();
        throw;
    }
}

AsyncCommand history_command(Pool<pg::Connection>& pool, std::string table, std::function<std::int64_t()> complete_to,
                             std::function<bool(std::string_view)> synced) {
    return [&pool, table = std::move(table), complete_to = std::move(complete_to), synced = std::move(synced)](
               std::string_view line, std::function<void(std::string)> reply) {
        std::string error;
        auto req = parse_history(line, error);
        if (!req) {
            reply("ERR " + error);
            return;
        }
        if (synced)
            std::erase_if(req->symbols, [&](const std::string& s) { return !synced(s); });
        // Read before the query: anything stored meanwhile is newer, so the
        // reply can only understate how complete it is, never overstate it.
        const std::int64_t done = complete_to ? complete_to() : 0;
        reply(run_history(pool, table, *req, done)); // a pg::Error or PoolTimeout becomes "ERR ..." in the gateway
    };
}

} // namespace fmdg
