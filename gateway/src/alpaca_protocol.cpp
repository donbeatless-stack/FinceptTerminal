// gateway/src/alpaca_protocol.cpp
#include "fmdg/alpaca_protocol.h"

#include "fmdg/http.h"

#include <simdjson.h>

#include <cstdio>
#include <format>

namespace fmdg::alpaca {

namespace {

void append_json_string(std::string& out, std::string_view s) {
    out.push_back('"');
    for (char c : s) {
        switch (c) {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            default:
                if (static_cast<unsigned char>(c) < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof buf, "\\u%04x", static_cast<unsigned>(static_cast<unsigned char>(c)));
                    out += buf;
                } else {
                    out.push_back(c);
                }
        }
    }
    out.push_back('"');
}

void append_symbol_array(std::string& out, const std::vector<std::string>& symbols) {
    out.push_back('[');
    for (std::size_t i = 0; i < symbols.size(); ++i) {
        if (i)
            out.push_back(',');
        append_json_string(out, symbols[i]);
    }
    out.push_back(']');
}

// Fixed-width decimal field; false on a non-digit or a short string.
bool digits(std::string_view s, std::size_t pos, std::size_t n, int& out) {
    if (pos + n > s.size())
        return false;
    int v = 0;
    for (std::size_t i = 0; i < n; ++i) {
        const char c = s[pos + i];
        if (c < '0' || c > '9')
            return false;
        v = v * 10 + (c - '0');
    }
    out = v;
    return true;
}

bool is_leap(int y) {
    return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
}

// Days since 1970-01-01 for a proleptic Gregorian date (H. Hinnant's days_from_civil).
std::int64_t days_from_civil(int y, int m, int d) {
    y -= m <= 2 ? 1 : 0;
    const std::int64_t era = (y >= 0 ? y : y - 399) / 400;
    const std::int64_t yoe = y - era * 400;
    const std::int64_t doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    const std::int64_t doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + doe - 719468;
}

using simdjson::dom::element;
using simdjson::dom::object;

bool get_str(const object& o, std::string_view key, std::string& out) {
    std::string_view v;
    if (o[key].get_string().get(v) != simdjson::SUCCESS)
        return false;
    out.assign(v);
    return true;
}

bool get_num(const object& o, std::string_view key, double& out) {
    return o[key].get_double().get(out) == simdjson::SUCCESS; // integers convert
}

bool get_time(const object& o, std::string_view key, std::int64_t& out) {
    std::string_view v;
    if (o[key].get_string().get(v) != simdjson::SUCCESS)
        return false;
    const auto ns = parse_rfc3339_ns(v);
    if (!ns)
        return false;
    out = *ns;
    return true;
}

} // namespace

std::optional<std::int64_t> parse_rfc3339_ns(std::string_view s) {
    int y = 0, mo = 0, d = 0, h = 0, mi = 0, sec = 0;
    if (s.size() < 20 || !digits(s, 0, 4, y) || s[4] != '-' || !digits(s, 5, 2, mo) || s[7] != '-' ||
        !digits(s, 8, 2, d) || (s[10] != 'T' && s[10] != 't' && s[10] != ' ') || !digits(s, 11, 2, h) ||
        s[13] != ':' || !digits(s, 14, 2, mi) || s[16] != ':' || !digits(s, 17, 2, sec))
        return std::nullopt;
    // int64 nanoseconds span 1677-09-21 .. 2262-04-11.
    static constexpr int kDaysIn[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (y < 1678 || y > 2261 || mo < 1 || mo > 12 || d < 1 ||
        d > kDaysIn[mo - 1] + (mo == 2 && is_leap(y) ? 1 : 0) || h > 23 || mi > 59 || sec > 60)
        return std::nullopt;

    std::size_t pos = 19;
    std::int64_t frac_ns = 0;
    if (s[pos] == '.') {
        const std::size_t start = ++pos;
        std::int64_t scale = 100'000'000;
        while (pos < s.size() && s[pos] >= '0' && s[pos] <= '9') {
            if (pos - start < 9) { // finer than a nanosecond: dropped
                frac_ns += (s[pos] - '0') * scale;
                scale /= 10;
            }
            ++pos;
        }
        if (pos == start)
            return std::nullopt;
    }
    if (pos >= s.size())
        return std::nullopt;
    std::int64_t offset_s = 0;
    if (s[pos] == 'Z' || s[pos] == 'z') {
        ++pos;
    } else if (s[pos] == '+' || s[pos] == '-') {
        int oh = 0, om = 0;
        if (!digits(s, pos + 1, 2, oh) || pos + 3 >= s.size() || s[pos + 3] != ':' || !digits(s, pos + 4, 2, om) ||
            oh > 23 || om > 59)
            return std::nullopt;
        offset_s = (oh * 3600 + om * 60) * (s[pos] == '+' ? 1 : -1);
        pos += 6;
    } else {
        return std::nullopt;
    }
    if (pos != s.size())
        return std::nullopt;

    const std::int64_t secs = days_from_civil(y, mo, d) * 86'400 + h * 3'600 + mi * 60 + sec - offset_s;
    return secs * 1'000'000'000 + frac_ns;
}

std::string auth_message(std::string_view key, std::string_view secret) {
    std::string m = R"({"action":"auth","key":)";
    append_json_string(m, key);
    m += R"(,"secret":)";
    append_json_string(m, secret);
    m += '}';
    return m;
}

std::string subscribe_message(const std::vector<std::string>& symbols, bool bars) {
    std::string m = R"({"action":"subscribe","trades":)";
    append_symbol_array(m, symbols);
    if (bars) {
        m += R"(,"bars":)";
        append_symbol_array(m, symbols);
        m += R"(,"updatedBars":)";
        append_symbol_array(m, symbols);
    }
    m += '}';
    return m;
}

std::string rfc3339(std::int64_t unix_seconds) {
    // H. Hinnant's civil_from_days.
    std::int64_t days = unix_seconds / 86'400, secs = unix_seconds % 86'400;
    if (secs < 0) {
        secs += 86'400;
        --days;
    }
    days += 719'468;
    const std::int64_t era = (days >= 0 ? days : days - 146'096) / 146'097;
    const std::int64_t doe = days - era * 146'097;
    const std::int64_t yoe = (doe - doe / 1460 + doe / 36524 - doe / 146'096) / 365;
    const std::int64_t doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const std::int64_t mp = (5 * doy + 2) / 153;
    const std::int64_t d = doy - (153 * mp + 2) / 5 + 1;
    const std::int64_t m = mp < 10 ? mp + 3 : mp - 9;
    const std::int64_t y = yoe + era * 400 + (m <= 2 ? 1 : 0);
    return std::format("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, m, d, secs / 3600, secs / 60 % 60, secs % 60);
}

std::string bars_target(const std::vector<std::string>& symbols, std::int64_t start, std::int64_t end,
                        std::string_view feed, std::string_view page_token, int limit) {
    std::string joined;
    for (std::size_t i = 0; i < symbols.size(); ++i) {
        if (i)
            joined += ',';
        joined += symbols[i];
    }
    std::string t = "/v2/stocks/bars?symbols=" + http::url_encode(joined) + "&timeframe=1Min&start=" +
                    http::url_encode(rfc3339(start)) + "&end=" + http::url_encode(rfc3339(end)) +
                    "&limit=" + std::to_string(limit) + "&feed=" + http::url_encode(feed) +
                    "&adjustment=raw&sort=asc";
    if (!page_token.empty())
        t += "&page_token=" + http::url_encode(page_token);
    return t;
}

ErrorAction classify_error(int code) {
    switch (code) {
        case 402: // auth failed: wrong key or secret
        case 409: // insufficient subscription: the plan lacks this feed
        case 410: // invalid subscribe action for this feed
            return ErrorAction::Stop;
        case 406: // connection limit exceeded: someone else holds the stream
            return ErrorAction::ReconnectSlowly;
        case 400: // invalid syntax (e.g. a malformed symbol): our side, logged
        case 403: // already authenticated
        case 405: // symbol limit exceeded: earlier subscriptions remain
            return ErrorAction::Ignore;
        default: // 401 not authenticated, 404 auth timeout, 407 slow client, 500 internal, unknown
            return ErrorAction::Reconnect;
    }
}

struct MessageParser::Impl {
    simdjson::dom::parser parser;
};

MessageParser::MessageParser() : impl_(std::make_unique<Impl>()) {}
MessageParser::~MessageParser() = default;

bool MessageParser::parse(std::string_view json, Batch& out, std::string* error) {
    element doc;
    // realloc_if_needed: copies into a padded buffer, since `json` has no
    // SIMDJSON_PADDING after it. Also validates UTF-8.
    if (const auto err = impl_->parser.parse(json.data(), json.size(), true).get(doc); err != simdjson::SUCCESS) {
        if (error)
            *error = simdjson::error_message(err);
        return false;
    }
    simdjson::dom::array items;
    if (doc.get_array().get(items) != simdjson::SUCCESS) {
        if (error)
            *error = "not a JSON array";
        return false;
    }

    for (element item : items) {
        object o;
        std::string_view type;
        if (item.get_object().get(o) != simdjson::SUCCESS || o["T"].get_string().get(type) != simdjson::SUCCESS) {
            ++out.malformed;
            continue;
        }

        if (type == "t") {
            Trade t;
            if (get_str(o, "S", t.symbol) && get_num(o, "p", t.price) && get_num(o, "s", t.size) &&
                get_time(o, "t", t.ts_ns))
                out.trades.push_back(std::move(t));
            else
                ++out.malformed;
        } else if (type == "b" || type == "u") {
            Bar b;
            b.updated = type == "u";
            if (get_str(o, "S", b.symbol) && get_num(o, "o", b.open) && get_num(o, "h", b.high) &&
                get_num(o, "l", b.low) && get_num(o, "c", b.close) && get_num(o, "v", b.volume) &&
                get_time(o, "t", b.start_ns))
                out.bars.push_back(std::move(b));
            else
                ++out.malformed;
        } else if (type == "success") {
            std::string msg;
            get_str(o, "msg", msg);
            if (msg == "connected")
                out.control.push_back({Control::Kind::Connected, 0, msg});
            else if (msg == "authenticated")
                out.control.push_back({Control::Kind::Authenticated, 0, msg});
            else
                ++out.other;
        } else if (type == "error") {
            Control c{Control::Kind::Error, 0, {}};
            double code = 0;
            get_num(o, "code", code);
            c.code = static_cast<int>(code);
            get_str(o, "msg", c.message);
            out.control.push_back(std::move(c));
        } else if (type == "subscription") {
            Control c{Control::Kind::Subscription, 0, {}};
            for (const auto field : o) {
                simdjson::dom::array syms;
                if (field.key == "T" || field.value.get_array().get(syms) != simdjson::SUCCESS || syms.size() == 0)
                    continue;
                if (!c.message.empty())
                    c.message += ' ';
                c.message.append(field.key).append("=[");
                bool first = true;
                for (element s : syms) {
                    std::string_view name;
                    if (s.get_string().get(name) != simdjson::SUCCESS)
                        continue;
                    if (!first)
                        c.message += ',';
                    c.message.append(name);
                    first = false;
                }
                c.message += ']';
            }
            out.control.push_back(std::move(c));
        } else {
            ++out.other; // quotes, statuses, corrections, cancels, ...
        }
    }
    return true;
}

bool MessageParser::parse_bars_page(std::string_view json, std::vector<Bar>& out, std::string& next_page_token,
                                    std::size_t& malformed, std::string* error) {
    next_page_token.clear();
    element doc;
    if (const auto err = impl_->parser.parse(json.data(), json.size(), true).get(doc); err != simdjson::SUCCESS) {
        if (error)
            *error = simdjson::error_message(err);
        return false;
    }
    object root;
    if (doc.get_object().get(root) != simdjson::SUCCESS) {
        if (error)
            *error = "not a JSON object";
        return false;
    }
    std::string_view token;
    if (root["next_page_token"].get_string().get(token) == simdjson::SUCCESS)
        next_page_token.assign(token); // null on the last page
    object by_symbol;
    if (root["bars"].get_object().get(by_symbol) != simdjson::SUCCESS)
        return true; // "bars": null or missing — an empty range
    for (const auto field : by_symbol) {
        simdjson::dom::array bars;
        if (field.value.get_array().get(bars) != simdjson::SUCCESS) {
            ++malformed;
            continue;
        }
        for (element item : bars) {
            object o;
            Bar b;
            b.symbol.assign(field.key);
            if (item.get_object().get(o) == simdjson::SUCCESS && get_num(o, "o", b.open) && get_num(o, "h", b.high) &&
                get_num(o, "l", b.low) && get_num(o, "c", b.close) && get_num(o, "v", b.volume) &&
                get_time(o, "t", b.start_ns))
                out.push_back(std::move(b));
            else
                ++malformed;
        }
    }
    return true;
}

} // namespace fmdg::alpaca
