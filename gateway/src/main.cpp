// gateway/src/main.cpp — the fmdg executable: a gateway fed by the synthetic
// feed or by Alpaca's real-time stream, printing a stats line every few seconds
// until Ctrl-C.
#include "fmdg/alpaca_feed.h"
#include "fmdg/bar_store.h"
#include "fmdg/bar_sync.h"
#include "fmdg/gateway.h"
#include "fmdg/history.h"
#include "fmdg/synthetic_feed.h"

#include <atomic>
#include <cctype>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <ctime>
#include <exception>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

namespace {

std::atomic<bool> g_quit{false};

extern "C" void on_signal(int) {
    g_quit.store(true);
}

void usage() {
    std::puts(
        "usage: fmdg [options]\n"
        "  --bind ADDR            listen address              (default 127.0.0.1)\n"
        "  --push-port N          long-connection port        (default 8589)\n"
        "  --request-port N       short-connection port       (default 8588)\n"
        "  --io-threads N         network threads             (default 2)\n"
        "  --max-clients N        push connections allowed    (default 10000)\n"
        "  --send-buffer BYTES    SO_SNDBUF of push sockets   (default: OS default)\n"
        "  --busy-poll            distributor never sleeps (lowest latency, burns a core)\n"
        "  --feed KIND            synthetic | alpaca | none   (default synthetic)\n"
        "  --stats-interval S     seconds between stats lines (default 5; 0 = quiet)\n"
        "  --duration S           exit after S seconds        (default 0 = until Ctrl-C)\n"
        "synthetic feed:\n"
        "  --feed-symbols N       symbols SYM0000..           (default 100; 0 = no feed)\n"
        "  --feed-rate R          ticks per second            (default 10000)\n"
        "alpaca feed (trades become ticks; minute bars are counted):\n"
        "  --alpaca-stream NAME   iex | sip | delayed_sip | test (default iex; test streams\n"
        "                         the made-up FAKEPACA around the clock)\n"
        "  --alpaca-symbols LIST  comma-separated, e.g. AAPL,MSFT (the free plan allows 30)\n"
        "  --alpaca-credentials F file with APCA_API_KEY_ID=... and APCA_API_SECRET_KEY=...\n"
        "                         lines; without it, those environment variables are read\n"
        "  --alpaca-no-bars       subscribe to trades only\n"
        "  --ca-file PEM          trust these roots instead of the system's\n"
        "minute bars into PostgreSQL (alpaca feed only):\n"
        "  --pg CONNINFO          libpq connection string; keep the password out of it, e.g.\n"
        "                         \"host=localhost dbname=fmdg user=fmdg passfile=C:/path/pgpass.conf\"\n"
        "  --pg-table NAME        table for the bars          (default bars_1m)\n"
        "                         with --pg, HISTORY <tf> <from> <to> <symbols> answers on the request port\n"
        "bar sync from Alpaca's REST API (needs --pg and credentials; no --feed needed):\n"
        "  --sync-symbols LIST    comma-separated, or @FILE (one per line or comma-separated)\n"
        "  --sync-days N          backfill depth              (default 60)\n"
        "  --sync-feed NAME       iex | sip                   (default iex)");
}

double us(std::int64_t ns) {
    return static_cast<double>(ns) / 1000.0;
}

std::string env(const char* name) {
#ifdef _WIN32
    char* buf = nullptr;
    std::size_t n = 0;
    if (_dupenv_s(&buf, &n, name) != 0 || buf == nullptr)
        return {};
    std::string v(buf);
    std::free(buf);
    return v;
#else
    const char* v = std::getenv(name);
    return v ? v : "";
#endif
}

std::string trim(std::string s) {
    const auto first = s.find_first_not_of(" \t\r\n\"'");
    const auto last = s.find_last_not_of(" \t\r\n\"'");
    return first == std::string::npos ? std::string{} : s.substr(first, last - first + 1);
}

/// Reads APCA_API_KEY_ID / APCA_API_SECRET_KEY from a KEY=value file (blank lines
/// and # comments allowed). False if the file cannot be opened.
bool read_credentials(const std::string& path, std::string& key, std::string& secret) {
    std::ifstream in(path);
    if (!in)
        return false;
    std::string line;
    while (std::getline(in, line)) {
        line = trim(line);
        if (line.empty() || line[0] == '#')
            continue;
        const auto eq = line.find('=');
        if (eq == std::string::npos)
            continue;
        const std::string name = trim(line.substr(0, eq));
        if (name == "APCA_API_KEY_ID")
            key = trim(line.substr(eq + 1));
        else if (name == "APCA_API_SECRET_KEY")
            secret = trim(line.substr(eq + 1));
    }
    return true;
}

/// "@path" → the file's contents with newlines as commas; anything else as is.
std::string read_list(const std::string& arg) {
    if (arg.empty() || arg[0] != '@')
        return arg;
    std::ifstream in(arg.substr(1));
    if (!in) {
        std::fprintf(stderr, "fmdg: cannot read %s\n", arg.substr(1).c_str());
        std::exit(2);
    }
    std::string out, line;
    while (std::getline(in, line))
        out += line + ",";
    return out;
}

/// A PostgreSQL text[] literal: {"AAPL","MSFT"}.
std::string pg_text_array(const std::vector<std::string>& items) {
    std::string out = "{";
    for (std::size_t i = 0; i < items.size(); ++i) {
        out += i ? ",\"" : "\"";
        for (char c : items[i]) {
            if (c == '"' || c == '\\')
                out += '\\';
            out += c;
        }
        out += '"';
    }
    return out + "}";
}

std::vector<std::string> split_symbols(const std::string& list) {
    std::vector<std::string> out;
    std::size_t start = 0;
    while (start <= list.size()) {
        const auto comma = list.find(',', start);
        std::string sym = trim(list.substr(start, comma == std::string::npos ? std::string::npos : comma - start));
        for (auto& c : sym)
            c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
        if (!sym.empty())
            out.push_back(sym);
        if (comma == std::string::npos)
            break;
        start = comma + 1;
    }
    return out;
}

void log_line(const char* source, const std::string& line) {
    const std::time_t t = std::time(nullptr);
    std::tm tm{};
#ifdef _WIN32
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    char stamp[16];
    std::strftime(stamp, sizeof stamp, "%H:%M:%S", &tm);
    std::printf("%s %s: %s\n", stamp, source, line.c_str());
    std::fflush(stdout);
}

} // namespace

int main(int argc, char** argv) {
    fmdg::GatewayConfig config;
    fmdg::SyntheticFeedConfig feed_config;
    fmdg::AlpacaFeedConfig alpaca_config;
    std::string feed_kind = "synthetic";
    std::string alpaca_stream = "iex";
    std::string alpaca_symbols;
    std::string credentials_file;
    std::string pg_conninfo;
    std::string pg_table = "bars_1m";
    std::string sync_symbols;
    std::string sync_feed = "iex";
    int sync_days = 60;
    double stats_interval = 5;
    double duration = 0;

    for (int i = 1; i < argc; ++i) {
        const std::string_view arg = argv[i];
        auto value = [&]() -> std::string {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "fmdg: %s needs a value\n", argv[i]);
                std::exit(2);
            }
            return argv[++i];
        };
        try {
            if (arg == "--bind")
                config.bind_address = value();
            else if (arg == "--push-port")
                config.push_port = static_cast<std::uint16_t>(std::stoul(value()));
            else if (arg == "--request-port")
                config.request_port = static_cast<std::uint16_t>(std::stoul(value()));
            else if (arg == "--io-threads")
                config.io_threads = std::stoi(value());
            else if (arg == "--max-clients")
                config.max_clients = std::stoull(value());
            else if (arg == "--send-buffer")
                config.send_buffer = std::stoi(value());
            else if (arg == "--busy-poll")
                config.busy_poll = true;
            else if (arg == "--feed-symbols")
                feed_config.symbols = std::stoull(value());
            else if (arg == "--feed-rate")
                feed_config.rate = std::stod(value());
            else if (arg == "--feed")
                feed_kind = value();
            else if (arg == "--alpaca-stream")
                alpaca_stream = value();
            else if (arg == "--alpaca-symbols")
                alpaca_symbols = value();
            else if (arg == "--alpaca-credentials")
                credentials_file = value();
            else if (arg == "--alpaca-no-bars")
                alpaca_config.bars = false;
            else if (arg == "--ca-file")
                alpaca_config.ca_file = value();
            else if (arg == "--pg")
                pg_conninfo = value();
            else if (arg == "--pg-table")
                pg_table = value();
            else if (arg == "--sync-symbols")
                sync_symbols = value();
            else if (arg == "--sync-days")
                sync_days = std::stoi(value());
            else if (arg == "--sync-feed")
                sync_feed = value();
            else if (arg == "--stats-interval")
                stats_interval = std::stod(value());
            else if (arg == "--duration")
                duration = std::stod(value());
            else if (arg == "--help" || arg == "-h") {
                usage();
                return 0;
            } else {
                std::fprintf(stderr, "fmdg: unknown option %s\n", argv[i]);
                usage();
                return 2;
            }
        } catch (const std::exception&) {
            std::fprintf(stderr, "fmdg: bad value for %s\n", std::string(arg).c_str());
            return 2;
        }
    }

    if (feed_kind != "synthetic" && feed_kind != "alpaca" && feed_kind != "none") {
        std::fprintf(stderr, "fmdg: --feed must be synthetic, alpaca or none\n");
        return 2;
    }
    const bool use_alpaca = feed_kind == "alpaca";
    const std::vector<std::string> sync_list = split_symbols(read_list(sync_symbols));
    if (!sync_symbols.empty() && sync_list.empty()) {
        std::fprintf(stderr, "fmdg: --sync-symbols names no symbols\n");
        return 2;
    }
    const bool use_sync = !sync_list.empty();
    if (use_sync && pg_conninfo.empty()) {
        std::fprintf(stderr, "fmdg: --sync-symbols stores bars: it needs --pg\n");
        return 2;
    }
    std::string api_key, api_secret;
    if (use_alpaca || use_sync) {
        if (!credentials_file.empty()) {
            if (!read_credentials(credentials_file, api_key, api_secret)) {
                std::fprintf(stderr, "fmdg: cannot read %s\n", credentials_file.c_str());
                return 2;
            }
        } else {
            api_key = env("APCA_API_KEY_ID");
            api_secret = env("APCA_API_SECRET_KEY");
        }
        if (api_key.empty() || api_secret.empty()) {
            std::fprintf(stderr, "fmdg: Alpaca credentials missing: set APCA_API_KEY_ID and APCA_API_SECRET_KEY, "
                                 "or pass --alpaca-credentials FILE with those two lines\n");
            return 2;
        }
    }
    if (use_alpaca) {
        if (alpaca_stream != "iex" && alpaca_stream != "sip" && alpaca_stream != "delayed_sip" &&
            alpaca_stream != "test") {
            std::fprintf(stderr, "fmdg: --alpaca-stream must be iex, sip, delayed_sip or test\n");
            return 2;
        }
        alpaca_config.path = "/v2/" + alpaca_stream;
        alpaca_config.symbols = split_symbols(alpaca_symbols.empty() && alpaca_stream == "test" ? "FAKEPACA"
                                                                                                : alpaca_symbols);
        if (alpaca_config.symbols.empty()) {
            std::fprintf(stderr, "fmdg: --alpaca-symbols is required (e.g. AAPL,MSFT)\n");
            return 2;
        }
        alpaca_config.key = api_key;
        alpaca_config.secret = api_secret;
        if (alpaca_stream == "iex" && alpaca_config.symbols.size() > 30)
            std::fprintf(stderr, "fmdg: warning: the free plan streams at most 30 symbols; Alpaca answers more "
                                 "with error 405\n");
        alpaca_config.log = [](const std::string& line) { log_line("alpaca", line); };
    }

    // The store before the gateway: HISTORY must be registered before it starts.
    std::unique_ptr<fmdg::BarStore> store;
    if (!pg_conninfo.empty()) {
        fmdg::BarStoreConfig sc;
        sc.conninfo = pg_conninfo;
        sc.table = pg_table;
        sc.log = [](const std::string& line) { log_line("store", line); };
        try {
            store = std::make_unique<fmdg::BarStore>(sc);
        } catch (const std::exception& e) {
            std::fprintf(stderr, "fmdg: %s\n", e.what());
            return 2;
        }
        store->start();
        alpaca_config.on_bar = [s = store.get()](const fmdg::alpaca::Bar& b) { s->add(b); };
    }

    std::unique_ptr<fmdg::BarSync> sync;
    if (use_sync) {
        fmdg::BarSyncConfig yc;
        yc.key = api_key;
        yc.secret = api_secret;
        yc.feed = sync_feed;
        yc.ca_file = alpaca_config.ca_file;
        yc.symbols = sync_list;
        yc.days = sync_days;
        fmdg::BarStore* s = store.get();
        yc.on_bars = [s](const std::vector<fmdg::alpaca::Bar>& bars) {
            std::uint64_t seq = 0;
            for (const auto& b : bars)
                seq = s->add(b);
            return seq;
        };
        yc.written_through = [s] { return s->written_through(); };
        yc.backlog = [s] { return s->stats().queued; };
        yc.resume_from = [s, pg_table](const std::vector<std::string>& syms) -> std::int64_t {
            try { // where the stored history of every one of `syms` ends; 0 if any has none
                auto lease = s->pool().acquire();
                const auto r = lease->exec("SELECT count(*), coalesce(extract(epoch FROM min(m))::bigint, 0) FROM "
                                           "(SELECT max(start_ts) AS m FROM " +
                                               pg_table + " WHERE symbol = ANY($1::text[]) GROUP BY symbol) s",
                                           {pg_text_array(syms)});
                if (r.integer(0, 0) < static_cast<std::int64_t>(syms.size()))
                    return 0;
                return r.integer(0, 1) + 60;
            } catch (const std::exception&) {
                return 0; // no table yet, or the database is down: backfill everything
            }
        };
        yc.log = [](const std::string& line) { log_line("sync", line); };
        try {
            sync = std::make_unique<fmdg::BarSync>(yc);
        } catch (const std::exception& e) {
            std::fprintf(stderr, "fmdg: %s\n", e.what());
            return 2;
        }
    }

    fmdg::Gateway gateway(config);
    if (store) {
        // With a sync, HISTORY answers for the synced symbols only: those are the
        // ones complete_to vouches for.
        std::function<bool(std::string_view)> synced;
        if (sync)
            synced = [set = std::set<std::string, std::less<>>(sync_list.begin(), sync_list.end())](
                         std::string_view s) { return set.contains(s); };
        gateway.add_command("HISTORY", fmdg::history_command(
                                           store->pool(), pg_table,
                                           [&sync] { return sync ? sync->complete_to() : std::int64_t{0}; },
                                           std::move(synced)));
    }
    try {
        gateway.start();
    } catch (const std::system_error& e) {
        std::fprintf(stderr, "fmdg: cannot listen on %s: %s\n", config.bind_address.c_str(), e.what());
        return 1;
    }
    std::printf("fmdg: push port %u, request port %u, %d io threads%s\n", gateway.push_port(),
                gateway.request_port(), config.io_threads, config.busy_poll ? ", busy-poll" : "");
    if (store)
        std::printf("fmdg: PostgreSQL table %s; HISTORY on the request port\n", pg_table.c_str());

    // Exactly one producer may call Gateway::ingest().
    std::unique_ptr<fmdg::SyntheticFeed> synthetic;
    std::unique_ptr<fmdg::AlpacaFeed> alpaca;
    if (use_alpaca) {
        try {
            alpaca = std::make_unique<fmdg::AlpacaFeed>(gateway, alpaca_config);
        } catch (const std::exception& e) { // e.g. an unreadable --ca-file
            std::fprintf(stderr, "fmdg: %s\n", e.what());
            gateway.stop();
            return 1;
        }
        alpaca->start();
        std::printf("fmdg: alpaca feed, %s stream, %zu symbol(s)%s\n", alpaca_stream.c_str(),
                    alpaca_config.symbols.size(), alpaca_config.bars ? ", trades + minute bars" : ", trades");
    } else if (feed_kind == "synthetic" && feed_config.symbols > 0) {
        synthetic = std::make_unique<fmdg::SyntheticFeed>(gateway, feed_config);
        synthetic->start();
        std::printf("fmdg: synthetic feed, %zu symbols at %.0f ticks/s\n", feed_config.symbols, feed_config.rate);
    }
    if (sync) {
        sync->start();
        std::printf("fmdg: bar sync, %zu symbol(s), %d days of %s one-minute bars, then a top-up each minute\n",
                    sync_list.size(), sync_days, sync_feed.c_str());
    }
    std::fflush(stdout);

    std::signal(SIGINT, on_signal);
    std::signal(SIGTERM, on_signal);

    using clock = std::chrono::steady_clock;
    const auto t0 = clock::now();
    auto last_print = t0;
    fmdg::GatewayStats prev = gateway.stats();
    while (!g_quit.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        const auto now = clock::now();
        if (duration > 0 && std::chrono::duration<double>(now - t0).count() >= duration)
            break;
        const double dt = std::chrono::duration<double>(now - last_print).count();
        if (stats_interval <= 0 || dt < stats_interval)
            continue;
        const fmdg::GatewayStats s = gateway.stats();
        std::printf("clients=%llu in=%.0f/s out=%.0f/s conflated=%llu backlog=%zu waits=%llu  "
                    "latency p50=%.1fus p99=%.1fus p99.9=%.1fus max=%.1fus\n",
                    static_cast<unsigned long long>(s.clients), static_cast<double>(s.ticks_in - prev.ticks_in) / dt,
                    static_cast<double>(s.ticks_out - prev.ticks_out) / dt,
                    static_cast<unsigned long long>(s.conflated), s.backlog,
                    static_cast<unsigned long long>(s.ingest_waits), us(s.latency_p50_ns), us(s.latency_p99_ns),
                    us(s.latency_p999_ns), us(s.latency_max_ns));
        if (alpaca) {
            const fmdg::AlpacaFeedStats a = alpaca->stats();
            std::printf("  alpaca %s: trades=%llu bars=%llu messages=%llu disconnects=%llu errors=%llu%s%s\n",
                        a.state.c_str(), static_cast<unsigned long long>(a.trades),
                        static_cast<unsigned long long>(a.bars), static_cast<unsigned long long>(a.messages),
                        static_cast<unsigned long long>(a.disconnects),
                        static_cast<unsigned long long>(a.server_errors + a.malformed),
                        a.last_error.empty() ? "" : "  last error: ", a.last_error.c_str());
        }
        if (store) {
            const fmdg::BarStoreStats b = store->stats();
            std::printf("  store: bars=%llu written=%llu batches=%llu queued=%zu failed=%llu dropped=%llu "
                        "pool open=%zu idle=%zu%s%s\n",
                        static_cast<unsigned long long>(b.received), static_cast<unsigned long long>(b.written),
                        static_cast<unsigned long long>(b.batches), b.queued,
                        static_cast<unsigned long long>(b.failed_batches), static_cast<unsigned long long>(b.dropped),
                        b.pool.open, b.pool.idle, b.last_error.empty() ? "" : "  last error: ", b.last_error.c_str());
        }
        if (sync) {
            const fmdg::BarSyncStats y = sync->stats();
            std::printf("  sync %s: complete_to=%s bars=%llu requests=%llu retries=%llu rate_limited=%llu%s%s\n",
                        y.state.c_str(), y.complete_to ? fmdg::alpaca::rfc3339(y.complete_to).c_str() : "-",
                        static_cast<unsigned long long>(y.bars), static_cast<unsigned long long>(y.requests),
                        static_cast<unsigned long long>(y.retries), static_cast<unsigned long long>(y.rate_limited),
                        y.last_error.empty() ? "" : "  last error: ", y.last_error.c_str());
        }
        std::fflush(stdout);
        prev = s;
        last_print = now;
    }

    // Producers first: ingest() must not race Gateway::stop(), and no bar may
    // reach the store after it flushed. Then the gateway, whose HISTORY workers
    // use the store's pool; the store last, writing what is queued.
    if (sync)
        sync->stop();
    if (synthetic)
        synthetic->stop();
    if (alpaca)
        alpaca->stop();
    gateway.stop();
    if (store)
        store->stop();
    std::puts("fmdg: stopped");
    return 0;
}
