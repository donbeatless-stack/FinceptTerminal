// gateway/src/main.cpp — the fmdg executable: a gateway, optionally fed by the
// synthetic feed, printing a stats line every few seconds until Ctrl-C.
#include "fmdg/gateway.h"
#include "fmdg/synthetic_feed.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>

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
        "  --feed-symbols N       synthetic symbols SYM0000.. (default 100; 0 = no feed)\n"
        "  --feed-rate R          synthetic ticks per second  (default 10000)\n"
        "  --stats-interval S     seconds between stats lines (default 5; 0 = quiet)\n"
        "  --duration S           exit after S seconds        (default 0 = until Ctrl-C)");
}

double us(std::int64_t ns) {
    return static_cast<double>(ns) / 1000.0;
}

} // namespace

int main(int argc, char** argv) {
    fmdg::GatewayConfig config;
    fmdg::SyntheticFeedConfig feed_config;
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

    fmdg::Gateway gateway(config);
    try {
        gateway.start();
    } catch (const std::system_error& e) {
        std::fprintf(stderr, "fmdg: cannot listen on %s: %s\n", config.bind_address.c_str(), e.what());
        return 1;
    }
    std::printf("fmdg: push port %u, request port %u, %d io threads%s\n", gateway.push_port(),
                gateway.request_port(), config.io_threads, config.busy_poll ? ", busy-poll" : "");

    fmdg::SyntheticFeed feed(gateway, feed_config);
    if (feed_config.symbols > 0) {
        feed.start();
        std::printf("fmdg: synthetic feed, %zu symbols at %.0f ticks/s\n", feed_config.symbols, feed_config.rate);
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
        std::fflush(stdout);
        prev = s;
        last_print = now;
    }

    feed.stop(); // the producer goes first: ingest() must not race stop()
    gateway.stop();
    std::puts("fmdg: stopped");
    return 0;
}
