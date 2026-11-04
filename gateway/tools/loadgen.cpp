// gateway/tools/loadgen.cpp — load generator and latency benchmark for fmdg.
//
// Opens N push-port clients, subscribes each to a set of symbols, and measures
// what they receive: throughput, end-to-end latency (gateway ingest → client
// receive, both on the same machine's monotonic clock), and per-symbol ordering.
// Some clients can be made deliberately slow (tiny receive buffer, a pause after
// every read) to show that they are conflated and do not hold anyone else back.
//
//   fmdg_loadgen --embedded --clients 50 --slow 5 --rate 200000 --duration 10
//       runs a gateway and synthetic feed in-process (one command, no ports)
//   fmdg_loadgen --port 8589 --clients 50
//       measures an fmdg already running on this machine
#include "fmdg/gateway.h"
#include "fmdg/latency_histogram.h"
#include "fmdg/protocol.h"
#include "fmdg/synthetic_feed.h"

#include <asio.hpp>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <format>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

using asio::ip::tcp;
using namespace std::chrono_literals;

namespace {

struct Options {
    std::string host = "127.0.0.1";
    std::uint16_t port = 8589;
    bool embedded = false;
    int clients = 50;
    int slow = 0;
    int slow_ms = 50;
    std::size_t symbols = 100;
    std::size_t per_client = 0; ///< symbols each client follows; 0 = all
    double rate = 100'000;
    double duration = 10;
    double warmup = 1;
    int io_threads = 2;
    int gateway_io_threads = 2;
    int send_buffer = 0;
    bool busy_poll = false;
    std::string prefix = "SYM";
};

/// Counters shared by the clients of one kind (fast or slow).
struct Group {
    fmdg::LatencyHistogram e2e;      ///< gateway ingest → client receive
    fmdg::LatencyHistogram internal; ///< gateway ingest → gateway send
    std::atomic<std::uint64_t> ticks{0};
    std::atomic<std::uint64_t> bytes{0};
    std::atomic<std::uint64_t> backwards{0}; ///< a symbol's sequence went down: must stay 0
    std::atomic<std::uint64_t> errors{0};
};

std::atomic<bool> g_measuring{false};
std::atomic<int> g_subscribed{0};

class LoadClient : public std::enable_shared_from_this<LoadClient> {
  public:
    LoadClient(asio::io_context& io, tcp::endpoint ep, std::vector<std::string> symbols, Group& group, int slow_ms)
        : socket_(asio::make_strand(io)), timer_(socket_.get_executor()), endpoint_(ep), symbols_(std::move(symbols)),
          group_(group), slow_ms_(slow_ms) {}

    void start() {
        socket_.open(endpoint_.protocol());
        if (slow_ms_ > 0) // a few KB of socket buffer: back-pressure reaches the gateway quickly
            socket_.set_option(asio::socket_base::receive_buffer_size(4096));
        socket_.async_connect(endpoint_, [self = shared_from_this()](const asio::error_code& ec) {
            if (ec) {
                std::fprintf(stderr, "loadgen: connect failed: %s\n", ec.message().c_str());
                self->group_.errors.fetch_add(1);
                return;
            }
            self->socket_.set_option(tcp::no_delay(true));
            fmdg::proto::append_symbols(self->out_, fmdg::proto::MsgType::Subscribe, self->symbols_);
            asio::async_write(self->socket_, asio::buffer(self->out_), [self](const asio::error_code& e, std::size_t) {
                if (!e)
                    self->read();
            });
        });
    }

    void stop() {
        asio::post(socket_.get_executor(), [self = shared_from_this()] {
            asio::error_code ignored;
            self->timer_.cancel();
            self->socket_.close(ignored);
        });
    }

  private:
    void read() {
        // A slow client takes at most 4 KB per read, so its bandwidth really is
        // capped (4 KB per slow_ms) however much the OS has buffered for it.
        const std::size_t want = slow_ms_ > 0 ? 4096 : buf_.size();
        socket_.async_read_some(asio::buffer(buf_.data(), want), [self = shared_from_this()](const asio::error_code& ec, std::size_t n) {
            if (ec)
                return; // closed: by stop(), or the gateway went away
            self->on_data(n);
            if (self->slow_ms_ > 0) {
                self->timer_.expires_after(std::chrono::milliseconds(self->slow_ms_));
                self->timer_.async_wait([self](const asio::error_code& e) {
                    if (!e)
                        self->read();
                });
            } else {
                self->read();
            }
        });
    }

    void on_data(std::size_t n) {
        const std::int64_t now = fmdg::now_ns(); // one receive time for everything in this read
        const bool measuring = g_measuring.load(std::memory_order_relaxed);
        if (measuring)
            group_.bytes.fetch_add(n, std::memory_order_relaxed);
        reader_.feed(buf_.data(), n);
        fmdg::proto::Header h;
        std::span<const std::uint8_t> body;
        std::uint64_t ticks = 0;
        for (;;) {
            const auto r = reader_.next(h, body);
            if (r == fmdg::proto::FrameReader::Result::NeedMore)
                break;
            if (r == fmdg::proto::FrameReader::Result::Bad) {
                group_.errors.fetch_add(1);
                return;
            }
            if (h.type == fmdg::proto::MsgType::Tick) {
                const auto m = fmdg::proto::parse_tick(body);
                if (!m)
                    continue;
                auto& last = last_seq_[m->tick.symbol];
                if (m->tick.sequence < last)
                    group_.backwards.fetch_add(1, std::memory_order_relaxed);
                last = m->tick.sequence;
                if (measuring && !(h.flags & fmdg::proto::kFlagSnapshot)) {
                    group_.e2e.record(now - m->tick.ingest_ts_ns);
                    group_.internal.record(m->send_ts_ns - m->tick.ingest_ts_ns);
                    ++ticks;
                }
            } else if (h.type == fmdg::proto::MsgType::Subscribed) {
                g_subscribed.fetch_add(1);
            } else if (h.type == fmdg::proto::MsgType::Error) {
                const auto e = fmdg::proto::parse_error(body);
                std::fprintf(stderr, "loadgen: gateway error: %s\n", e ? e->message.c_str() : "?");
                group_.errors.fetch_add(1);
            }
        }
        if (ticks)
            group_.ticks.fetch_add(ticks, std::memory_order_relaxed);
    }

    tcp::socket socket_;
    asio::steady_timer timer_;
    tcp::endpoint endpoint_;
    std::vector<std::string> symbols_;
    Group& group_;
    const int slow_ms_;
    fmdg::proto::Bytes out_;
    std::array<std::uint8_t, 64 * 1024> buf_{};
    fmdg::proto::FrameReader reader_;
    std::unordered_map<fmdg::SymbolId, std::uint64_t> last_seq_;
};

void usage() {
    std::puts(
        "usage: fmdg_loadgen [options]\n"
        "  --embedded            run a gateway + synthetic feed in this process\n"
        "  --host ADDR --port N  the gateway's push port      (default 127.0.0.1:8589)\n"
        "  --clients N           push connections             (default 50)\n"
        "  --slow N              of which deliberately slow   (default 0)\n"
        "  --slow-ms N           slow clients read 4 KB, then pause N ms (default 50)\n"
        "  --symbols N           symbols SYM0000.. to use     (default 100)\n"
        "  --per-client N        symbols each client follows  (default all)\n"
        "  --duration S          measured seconds             (default 10)\n"
        "  --warmup S            unmeasured seconds first     (default 1)\n"
        "  --io-threads N        loadgen network threads      (default 2)\n"
        "  --send-buffer BYTES   gateway SO_SNDBUF per push socket (default: OS default)\n"
        "embedded only:\n"
        "  --rate R              feed ticks per second, 0 = as fast as possible (default 100000)\n"
        "  --gateway-io-threads N                             (default 2)\n"
        "  --busy-poll           gateway distributor never sleeps");
}

Options parse(int argc, char** argv) {
    Options o;
    for (int i = 1; i < argc; ++i) {
        const std::string_view a = argv[i];
        auto next = [&]() -> std::string {
            if (i + 1 >= argc) {
                std::fprintf(stderr, "loadgen: %s needs a value\n", argv[i]);
                std::exit(2);
            }
            return argv[++i];
        };
        try {
            if (a == "--embedded")
                o.embedded = true;
            else if (a == "--host")
                o.host = next();
            else if (a == "--port")
                o.port = static_cast<std::uint16_t>(std::stoul(next()));
            else if (a == "--clients")
                o.clients = std::stoi(next());
            else if (a == "--slow")
                o.slow = std::stoi(next());
            else if (a == "--slow-ms")
                o.slow_ms = std::stoi(next());
            else if (a == "--symbols")
                o.symbols = std::stoull(next());
            else if (a == "--per-client")
                o.per_client = std::stoull(next());
            else if (a == "--rate")
                o.rate = std::stod(next());
            else if (a == "--duration")
                o.duration = std::stod(next());
            else if (a == "--warmup")
                o.warmup = std::stod(next());
            else if (a == "--io-threads")
                o.io_threads = std::stoi(next());
            else if (a == "--gateway-io-threads")
                o.gateway_io_threads = std::stoi(next());
            else if (a == "--send-buffer")
                o.send_buffer = std::stoi(next());
            else if (a == "--busy-poll")
                o.busy_poll = true;
            else if (a == "--help" || a == "-h") {
                usage();
                std::exit(0);
            } else {
                std::fprintf(stderr, "loadgen: unknown option %s\n", argv[i]);
                usage();
                std::exit(2);
            }
        } catch (const std::exception&) {
            std::fprintf(stderr, "loadgen: bad value for %s\n", std::string(a).c_str());
            std::exit(2);
        }
    }
    if (o.symbols == 0 || o.clients <= 0 || o.slow < 0 || o.slow > o.clients) {
        std::fprintf(stderr, "loadgen: need symbols > 0 and 0 <= slow <= clients\n");
        std::exit(2);
    }
    if (o.per_client == 0 || o.per_client > o.symbols)
        o.per_client = o.symbols;
    return o;
}

std::string us(std::int64_t ns) {
    return std::format("{:.1f}", static_cast<double>(ns) / 1000.0);
}

void print_latency(const char* label, const fmdg::LatencyHistogram& h) {
    if (h.count() == 0) {
        std::printf("  %-34s (no samples)\n", label);
        return;
    }
    std::printf("  %-34s p50 %8s   p99 %8s   p99.9 %8s   max %9s  us\n", label, us(h.percentile(0.5)).c_str(),
                us(h.percentile(0.99)).c_str(), us(h.percentile(0.999)).c_str(), us(h.max()).c_str());
}

} // namespace

int main(int argc, char** argv) {
    const Options o = parse(argc, argv);

    std::unique_ptr<fmdg::Gateway> gateway;
    std::unique_ptr<fmdg::SyntheticFeed> feed;
    std::uint16_t port = o.port;
    if (o.embedded) {
        fmdg::GatewayConfig gc;
        gc.push_port = 0;
        gc.request_port = 0;
        gc.io_threads = o.gateway_io_threads;
        gc.busy_poll = o.busy_poll;
        gc.send_buffer = o.send_buffer;
        gateway = std::make_unique<fmdg::Gateway>(gc);
        gateway->start();
        port = gateway->push_port();
        fmdg::SyntheticFeedConfig fc;
        fc.symbols = o.symbols;
        fc.rate = o.rate;
        fc.prefix = o.prefix;
        feed = std::make_unique<fmdg::SyntheticFeed>(*gateway, fc);
        feed->start();
    }

    asio::io_context io;
    auto work = asio::make_work_guard(io);
    std::vector<std::thread> threads;
    for (int i = 0; i < std::max(1, o.io_threads); ++i)
        threads.emplace_back([&io] { io.run(); });

    Group fast, slow;
    const tcp::endpoint ep(asio::ip::make_address(o.host), port);
    std::vector<std::shared_ptr<LoadClient>> clients;
    for (int c = 0; c < o.clients; ++c) {
        std::vector<std::string> syms;
        for (std::size_t j = 0; j < o.per_client; ++j)
            syms.push_back(std::format("{}{:04}", o.prefix, (static_cast<std::size_t>(c) * o.per_client + j) % o.symbols));
        const bool is_slow = c < o.slow;
        clients.push_back(std::make_shared<LoadClient>(io, ep, std::move(syms), is_slow ? slow : fast,
                                                       is_slow ? o.slow_ms : 0));
        clients.back()->start();
    }

    const auto subscribe_deadline = std::chrono::steady_clock::now() + 10s;
    while (g_subscribed.load() < o.clients && std::chrono::steady_clock::now() < subscribe_deadline)
        std::this_thread::sleep_for(10ms);
    if (g_subscribed.load() < o.clients)
        std::fprintf(stderr, "loadgen: only %d of %d clients subscribed\n", g_subscribed.load(), o.clients);

    std::this_thread::sleep_for(std::chrono::duration<double>(o.warmup));
    const fmdg::GatewayStats before = gateway ? gateway->stats() : fmdg::GatewayStats{};
    const std::uint64_t produced_before = feed ? feed->produced() : 0;
    const auto t0 = std::chrono::steady_clock::now();
    g_measuring = true;
    std::this_thread::sleep_for(std::chrono::duration<double>(o.duration));
    g_measuring = false;
    const double secs = std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count();
    const fmdg::GatewayStats after = gateway ? gateway->stats() : fmdg::GatewayStats{};
    const std::uint64_t produced = feed ? feed->produced() - produced_before : 0;

    for (auto& c : clients)
        c->stop();
    work.reset();
    for (auto& t : threads)
        t.join();
    if (feed)
        feed->stop();
    if (gateway)
        gateway->stop();

    // ── report ──────────────────────────────────────────────────────────────
    const int n_fast = o.clients - o.slow;
    const double share = static_cast<double>(o.per_client) / static_cast<double>(o.symbols);
    std::printf("\nfmdg loadgen: %d clients (%d fast, %d slow @ %d ms/read), %zu symbols, %zu per client, %.1f s\n",
                o.clients, n_fast, o.slow, o.slow_ms, o.symbols, o.per_client, secs);
    if (o.embedded) {
        std::printf("  feed                               %.0f ticks/s%s\n", static_cast<double>(produced) / secs,
                    o.rate > 0 ? std::format(" (target {:.0f})", o.rate).c_str() : " (unpaced)");
        std::printf("  gateway in / deliveries / out      %.0f / %.0f / %.0f per s\n",
                    static_cast<double>(after.ticks_in - before.ticks_in) / secs,
                    static_cast<double>(after.deliveries - before.deliveries) / secs,
                    static_cast<double>(after.ticks_out - before.ticks_out) / secs);
        std::printf("  conflated (all clients)            %llu    ingest waits (ring full): %llu\n",
                    static_cast<unsigned long long>(after.conflated - before.conflated),
                    static_cast<unsigned long long>(after.ingest_waits - before.ingest_waits));
    }
    auto group_line = [&](const char* name, const Group& g, int n) {
        if (n == 0)
            return;
        const double per_client = static_cast<double>(g.ticks.load()) / secs / n;
        std::printf("  %-5s clients received               %.0f ticks/s each", name, per_client);
        if (o.embedded && produced > 0)
            std::printf("  (%.1f%% of their symbols' ticks)",
                        100.0 * per_client / (static_cast<double>(produced) / secs * share));
        std::printf("\n");
    };
    group_line("fast", fast, n_fast);
    group_line("slow", slow, o.slow);
    print_latency("fast: ingest -> client receive", fast.e2e);
    print_latency("fast: ingest -> gateway send", fast.internal);
    if (o.slow > 0)
        print_latency("slow: ingest -> client receive", slow.e2e);
    const std::uint64_t backwards = fast.backwards.load() + slow.backwards.load();
    const std::uint64_t errors = fast.errors.load() + slow.errors.load();
    std::printf("  ordering violations                %llu    errors: %llu\n", static_cast<unsigned long long>(backwards),
                static_cast<unsigned long long>(errors));
    return backwards == 0 && errors == 0 ? 0 : 1;
}
