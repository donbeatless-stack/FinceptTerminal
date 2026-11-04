// gateway/src/gateway.cpp
#include "fmdg/gateway.h"

#include "fmdg/distributor.h"
#include "fmdg/latency_histogram.h"
#include "fmdg/protocol.h"
#include "push_session.h"
#include "request_session.h"

#include <asio.hpp>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <format>
#include <future>
#include <mutex>
#include <optional>
#include <thread>
#include <utility>
#include <vector>

namespace fmdg {

using asio::ip::tcp;

namespace {

void open_acceptor(tcp::acceptor& acceptor, const tcp::endpoint& ep) {
    acceptor.open(ep.protocol());
#if !defined(_WIN32)
    // On Windows SO_REUSEADDR would let a second process bind the same port
    // and steal connections; its default is already what we want there.
    acceptor.set_option(tcp::acceptor::reuse_address(true));
#endif
    acceptor.bind(ep);
    acceptor.listen(asio::socket_base::max_listen_connections);
}

std::vector<std::string_view> split_words(std::string_view line) {
    std::vector<std::string_view> words;
    std::size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && (line[i] == ' ' || line[i] == '\t'))
            ++i;
        const std::size_t start = i;
        while (i < line.size() && line[i] != ' ' && line[i] != '\t')
            ++i;
        if (i > start)
            words.push_back(line.substr(start, i - start));
    }
    return words;
}

std::string upper(std::string_view s) {
    std::string out(s);
    for (char& c : out)
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    return out;
}

} // namespace

struct Gateway::Impl {
    explicit Impl(GatewayConfig c)
        : config(std::move(c)), push_acceptor(asio::make_strand(io)), request_acceptor(asio::make_strand(io)),
          push_retry(push_acceptor.get_executor()), request_retry(request_acceptor.get_executor()),
          symbols(config.max_symbols), distributor(config.ring_capacity),
          ctx{symbols, distributor, latency, counters, push_sessions, std::max<std::size_t>(config.max_batch, 1)} {}

    void accept_push();
    void accept_request();
    void rearm(tcp::acceptor& acceptor, asio::steady_timer& retry, const asio::error_code& ec, void (Impl::*accept)());
    GatewayStats stats() const;
    std::string handle_command(std::string_view line);

    const GatewayConfig config;

    // Declared before everything that holds sockets, timers or handlers, so it
    // is destroyed after them: their teardown needs it alive.
    asio::io_context io;
    std::optional<asio::executor_work_guard<asio::io_context::executor_type>> work;
    std::vector<std::thread> io_threads;
    tcp::acceptor push_acceptor;
    tcp::acceptor request_acceptor;
    asio::steady_timer push_retry; ///< accept back-off, on the acceptor's strand
    asio::steady_timer request_retry;

    SymbolTable symbols;
    Distributor distributor;
    LatencyHistogram latency;
    detail::Counters counters;
    detail::Registry<detail::PushSession> push_sessions;
    detail::Registry<detail::RequestSession> request_sessions;
    detail::SessionContext ctx;

    std::uint64_t sequence = 0; ///< producer thread only
    std::atomic<std::uint64_t> ingest_waits{0};
    std::atomic<bool> running{false};
    std::atomic<bool> stopping{false};

    std::mutex lifecycle_mu;
    bool started = false;
    bool stopped = false;
    std::uint16_t bound_push_port = 0;
    std::uint16_t bound_request_port = 0;
};

// Accept handlers run on their acceptor's strand. Gateway::stop() closes each
// acceptor on that same strand after setting `stopping`, so a connection is
// either registered before stop() lists the sessions or turned away.
void Gateway::Impl::accept_push() {
    push_acceptor.async_accept(asio::make_strand(io), [this](const asio::error_code& ec, tcp::socket socket) {
        if (ec == asio::error::operation_aborted || !push_acceptor.is_open())
            return;
        if (!ec) {
            if (stopping.load(std::memory_order_acquire)) {
                return; // socket closes as it goes out of scope
            }
            if (counters.clients.load(std::memory_order_relaxed) >= config.max_clients) {
                counters.rejected_clients.fetch_add(1, std::memory_order_relaxed);
            } else {
                asio::error_code ignored;
                socket.set_option(tcp::no_delay(true), ignored);
                if (config.send_buffer > 0)
                    socket.set_option(asio::socket_base::send_buffer_size(config.send_buffer), ignored);
                auto session = std::make_shared<detail::PushSession>(std::move(socket), ctx);
                push_sessions.add(session);
                session->start();
            }
        }
        rearm(push_acceptor, push_retry, ec, &Impl::accept_push);
    });
}

void Gateway::Impl::accept_request() {
    request_acceptor.async_accept(asio::make_strand(io), [this](const asio::error_code& ec, tcp::socket socket) {
        if (ec == asio::error::operation_aborted || !request_acceptor.is_open())
            return;
        if (!ec && !stopping.load(std::memory_order_acquire)) {
            auto session = std::make_shared<detail::RequestSession>(
                std::move(socket), [this](std::string_view line) { return handle_command(line); }, request_sessions);
            request_sessions.add(session);
            session->start();
        }
        rearm(request_acceptor, request_retry, ec, &Impl::accept_request);
    });
}

// A failed accept must not stop the server, but some failures persist (the
// process is out of socket handles), and re-arming at once would spin a core
// on them. So after an error, wait briefly before accepting again.
void Gateway::Impl::rearm(tcp::acceptor& acceptor, asio::steady_timer& retry, const asio::error_code& ec,
                          void (Impl::*accept)()) {
    if (!ec) {
        (this->*accept)();
        return;
    }
    retry.expires_after(std::chrono::milliseconds(50));
    retry.async_wait([this, &acceptor, accept](const asio::error_code& e) {
        if (!e && acceptor.is_open())
            (this->*accept)();
    });
}

Gateway::Gateway(GatewayConfig config) : impl_(std::make_unique<Impl>(std::move(config))) {}

Gateway::~Gateway() {
    stop();
}

void Gateway::start() {
    Impl& im = *impl_;
    std::lock_guard lock(im.lifecycle_mu);
    if (im.started)
        return;
    const auto address = asio::ip::make_address(im.config.bind_address);
    open_acceptor(im.push_acceptor, {address, im.config.push_port});
    open_acceptor(im.request_acceptor, {address, im.config.request_port});
    im.bound_push_port = im.push_acceptor.local_endpoint().port();
    im.bound_request_port = im.request_acceptor.local_endpoint().port();

    im.distributor.start(im.config.busy_poll);
    im.running.store(true, std::memory_order_release);
    im.accept_push();
    im.accept_request();
    im.work.emplace(asio::make_work_guard(im.io));
    const int n = std::max(1, im.config.io_threads);
    for (int i = 0; i < n; ++i)
        im.io_threads.emplace_back([&im] { im.io.run(); });
    im.started = true;
}

void Gateway::stop() {
    Impl& im = *impl_;
    std::lock_guard lock(im.lifecycle_mu);
    if (!im.started || im.stopped)
        return;
    im.stopped = true;
    im.stopping.store(true, std::memory_order_release);
    im.running.store(false, std::memory_order_release);

    // 1. No new connections. Wait until each acceptor is closed on its strand.
    const std::pair<tcp::acceptor*, asio::steady_timer*> listeners[] = {{&im.push_acceptor, &im.push_retry},
                                                                         {&im.request_acceptor, &im.request_retry}};
    for (const auto& [acceptor, retry] : listeners) {
        std::promise<void> closed;
        asio::post(acceptor->get_executor(), [acceptor, retry, &closed] {
            asio::error_code ignored;
            acceptor->close(ignored);
            retry->cancel();
            closed.set_value();
        });
        closed.get_future().wait();
    }
    // 2. No new ticks: the distributor delivers what is already in the ring.
    im.distributor.stop();
    // 3. Close every connection; their pending reads, writes and timers complete
    //    with operation_aborted, and the I/O threads run out of work and return.
    for (const auto& s : im.push_sessions.live())
        s->close();
    for (const auto& s : im.request_sessions.live())
        s->close();
    im.work.reset();
    for (auto& t : im.io_threads)
        t.join();
    im.io_threads.clear();
}

void Gateway::ingest(Tick t) {
    Impl& im = *impl_;
    t.sequence = ++im.sequence;
    if (t.ingest_ts_ns == 0)
        t.ingest_ts_ns = now_ns();
    if (im.distributor.publish(t))
        return;
    // Ring full: the distributor is behind. Back-pressure the feed rather than
    // drop — a feed handler that can't keep up must see it, not lose data silently.
    im.ingest_waits.fetch_add(1, std::memory_order_relaxed);
    while (!im.distributor.publish(t)) {
        if (!im.running.load(std::memory_order_acquire))
            return; // stopped (or never started): nobody will drain the ring
        std::this_thread::yield();
    }
}

SymbolTable& Gateway::symbols() {
    return impl_->symbols;
}

std::uint16_t Gateway::push_port() const {
    return impl_->bound_push_port;
}

std::uint16_t Gateway::request_port() const {
    return impl_->bound_request_port;
}

GatewayStats Gateway::stats() const {
    return impl_->stats();
}

std::string Gateway::handle_command(std::string_view line) {
    return impl_->handle_command(line);
}

GatewayStats Gateway::Impl::stats() const {
    const Impl& im = *this;
    GatewayStats s;
    s.clients = im.counters.clients.load(std::memory_order_relaxed);
    s.ticks_in = im.distributor.ticks_in();
    s.deliveries = im.distributor.deliveries();
    s.ticks_out = im.counters.ticks_out.load(std::memory_order_relaxed);
    // A session that closes between these two reads is counted twice; the
    // figure is a monitoring number, exact once the gateway is quiet.
    s.conflated = im.counters.conflated_closed.load(std::memory_order_relaxed);
    for (const auto& session : im.push_sessions.live())
        s.conflated += session->conflated();
    s.ingest_waits = im.ingest_waits.load(std::memory_order_relaxed);
    s.bytes_out = im.counters.bytes_out.load(std::memory_order_relaxed);
    s.protocol_errors = im.counters.protocol_errors.load(std::memory_order_relaxed);
    s.rejected_clients = im.counters.rejected_clients.load(std::memory_order_relaxed);
    s.symbols = im.symbols.size();
    s.backlog = im.distributor.backlog();
    s.latency_samples = im.latency.count();
    s.latency_p50_ns = im.latency.percentile(0.50);
    s.latency_p99_ns = im.latency.percentile(0.99);
    s.latency_p999_ns = im.latency.percentile(0.999);
    s.latency_max_ns = im.latency.max();
    return s;
}

// Request-port protocol: one line in, one reply out.
//   PING                 → PONG
//   SNAPSHOT <sym>...    → one line per symbol: "<sym> <seq> <price> <size> <exchange_ts_ns>",
//                          or "<sym> NA" when the gateway has no value for it yet
//   STATS                → one "key=value" line per GatewayStats field
// Anything else → "ERR ..." . Replies end with '\n'.
std::string Gateway::Impl::handle_command(std::string_view line) {
    const auto words = split_words(line);
    if (words.empty())
        return "ERR empty request\n";
    const std::string cmd = upper(words[0]);

    if (cmd == "PING")
        return "PONG\n";

    if (cmd == "SNAPSHOT") {
        if (words.size() < 2)
            return "ERR usage: SNAPSHOT <symbol>...\n";
        if (words.size() - 1 > proto::kMaxSymbolsPerRequest)
            return "ERR too many symbols\n";
        std::string out;
        for (std::size_t i = 1; i < words.size(); ++i) {
            const std::string_view sym = words[i];
            if (!proto::valid_symbol(sym))
                return "ERR invalid symbol\n";
            std::optional<Tick> t;
            if (const auto id = symbols.find(sym))
                t = distributor.last(*id);
            if (t)
                out += std::format("{} {} {} {} {}\n", sym, t->sequence, t->price, t->size, t->exchange_ts_ns);
            else
                out += std::format("{} NA\n", sym);
        }
        return out;
    }

    if (cmd == "STATS") {
        const GatewayStats s = stats();
        return std::format("clients={}\nticks_in={}\ndeliveries={}\nticks_out={}\nconflated={}\n"
                           "ingest_waits={}\nbytes_out={}\nprotocol_errors={}\nrejected_clients={}\n"
                           "symbols={}\nbacklog={}\nlatency_samples={}\nlatency_p50_ns={}\n"
                           "latency_p99_ns={}\nlatency_p999_ns={}\nlatency_max_ns={}\n",
                           s.clients, s.ticks_in, s.deliveries, s.ticks_out, s.conflated, s.ingest_waits,
                           s.bytes_out, s.protocol_errors, s.rejected_clients, s.symbols, s.backlog,
                           s.latency_samples, s.latency_p50_ns, s.latency_p99_ns, s.latency_p999_ns,
                           s.latency_max_ns);
    }

    return "ERR unknown command\n";
}

} // namespace fmdg
