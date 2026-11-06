// gateway/include/fmdg/gateway.h
#pragma once

#include "fmdg/symbol_table.h"
#include "fmdg/tick.h"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

namespace fmdg {

struct GatewayConfig {
    std::string bind_address = "127.0.0.1";
    std::uint16_t push_port = 8589;    ///< long connections; 0 = any free port
    std::uint16_t request_port = 8588; ///< short connections; 0 = any free port
    int io_threads = 2;
    std::size_t ring_capacity = 1 << 16;
    std::size_t max_batch = 256; ///< ticks per socket write
    std::size_t max_clients = 10'000;
    std::size_t max_symbols = 100'000;
    /// SO_SNDBUF for push connections; 0 keeps the OS default. Conflation bounds
    /// what the gateway queues for a slow client, but not what the kernel has
    /// already buffered: a smaller send buffer means a slow client's data is
    /// fresher once it reads, at the cost of more, smaller writes.
    int send_buffer = 0;
    bool busy_poll = false; ///< distributor never sleeps: lowest latency, one core busy
    /// Worker threads for commands added with add_command() (database queries);
    /// started only if any command is added.
    int command_threads = 4;
    /// How long a request-port client waits for such a command before
    /// "ERR timed out" (the I/O-side wait; the command itself is not killed).
    std::chrono::milliseconds command_timeout{30'000};
};

/// A request-port command that does slow work, a database query for example.
/// It runs on a worker thread, never on an I/O thread, and calls `reply` once,
/// from any thread; later calls are ignored.
using AsyncCommand = std::function<void(std::string_view line, std::function<void(std::string)> reply)>;

struct GatewayStats {
    std::uint64_t clients = 0;      ///< push connections open now
    std::uint64_t ticks_in = 0;     ///< ticks taken off the ingest ring
    std::uint64_t deliveries = 0;   ///< ticks handed to client queues (live fan-out + subscribe snapshots)
    std::uint64_t ticks_out = 0;    ///< tick frames written to sockets
    std::uint64_t conflated = 0;    ///< ticks replaced by a newer one before they were sent
    std::uint64_t ingest_waits = 0; ///< times ingest() found the ring full and waited
    std::uint64_t bytes_out = 0;
    std::uint64_t protocol_errors = 0;
    std::uint64_t rejected_clients = 0;
    std::size_t symbols = 0;
    std::size_t backlog = 0; ///< ticks waiting in the ingest ring
    // Inside the gateway: ingest() → frame handed to the socket, live ticks only.
    std::uint64_t latency_samples = 0;
    std::int64_t latency_p50_ns = 0;
    std::int64_t latency_p99_ns = 0;
    std::int64_t latency_p999_ns = 0;
    std::int64_t latency_max_ns = 0;
};

/// Market-data gateway: one feed in, many TCP clients out.
///
///   push port (8589)     a client subscribes to symbols (protocol.h); the gateway
///                        answers with symbol ids, sends each symbol's current
///                        value, then pushes every update. Slow clients are
///                        conflated per symbol (ConflatingQueue), never queued
///                        without bound, and never slow anyone else down.
///   request port (8588)  one line of text in, a text reply out, connection
///                        closed: `PING`, `SNAPSHOT <sym>...`, `STATS`.
///
/// Threads: the feed calls ingest() (one producer); a distributor thread fans
/// ticks out; `io_threads` threads run all socket I/O, each connection on its own
/// strand, so one connection's handlers never run concurrently.
class Gateway {
  public:
    explicit Gateway(GatewayConfig config = {});
    ~Gateway();

    Gateway(const Gateway&) = delete;
    Gateway& operator=(const Gateway&) = delete;

    /// Binds both ports and starts the threads. Throws std::system_error when a
    /// port cannot be bound.
    void start();
    /// Closes every connection and joins the threads. Idempotent; final (a
    /// stopped gateway does not restart). Call it from outside the gateway's own
    /// I/O handlers: it waits for them. Stop the feed first: ingest() must not
    /// race stop().
    void stop();

    /// Single producer. Stamps the sequence number (and ingest time, if unset),
    /// then hands the tick to the distributor, waiting while the ring is full.
    void ingest(Tick t);

    SymbolTable& symbols();
    /// Called whenever the set of symbols some push client is subscribed to
    /// changes, so an upstream feed can follow what clients ask for. Runs on an
    /// I/O thread and must only take a note (post to its own thread); nullptr
    /// removes it, and after that returns it is not running. One listener.
    void set_demand_listener(std::function<void()> listener);
    /// The symbols at least one push client is subscribed to now.
    std::vector<std::string> demanded_symbols() const;
    std::uint16_t push_port() const;    ///< the bound port (resolves 0)
    std::uint16_t request_port() const; ///< the bound port (resolves 0)
    GatewayStats stats() const;

    /// One request-port line → its reply, for the built-in commands (public so
    /// tests need no socket).
    std::string handle_command(std::string_view line);

    /// Adds a request-port command, matched on its first word (case-insensitive).
    /// Before start(); throws std::logic_error after it.
    void add_command(std::string verb, AsyncCommand command);

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace fmdg
