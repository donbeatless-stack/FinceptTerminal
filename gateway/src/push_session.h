// gateway/src/push_session.h — internal
#pragma once

#include "fmdg/conflating_queue.h"
#include "fmdg/distributor.h"
#include "fmdg/latency_histogram.h"
#include "fmdg/protocol.h"
#include "fmdg/symbol_table.h"
#include "registry.h"

#include <asio.hpp>

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string_view>
#include <vector>

namespace fmdg::detail {

struct Counters {
    std::atomic<std::uint64_t> clients{0};
    std::atomic<std::uint64_t> rejected_clients{0};
    std::atomic<std::uint64_t> ticks_out{0};
    std::atomic<std::uint64_t> bytes_out{0};
    std::atomic<std::uint64_t> conflated_closed{0}; ///< from sessions already gone
    std::atomic<std::uint64_t> protocol_errors{0};
};

class PushSession;
using SessionRegistry = Registry<PushSession>;

struct SessionContext {
    SymbolTable& symbols;
    Distributor& distributor;
    LatencyHistogram& latency;
    Counters& counters;
    SessionRegistry& registry;
    std::size_t max_batch;
};

/// One long connection on the push port. The socket's executor is a strand: the
/// read loop, the write loop and close all run on it, one at a time. deliver()
/// is the only entry point from another thread (the distributor's); it queues
/// the tick and, if no write is running, posts one onto the strand.
///
/// At most one async_write is in flight. `writing_` is true from the moment a
/// write is scheduled until write_next() finds nothing left; a deliver() that
/// sees it true knows the running write loop will pick its tick up.
class PushSession final : public Subscriber, public std::enable_shared_from_this<PushSession> {
  public:
    PushSession(asio::ip::tcp::socket socket, SessionContext& ctx);

    void start();
    void deliver(const Tick& t) override;
    /// Any thread, idempotent.
    void close();

    std::uint64_t conflated() const { return queue_.conflated(); }
    std::size_t queued() const { return queue_.size(); }

  private:
    void read_header();
    void read_body(proto::Header h);
    void handle(proto::MsgType type);
    void on_symbols(proto::MsgType type);
    void fail(proto::ErrorCode code, std::string_view why);
    void wake();
    void write_next();

    asio::ip::tcp::socket socket_;
    SessionContext& ctx_;
    ConflatingQueue queue_;
    std::atomic<bool> writing_{false};
    std::atomic<bool> closed_{false};
    bool closing_ = false; ///< strand only: an error is queued, close once flushed

    std::array<std::uint8_t, proto::kHeaderSize> header_{};
    proto::Bytes body_;
    proto::Bytes control_; ///< strand only: replies not yet written
    proto::Bytes out_;     ///< strand only: the buffer of the write in flight
    std::vector<Tick> batch_;
};

} // namespace fmdg::detail
