// gateway/include/fmdg/conflating_queue.h
#pragma once

#include "fmdg/tick.h"

#include <cstddef>
#include <cstdint>
#include <deque>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace fmdg {

/// Outbound ticks for ONE client, conflated per symbol: while a symbol's tick is
/// still waiting to be written, a newer tick for that symbol overwrites it in
/// place instead of queueing behind it.
///
/// That is the standard way market-data systems handle a slow consumer. Its
/// backlog can never exceed one entry per symbol it follows — so a client that
/// stops reading costs bounded memory, never blocks the fan-out to anyone else,
/// and on recovery receives the latest value of every symbol rather than minutes
/// of stale history. A client that keeps up sees every tick.
///
/// Thread-safe: the distributor thread pushes, the client's network strand drains.
class ConflatingQueue {
  public:
    /// Queues `t`, or overwrites the tick already waiting for t.symbol (keeping
    /// that symbol's place in line). Returns true when the queue was empty, i.e.
    /// the writer may be idle and needs waking.
    bool push(const Tick& t);

    /// Moves up to `max` ticks into `out` (appended), in the order their symbols
    /// became pending. Returns how many were moved.
    std::size_t drain(std::vector<Tick>& out, std::size_t max);

    std::size_t size() const;
    /// Ticks overwritten before they were written.
    std::uint64_t conflated() const;
    /// Largest size() ever reached.
    std::size_t high_water() const;

  private:
    struct Slot {
        Tick tick;
        bool pending = false;
    };

    mutable std::mutex mu_;
    std::unordered_map<SymbolId, std::uint32_t> slot_of_; ///< symbol → index into slots_
    std::vector<Slot> slots_;                              ///< one per symbol ever queued
    std::deque<std::uint32_t> order_;                      ///< pending slots, oldest first
    std::uint64_t conflated_ = 0;
    std::size_t high_water_ = 0;
};

} // namespace fmdg
