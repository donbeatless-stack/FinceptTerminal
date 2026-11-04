// gateway/include/fmdg/symbol_table.h
#pragma once

#include "fmdg/tick.h"

#include <cstddef>
#include <functional>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace fmdg {

/// Maps symbol names to dense ids (0, 1, 2, …) so the hot path indexes arrays
/// instead of hashing strings. Thread-safe: the feed and client sessions intern
/// concurrently. Capacity is capped, because clients can name symbols: without a
/// cap, a client subscribing to millions of made-up names would grow it forever.
class SymbolTable {
  public:
    explicit SymbolTable(std::size_t capacity = 100'000) : capacity_(capacity) {}

    /// The id of `name`, adding it if new; nullopt when the table is full.
    std::optional<SymbolId> intern(std::string_view name);
    std::optional<SymbolId> find(std::string_view name) const;
    /// Empty for an id never handed out.
    std::string name(SymbolId id) const;
    std::size_t size() const;
    std::size_t capacity() const { return capacity_; }

  private:
    struct Hash {
        using is_transparent = void;
        std::size_t operator()(std::string_view s) const { return std::hash<std::string_view>{}(s); }
    };

    const std::size_t capacity_;
    mutable std::shared_mutex mu_;
    std::unordered_map<std::string, SymbolId, Hash, std::equal_to<>> ids_;
    std::vector<std::string> names_;
};

} // namespace fmdg
