// gateway/src/symbol_table.cpp
#include "fmdg/symbol_table.h"

#include <mutex>

namespace fmdg {

std::optional<SymbolId> SymbolTable::intern(std::string_view name) {
    {
        std::shared_lock lock(mu_);
        if (const auto it = ids_.find(name); it != ids_.end())
            return it->second;
    }
    std::unique_lock lock(mu_);
    if (const auto it = ids_.find(name); it != ids_.end()) // added while we waited
        return it->second;
    if (names_.size() >= capacity_)
        return std::nullopt;
    const auto id = static_cast<SymbolId>(names_.size());
    names_.emplace_back(name);
    ids_.emplace(names_.back(), id);
    return id;
}

std::optional<SymbolId> SymbolTable::find(std::string_view name) const {
    std::shared_lock lock(mu_);
    if (const auto it = ids_.find(name); it != ids_.end())
        return it->second;
    return std::nullopt;
}

std::string SymbolTable::name(SymbolId id) const {
    std::shared_lock lock(mu_);
    return id < names_.size() ? names_[id] : std::string();
}

std::size_t SymbolTable::size() const {
    std::shared_lock lock(mu_);
    return names_.size();
}

} // namespace fmdg
