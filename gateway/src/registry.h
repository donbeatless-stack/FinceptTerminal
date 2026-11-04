// gateway/src/registry.h — internal
#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace fmdg::detail {

/// The live sessions of one kind, so Gateway::stop() can close them all and
/// stats() can sum over them. Holds weak references: a session's lifetime is
/// owned by its pending I/O handlers, and it removes itself when it closes.
template <class T>
class Registry {
  public:
    void add(const std::shared_ptr<T>& s) {
        std::lock_guard lock(mu_);
        items_[s.get()] = s;
    }

    void remove(const T* s) {
        std::lock_guard lock(mu_);
        items_.erase(s);
    }

    std::vector<std::shared_ptr<T>> live() const {
        std::lock_guard lock(mu_);
        std::vector<std::shared_ptr<T>> out;
        out.reserve(items_.size());
        for (const auto& entry : items_)
            if (auto s = entry.second.lock())
                out.push_back(std::move(s));
        return out;
    }

    std::size_t size() const {
        std::lock_guard lock(mu_);
        return items_.size();
    }

  private:
    mutable std::mutex mu_;
    std::unordered_map<const T*, std::weak_ptr<T>> items_;
};

} // namespace fmdg::detail
