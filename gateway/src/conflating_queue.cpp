// gateway/src/conflating_queue.cpp
#include "fmdg/conflating_queue.h"

#include <algorithm>

namespace fmdg {

bool ConflatingQueue::push(const Tick& t) {
    std::lock_guard lock(mu_);
    const auto [it, inserted] = slot_of_.try_emplace(t.symbol, static_cast<std::uint32_t>(slots_.size()));
    if (inserted)
        slots_.emplace_back();
    Slot& slot = slots_[it->second];
    slot.tick = t;
    if (slot.pending) {
        ++conflated_; // replaced in place; its turn in order_ is unchanged
        return false;
    }
    const bool was_empty = order_.empty();
    slot.pending = true;
    order_.push_back(it->second);
    high_water_ = std::max(high_water_, order_.size());
    return was_empty;
}

std::size_t ConflatingQueue::drain(std::vector<Tick>& out, std::size_t max) {
    std::lock_guard lock(mu_);
    std::size_t moved = 0;
    while (moved < max && !order_.empty()) {
        Slot& slot = slots_[order_.front()];
        order_.pop_front();
        slot.pending = false;
        out.push_back(slot.tick);
        ++moved;
    }
    return moved;
}

std::size_t ConflatingQueue::size() const {
    std::lock_guard lock(mu_);
    return order_.size();
}

std::uint64_t ConflatingQueue::conflated() const {
    std::lock_guard lock(mu_);
    return conflated_;
}

std::size_t ConflatingQueue::high_water() const {
    std::lock_guard lock(mu_);
    return high_water_;
}

} // namespace fmdg
