// gateway/src/distributor.cpp
#include "fmdg/distributor.h"

#include <algorithm>

#if defined(_M_X64) || defined(__x86_64__)
#include <immintrin.h>
#endif

namespace fmdg {

namespace {

void cpu_relax() {
#if defined(_M_X64) || defined(__x86_64__)
    _mm_pause();
#else
    std::this_thread::yield();
#endif
}

} // namespace

Distributor::Distributor(std::size_t ring_capacity, int spins_before_sleep)
    : ring_(ring_capacity), spins_before_sleep_(spins_before_sleep) {}

Distributor::~Distributor() {
    stop();
}

bool Distributor::publish(const Tick& t) {
    if (!ring_.try_push(t))
        return false;
    // Pairs with the fence in run(): either the consumer sees the new tick when it
    // re-checks the ring, or we see it asleep here and wake it (Dekker-style).
    std::atomic_thread_fence(std::memory_order_seq_cst);
    if (sleeping_.load(std::memory_order_relaxed)) {
        wake_.fetch_add(1, std::memory_order_relaxed);
        wake_.notify_one();
    }
    return true;
}

void Distributor::start(bool busy_poll) {
    if (running_.exchange(true))
        return;
    thread_ = std::thread([this, busy_poll] { run(busy_poll); });
}

void Distributor::stop() {
    if (!running_.exchange(false))
        return;
    wake_.fetch_add(1, std::memory_order_relaxed);
    wake_.notify_one();
    if (thread_.joinable())
        thread_.join();
}

void Distributor::run(bool busy_poll) {
    Tick t;
    int idle = 0;
    while (running_.load(std::memory_order_relaxed)) {
        if (ring_.try_pop(t)) {
            process(t);
            idle = 0;
            continue;
        }
        if (busy_poll || ++idle < spins_before_sleep_) {
            cpu_relax();
            continue;
        }
        const std::uint32_t seen = wake_.load(std::memory_order_relaxed);
        sleeping_.store(true, std::memory_order_relaxed);
        std::atomic_thread_fence(std::memory_order_seq_cst);
        if (ring_.size_approx() == 0 && running_.load(std::memory_order_relaxed))
            wake_.wait(seen); // returns at once if publish()/stop() bumped wake_ meanwhile
        sleeping_.store(false, std::memory_order_relaxed);
        idle = 0;
    }
    while (ring_.try_pop(t)) // deliver what was already published
        process(t);
}

std::size_t Distributor::drain_now() {
    Tick t;
    std::size_t n = 0;
    while (ring_.try_pop(t)) {
        process(t);
        ++n;
    }
    return n;
}

void Distributor::process(const Tick& t) {
    {
        std::lock_guard lock(last_mu_);
        if (t.symbol >= last_.size()) {
            last_.resize(static_cast<std::size_t>(t.symbol) + 1);
            has_last_.resize(static_cast<std::size_t>(t.symbol) + 1, 0);
        }
        last_[t.symbol] = t;
        has_last_[t.symbol] = 1;
    }
    ticks_in_.fetch_add(1, std::memory_order_relaxed);
    // last_ is updated BEFORE taking subs_mu_: a subscribe() that runs in
    // between then reads this tick as its snapshot, so a subscriber can never be
    // handed an older value after a newer one.
    std::shared_lock lock(subs_mu_);
    if (t.symbol >= subs_.size())
        return;
    const auto& list = subs_[t.symbol];
    for (const auto& sub : list)
        sub->deliver(t);
    deliveries_.fetch_add(list.size(), std::memory_order_relaxed);
}

void Distributor::subscribe(const std::shared_ptr<Subscriber>& sub, const std::vector<SymbolId>& ids) {
    std::unique_lock lock(subs_mu_);
    auto& mine = by_sub_[sub.get()];
    bool first_subscriber = false;
    for (const SymbolId id : ids) {
        if (id >= subs_.size())
            subs_.resize(static_cast<std::size_t>(id) + 1);
        auto& list = subs_[id];
        if (std::find(list.begin(), list.end(), sub) != list.end())
            continue;
        first_subscriber |= list.empty();
        list.push_back(sub);
        mine.push_back(id);
    }
    // Snapshots go out while subs_mu_ is still held, so no live tick for these
    // symbols can be fanned out to `sub` in between.
    std::uint64_t snapshots = 0;
    for (const SymbolId id : ids)
        if (auto t = last(id)) {
            t->flags |= kTickSnapshot;
            sub->deliver(*t);
            ++snapshots;
        }
    deliveries_.fetch_add(snapshots, std::memory_order_relaxed);
    lock.unlock();
    if (first_subscriber)
        notify_demand();
}

void Distributor::unsubscribe(const Subscriber* sub, const std::vector<SymbolId>& ids) {
    std::unique_lock lock(subs_mu_);
    const auto mine = by_sub_.find(sub);
    if (mine == by_sub_.end())
        return;
    bool last_subscriber = false;
    for (const SymbolId id : ids) {
        if (id < subs_.size() && std::erase_if(subs_[id], [sub](const auto& p) { return p.get() == sub; }) > 0)
            last_subscriber |= subs_[id].empty();
        std::erase(mine->second, id);
    }
    lock.unlock();
    if (last_subscriber)
        notify_demand();
}

void Distributor::remove(const Subscriber* sub) {
    std::unique_lock lock(subs_mu_);
    const auto mine = by_sub_.find(sub);
    if (mine == by_sub_.end())
        return;
    bool last_subscriber = false;
    for (const SymbolId id : mine->second)
        if (id < subs_.size() && std::erase_if(subs_[id], [sub](const auto& p) { return p.get() == sub; }) > 0)
            last_subscriber |= subs_[id].empty();
    by_sub_.erase(mine);
    lock.unlock();
    if (last_subscriber)
        notify_demand();
}

void Distributor::set_demand_listener(std::function<void()> listener) {
    std::lock_guard lock(listener_mu_);
    listener_ = std::move(listener);
}

void Distributor::notify_demand() {
    std::lock_guard lock(listener_mu_);
    if (listener_)
        listener_();
}

std::vector<SymbolId> Distributor::demanded() const {
    std::shared_lock lock(subs_mu_);
    std::vector<SymbolId> out;
    for (std::size_t id = 0; id < subs_.size(); ++id)
        if (!subs_[id].empty())
            out.push_back(static_cast<SymbolId>(id));
    return out;
}

std::optional<Tick> Distributor::last(SymbolId id) const {
    std::lock_guard lock(last_mu_);
    if (id < last_.size() && has_last_[id])
        return last_[id];
    return std::nullopt;
}

} // namespace fmdg
