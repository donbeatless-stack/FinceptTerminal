// gateway/include/fmdg/spsc_ring.h
#pragma once

#include <atomic>
#include <cstddef>
#include <type_traits>
#include <vector>

namespace fmdg {

#if defined(_MSC_VER)
#pragma warning(push)
#pragma warning(disable : 4324) // "padded due to alignment specifier": the padding is the point
#endif

/// Bounded single-producer / single-consumer ring. One thread calls try_push, one
/// thread calls try_pop; both are wait-free and share nothing but two indices.
///
/// Each index, and each side's cached copy of the other side's index, has a cache
/// line of its own. A push normally touches only producer-owned lines (head_,
/// tail_cache_) and a pop only consumer-owned ones (tail_, head_cache_): the
/// shared index is re-read (acquire) only when the cached copy says the ring
/// looks full (producer) or empty (consumer). That avoids the cache-line
/// ping-pong a naive head/tail pair causes under load.
template <class T>
class SpscRing {
    static_assert(std::is_trivially_copyable_v<T>, "SpscRing copies elements by value");

  public:
    /// Capacity is rounded up to a power of two (at least 2) so the slot index is
    /// a mask, not a modulo.
    explicit SpscRing(std::size_t min_capacity) : buf_(round_up(min_capacity)), mask_(buf_.size() - 1) {}

    SpscRing(const SpscRing&) = delete;
    SpscRing& operator=(const SpscRing&) = delete;

    /// Producer thread only. False when the ring is full.
    bool try_push(const T& value) {
        const std::size_t head = head_.load(std::memory_order_relaxed);
        if (head - tail_cache_ == buf_.size()) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (head - tail_cache_ == buf_.size())
                return false;
        }
        buf_[head & mask_] = value;
        head_.store(head + 1, std::memory_order_release); // publishes the slot write
        return true;
    }

    /// Consumer thread only. False when the ring is empty.
    bool try_pop(T& out) {
        const std::size_t tail = tail_.load(std::memory_order_relaxed);
        if (tail == head_cache_) {
            head_cache_ = head_.load(std::memory_order_acquire);
            if (tail == head_cache_)
                return false;
        }
        out = buf_[tail & mask_];
        tail_.store(tail + 1, std::memory_order_release); // hands the slot back
        return true;
    }

    std::size_t capacity() const { return buf_.size(); }

    /// Any thread; exact only while both sides are idle. Tail is read first: head
    /// only grows, so the difference can never go negative.
    std::size_t size_approx() const {
        const std::size_t tail = tail_.load(std::memory_order_acquire);
        const std::size_t head = head_.load(std::memory_order_acquire);
        return head - tail;
    }

  private:
    static std::size_t round_up(std::size_t n) {
        std::size_t c = 2;
        while (c < n)
            c <<= 1;
        return c;
    }

    std::vector<T> buf_;
    const std::size_t mask_;
    alignas(64) std::atomic<std::size_t> head_{0}; ///< next slot to write — written by the producer
    alignas(64) std::size_t head_cache_ = 0;        ///< consumer's last view of head_
    alignas(64) std::atomic<std::size_t> tail_{0}; ///< next slot to read — written by the consumer
    alignas(64) std::size_t tail_cache_ = 0;        ///< producer's last view of tail_
};

#if defined(_MSC_VER)
#pragma warning(pop)
#endif

} // namespace fmdg
