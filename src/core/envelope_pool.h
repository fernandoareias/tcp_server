#ifndef ENVELOPE_POOL_H
#define ENVELOPE_POOL_H


#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <vector>

inline constexpr size_t kEnvelopeInlineCapacity = 512;

struct Envelope {
    uint64_t connId      = 0;
    uint32_t originShard = 0;
    uint32_t sessionId   = 0;
    uint32_t streamId    = 0;

    std::span<const uint8_t> payload() const {
        return overflow_.empty() ? std::span<const uint8_t>(inline_.data(), inlineLen_)
                                  : std::span<const uint8_t>(overflow_);
    }
    void setPayload(std::span<const uint8_t> src) {
        if (src.size() <= kEnvelopeInlineCapacity) {
            if (!src.empty()) std::memcpy(inline_.data(), src.data(), src.size());
            inlineLen_ = uint32_t(src.size());
            overflow_.clear();
        } else {
            overflow_.assign(src.begin(), src.end());
            inlineLen_ = 0;
        }
    }

private:
    friend class EnvelopePool;

    std::array<uint8_t, kEnvelopeInlineCapacity> inline_{};
    uint32_t             inlineLen_ = 0;
    std::vector<uint8_t> overflow_;
    Envelope*            poolNext_ = nullptr;
    bool                 pooled_   = false;
};

class EnvelopePool {
public:
    explicit EnvelopePool(size_t capacity) : storage_(capacity) {
        for (auto& e : storage_) {
            e.pooled_ = true;
            release(&e);
        }
    }
    EnvelopePool(const EnvelopePool&) = delete;
    EnvelopePool& operator=(const EnvelopePool&) = delete;

    Envelope* acquire() {
        Envelope* head = head_.load(std::memory_order_acquire);
        while (head) {
            Envelope* next = head->poolNext_;
            if (head_.compare_exchange_weak(head, next, std::memory_order_acq_rel,
                                             std::memory_order_acquire))
                return head;
        }
        Envelope* e = new Envelope();
        e->pooled_ = false;
        return e;
    }

    void release(Envelope* e) {
        e->inlineLen_ = 0;
        e->overflow_.clear();
        if (!e->pooled_) { delete e; return; }
        Envelope* head = head_.load(std::memory_order_relaxed);
        do {
            e->poolNext_ = head;
        } while (!head_.compare_exchange_weak(head, e, std::memory_order_release,
                                               std::memory_order_relaxed));
    }

private:
    std::vector<Envelope>  storage_;
    std::atomic<Envelope*> head_{nullptr};
};


struct EnvelopeDeleter {
    EnvelopePool* pool;
    void operator()(Envelope* e) const noexcept { pool->release(e); }
};
using EnvelopePtr = std::unique_ptr<Envelope, EnvelopeDeleter>;


#endif // ENVELOPE_POOL_H
