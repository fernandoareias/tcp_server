#ifndef SHARD_MESH_H
#define SHARD_MESH_H

#include "core/envelope_pool.h"
#include "concurrency/spsc_queue.h"

#include <cstdint>
#include <memory>
#include <vector>

#include <unistd.h>

struct ShardMessage {
    enum class Kind : uint8_t { Payload, Reply };
    Kind      kind = Kind::Payload;
    Envelope* env  = nullptr;
};

inline constexpr size_t kShardQueueCapacity = 4096;
using ShardQueue = SpscQueue<ShardMessage, kShardQueueCapacity>;

extern template class SpscQueue<ShardMessage, kShardQueueCapacity>;

class ShardMesh {
public:
    ShardMesh(uint32_t n, EnvelopePool& pool)
        : n_(n), pool_(pool), queues_(size_t(n) * n), wakeFds_(n, -1) {
        for (uint32_t a = 0; a < n; ++a)
            for (uint32_t b = 0; b < n; ++b)
                if (a != b) queues_[size_t(a) * n + b] = std::make_unique<ShardQueue>();
    }

    ~ShardMesh() {
        for (auto& q : queues_)
            if (q) q->consume_all([this](const ShardMessage& m) { pool_.release(m.env); });
    }

    ShardMesh(const ShardMesh&) = delete;
    ShardMesh& operator=(const ShardMesh&) = delete;

    uint32_t size() const { return n_; }

    ShardQueue& queue(uint32_t from, uint32_t to) { return *queues_[size_t(from) * n_ + to]; }

    void setWakeFd(uint32_t shard, int fd) { wakeFds_[shard] = fd; }

    void wake(uint32_t to) {
        uint64_t one = 1;
        ssize_t r = ::write(wakeFds_[to], &one, sizeof(one));
        (void)r;   // EAGAIN só ocorre se o contador saturar; o shard já está acordado
    }

private:
    uint32_t                                 n_;
    EnvelopePool&                            pool_;
    std::vector<std::unique_ptr<ShardQueue>> queues_;
    std::vector<int>                         wakeFds_;
};


#endif // SHARD_MESH_H
