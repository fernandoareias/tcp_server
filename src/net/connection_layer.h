#ifndef CONNECTION_LAYER_H
#define CONNECTION_LAYER_H

#include "protocol/protocol.h"

#include <cstdint>

struct ConnectionContext {
    bool     established = false;
    uint32_t sessionId   = 0;
};


class ConnectionLayer {
public:
    enum class Verdict {
        Consumed,
        Forward,
        Close,
        Reject,
    };

    struct Decision {
        Verdict  verdict;
        uint32_t targetShard = 0;
    };

    explicit ConnectionLayer(uint32_t shardCount) : shardCount_(shardCount) {}

    Decision onHeader(ConnectionContext& ctx, const proto::FrameHeader& h) const {
        using proto::FrameType;
        if (h.version != proto::kVersion) return {Verdict::Reject};

        switch (FrameType(h.type)) {
        case FrameType::Hello:
            if (ctx.established) return {Verdict::Reject};   // Hello duplicado
            ctx.established = true;
            ctx.sessionId   = h.sessionId;
            return {Verdict::Consumed};

        case FrameType::Data:
            if (!ctx.established || h.sessionId != ctx.sessionId) return {Verdict::Reject};
            // Afinidade: todo payload de uma sessão é processado no mesmo shard,
            // então o estado da sessão nunca é compartilhado entre threads.
            return {Verdict::Forward, ctx.sessionId % shardCount_};

        case FrameType::Bye:
            return {Verdict::Close};

        default:
            return {Verdict::Reject};
        }
    }

private:
    uint32_t shardCount_;
};


#endif // CONNECTION_LAYER_H
