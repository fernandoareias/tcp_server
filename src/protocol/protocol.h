#ifndef PROTOCOL_H
#define PROTOCOL_H


#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <span>
#include <vector>

namespace proto {

//
//   +------------+----------------------------+-----------+
//   | LENGTH u32 | HEADER (kHeaderSize bytes) | PAYLOAD   |
//   +------------+----------------------------+-----------+
//
// LENGTH = tamanho de HEADER + PAYLOAD (não conta os 4 bytes do próprio LENGTH).
//
// HEADER (12 bytes):
//   version u8 | type u8 | flags u16 | sessionId u32 | streamId u32
//

constexpr size_t   kLengthSize   = 4;
constexpr size_t   kHeaderSize   = 12;
constexpr uint32_t kMaxFrameSize = 16u * 1024 * 1024;   // limite contra LENGTH malicioso
constexpr uint8_t  kVersion      = 1;

enum class FrameType : uint8_t {
    Hello = 1,
    Data  = 2,
    Bye   = 3,
    Reply = 0x82,
};

struct FrameHeader {
    uint8_t  version   = 0;
    uint8_t  type      = 0;
    uint16_t flags     = 0;
    uint32_t sessionId = 0;
    uint32_t streamId  = 0;
};


struct FrameView {
    FrameHeader              header;
    std::span<const uint8_t> payload;
};

inline uint16_t load16(const uint8_t* p) { return uint16_t(p[0] << 8 | p[1]); }
inline uint32_t load32(const uint8_t* p) {
    return uint32_t(p[0]) << 24 | uint32_t(p[1]) << 16 | uint32_t(p[2]) << 8 | uint32_t(p[3]);
}
inline void store16(uint8_t* p, uint16_t v) { p[0] = uint8_t(v >> 8); p[1] = uint8_t(v); }
inline void store32(uint8_t* p, uint32_t v) {
    p[0] = uint8_t(v >> 24); p[1] = uint8_t(v >> 16); p[2] = uint8_t(v >> 8); p[3] = uint8_t(v);
}

inline FrameHeader decodeHeader(const uint8_t* p) {
    FrameHeader h;
    h.version   = p[0];
    h.type      = p[1];
    h.flags     = load16(p + 2);
    h.sessionId = load32(p + 4);
    h.streamId  = load32(p + 8);
    return h;
}

inline void encodeFrame(std::vector<uint8_t>& out, const FrameHeader& h,
                        std::span<const uint8_t> payload) {
    const size_t start = out.size();
    out.resize(start + kLengthSize + kHeaderSize + payload.size());
    uint8_t* p = out.data() + start;
    store32(p, uint32_t(kHeaderSize + payload.size()));
    p += kLengthSize;
    p[0] = h.version;
    p[1] = h.type;
    store16(p + 2, h.flags);
    store32(p + 4, h.sessionId);
    store32(p + 8, h.streamId);
    if (!payload.empty()) std::memcpy(p + kHeaderSize, payload.data(), payload.size());
}


class FrameParser {
public:
    enum class Status { NeedMore, Ready, Error };

    std::span<uint8_t> prepare() {
        reserve();
        return {buf_.data() + wr_, buf_.size() - wr_};
    }
    void commit(size_t n) { wr_ += n; }

    Status next(FrameView& out) {
        const size_t avail = wr_ - rd_;
        if (avail < kLengthSize) { needed_ = kLengthSize; return Status::NeedMore; }

        const uint8_t* p   = buf_.data() + rd_;
        const uint32_t len = load32(p);
        if (len < kHeaderSize || len > kMaxFrameSize) return Status::Error;

        const size_t total = kLengthSize + len;
        if (avail < total) { needed_ = total; return Status::NeedMore; }

        out.header  = decodeHeader(p + kLengthSize);
        out.payload = {p + kLengthSize + kHeaderSize, len - kHeaderSize};
        rd_ += total;
        needed_ = 0;
        if (rd_ == wr_) rd_ = wr_ = 0;
        return Status::Ready;
    }

private:
    static constexpr size_t kRecvChunk   = 16 * 1024;
    static constexpr size_t kShrinkAbove = 1024 * 1024;

    void reserve() {
        const size_t buffered = wr_ - rd_;
        size_t want = kRecvChunk;
        if (needed_ > buffered) want = std::max(want, needed_ - buffered);
        if (buf_.size() - wr_ >= want) return;

        if (buffered == 0 && buf_.size() > kShrinkAbove) {
            buf_ = std::vector<uint8_t>();
            rd_ = wr_ = 0;
        }
        if (rd_ > 0) {
            std::memmove(buf_.data(), buf_.data() + rd_, buffered);
            rd_ = 0;
            wr_ = buffered;
        }
        if (buf_.size() - wr_ < want) buf_.resize(wr_ + want);
    }

    std::vector<uint8_t> buf_;
    size_t rd_ = 0, wr_ = 0, needed_ = 0;
};

}  // namespace proto


#endif // PROTOCOL_H
