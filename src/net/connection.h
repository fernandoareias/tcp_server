#ifndef CONNECTION_H
#define CONNECTION_H


#include "net/connection_layer.h"
#include "protocol/protocol.h"

#include <coroutine>
#include <cstdint>
#include <vector>

#include <unistd.h>

struct Connection {
    Connection(uint64_t id_, int fd_) : id(id_), fd(fd_) {}
    ~Connection() {
        if (waiter) waiter.destroy();
        if (fd >= 0) ::close(fd);
    }
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    size_t pendingOut() const { return out.size() - outOff; }

    const uint64_t id;
    const int      fd;

    ConnectionContext  ctx;
    proto::FrameParser parser;

    std::vector<uint8_t> out;
    size_t               outOff = 0;

    bool readReady = true;
    bool closed    = false;

    std::coroutine_handle<> waiter;
};


#endif // CONNECTION_H
