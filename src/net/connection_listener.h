#ifndef CONNECTION_LISTENER_H
#define CONNECTION_LISTENER_H

#include "core/reactor.h"

#include <cstdint>

class ConnectionListener {
public:
    ConnectionListener(uint16_t port, Reactor::PayloadHandler handler);

    int Run();

private:
    uint16_t                port_;
    Reactor::PayloadHandler handler_;
};


#endif // CONNECTION_LISTENER_H
