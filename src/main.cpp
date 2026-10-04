#include "net/connection_listener.h"

#include <cstdlib>

int main(int argc, char** argv)
{
    const uint16_t port = argc > 1 ? uint16_t(std::atoi(argv[1])) : 8080;

    ConnectionListener listener(port, [](Reactor& reactor, EnvelopePtr env) {
        reactor.reply(std::move(env));
    });
    return listener.Run();

}
