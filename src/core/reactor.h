#ifndef REACTOR_H
#define REACTOR_H

#include "net/connection.h"
#include "net/connection_layer.h"
#include "core/envelope_pool.h"
#include "core/shard_mesh.h"
#include "core/task.h"

#include <atomic>
#include <coroutine>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <vector>

class Reactor {
public:

    using PayloadHandler = std::function<void(Reactor&, EnvelopePtr)>;

    struct Stats {
        uint64_t accepted       = 0;
        uint64_t framesIn       = 0;
        uint64_t payloadsLocal  = 0;
        uint64_t payloadsRemote = 0;
        uint64_t rejected       = 0;
    };

    Reactor(uint32_t id, ShardMesh& mesh, EnvelopePool& pool, uint16_t port, PayloadHandler handler);
    ~Reactor();
    Reactor(const Reactor&) = delete;
    Reactor& operator=(const Reactor&) = delete;

    void run();
    void stop();

    void reply(EnvelopePtr env);

    uint32_t     id() const { return id_; }
    int          wakeFd() const { return wakeFd_; }
    const Stats& stats() const { return stats_; }

private:

    struct Park {
        Reactor&    r;
        Connection& c;
        bool        yield;
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) noexcept {
            c.waiter = h;
            if (yield) r.ready_.push_back(c.id);
        }
        void await_resume() const noexcept {}
    };

    Task serve(Connection& c);
    void onFrame(Connection& c, const proto::FrameView& f);

    void acceptAll();
    void onConnEvent(uint64_t connId, uint32_t events);
    void wakeConn(Connection& c);
    void markClosed(Connection& c);
    void flush(Connection& c);

    Connection& createConnection(int fd);
    Connection* findConnection(uint64_t connId);
    void        destroyConnection(uint64_t connId);
    void        destroyAllConnections();

    void send(uint32_t dst, ShardMessage msg);
    void handleMessage(const ShardMessage& msg);
    void deliverReply(EnvelopePtr env);
    void drainInbox();
    void flushOverflow();
    void notifyPeers();
    void runReady();

    const uint32_t id_;
    ShardMesh&     mesh_;
    EnvelopePool&  envelopePool_;
    ConnectionLayer layer_;
    PayloadHandler handler_;

    int epfd_      = -1;
    int listenFd_  = -1;
    int wakeFd_    = -1;
    int reserveFd_ = -1;

    std::atomic<bool> stopping_{false};

    std::vector<Connection*> slots_;
    std::vector<void*>       slotMem_;
    std::vector<uint64_t>    freeIds_;

    std::vector<uint64_t>                 ready_;
    std::deque<ShardMessage>              localInbox_;
    std::vector<std::deque<ShardMessage>> overflow_;
    std::vector<bool>                     dirty_;

    Stats stats_;
};

#endif // REACTOR_H
