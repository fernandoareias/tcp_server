#include "core/reactor.h"

#include <cerrno>
#include <cstdio>
#include <system_error>
#include <utility>

#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

constexpr uint64_t kListenerTag    = ~uint64_t(0);
constexpr uint64_t kWakeTag        = ~uint64_t(0) - 1;
constexpr int      kMaxEvents      = 256;
constexpr size_t   kFramesPerSlice = 64;                // depois disso a corrotina cede a vez
constexpr size_t   kOutHighWater   = 4 * 1024 * 1024;   // backpressure de escrita

[[noreturn]] void fail(const char* what) {
    throw std::system_error(errno, std::generic_category(), what);
}

// Cada shard tem o próprio socket de escuta na mesma porta. Com SO_REUSEPORT
// o kernel distribui as conexões novas entre eles: não existe shard acceptor
// nem repasse de fd entre threads.
int createListenSocket(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) fail("socket");

    int yes = 1;
    if (::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0 ||
        ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &yes, sizeof(yes)) < 0) {
        ::close(fd);
        fail("setsockopt");
    }

    sockaddr_in addr{};
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) { ::close(fd); fail("bind"); }
    if (::listen(fd, 1024) < 0) { ::close(fd); fail("listen"); }
    return fd;
}

}  // namespace

Reactor::Reactor(uint32_t id, ShardMesh& mesh, EnvelopePool& pool, uint16_t port, PayloadHandler handler)
    : id_(id),
    mesh_(mesh),
    envelopePool_(pool),
    layer_(mesh.size()),
    handler_(std::move(handler)),
    overflow_(mesh.size()),
    dirty_(mesh.size(), false) {
    epfd_ = ::epoll_create1(EPOLL_CLOEXEC);
    if (epfd_ < 0) fail("epoll_create1");

    wakeFd_ = ::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeFd_ < 0) fail("eventfd");

    listenFd_  = createListenSocket(port);
    reserveFd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);

    epoll_event ev{};
    ev.events   = EPOLLIN;   // level-triggered; drenamos com accept até EAGAIN
    ev.data.u64 = kListenerTag;
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, listenFd_, &ev) < 0) fail("epoll_ctl(listen)");

    ev.events   = EPOLLIN;
    ev.data.u64 = kWakeTag;
    if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, wakeFd_, &ev) < 0) fail("epoll_ctl(eventfd)");
}

Reactor::~Reactor() {
    destroyAllConnections();
    for (void* m : slotMem_) ::operator delete(m);
    for (int fd : {listenFd_, wakeFd_, reserveFd_, epfd_})
        if (fd >= 0) ::close(fd);
}

void Reactor::stop() {
    stopping_.store(true, std::memory_order_relaxed);
    uint64_t one = 1;
    ssize_t r = ::write(wakeFd_, &one, sizeof(one));
    (void)r;
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

void Reactor::run() {
    epoll_event events[kMaxEvents];

    while (!stopping_.load(std::memory_order_relaxed)) {
        notifyPeers();

        bool hasOverflow = false;
        for (auto& q : overflow_) hasOverflow |= !q.empty();

        // Trabalho local pendente: não dorme. Fila remota cheia: tenta de novo em 1 ms.
        int timeout = (!ready_.empty() || !localInbox_.empty()) ? 0 : hasOverflow ? 1 : -1;

        int n = ::epoll_wait(epfd_, events, kMaxEvents, timeout);
        if (n < 0) {
            if (errno == EINTR) continue;
            std::perror("epoll_wait");
            break;
        }

        for (int i = 0; i < n; ++i) {
            const uint64_t tag = events[i].data.u64;
            if (tag == kListenerTag) {
                acceptAll();
            } else if (tag == kWakeTag) {
                uint64_t v;
                ssize_t r = ::read(wakeFd_, &v, sizeof(v));   // zera o contador
                (void)r;
            } else {
                onConnEvent(tag, events[i].events);
            }
        }

        drainInbox();   // payloads/respostas vindos de outros shards
        while (!localInbox_.empty()) {
            ShardMessage m = localInbox_.front();
            localInbox_.pop_front();
            handleMessage(m);
        }
        runReady();      // retoma as threads virtuais acordadas
        flushOverflow();
    }

    // Shutdown: destrói as corrotinas suspensas e fecha os sockets deste shard.
    ready_.clear();
    for (auto& m : localInbox_) envelopePool_.release(m.env);
    localInbox_.clear();
    for (auto& q : overflow_) {
        for (auto& m : q) envelopePool_.release(m.env);
        q.clear();
    }
    destroyAllConnections();
}

void Reactor::runReady() {
    std::vector<uint64_t> batch;
    batch.swap(ready_);   // quem for agendado agora roda na próxima volta
    for (uint64_t cid : batch) {
        Connection* c = findConnection(cid);
        if (!c || !c->waiter) continue;   // conexão já encerrada, ou agendamento duplicado
        std::exchange(c->waiter, {}).resume();
    }
}

// ---------------------------------------------------------------------------
// Conexões
// ---------------------------------------------------------------------------

void Reactor::acceptAll() {
    for (;;) {
        int fd = ::accept4(listenFd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR || errno == ECONNABORTED) continue;
            if ((errno == EMFILE || errno == ENFILE) && reserveFd_ >= 0) {
                // Sem descritores: usa o reserva para aceitar e recusar na hora,
                // senão a conexão fica na fila e o epoll dispara sem parar.
                ::close(reserveFd_);
                int t = ::accept(listenFd_, nullptr, nullptr);
                if (t >= 0) ::close(t);
                reserveFd_ = ::open("/dev/null", O_RDONLY | O_CLOEXEC);
                std::fprintf(stderr, "[shard %u] sem file descriptors, conexão recusada\n", id_);
                continue;
            }
            std::perror("accept4");
            return;
        }

        int one = 1;
        ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        Connection&    ref = createConnection(fd);
        const uint64_t cid = ref.id;

        epoll_event ev{};
        ev.events   = EPOLLIN | EPOLLOUT | EPOLLRDHUP | EPOLLET;   // edge-triggered
        ev.data.u64 = cid;
        if (::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            std::perror("epoll_ctl(conn)");
            destroyConnection(cid);   // fecha o fd e recicla o slot
            continue;
        }

        ++stats_.accepted;
        serve(ref);   // dispara a thread virtual; roda até o primeiro co_await
    }
}

void Reactor::onConnEvent(uint64_t connId, uint32_t events) {
    Connection* cp = findConnection(connId);
    if (!cp) return;   // conexão já encerrada nesta mesma volta
    Connection& c = *cp;

    if (events & (EPOLLIN | EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
        c.readReady = true;   // o recv vai revelar dados, EOF ou erro
        wakeConn(c);
    }
    if ((events & (EPOLLOUT | EPOLLHUP | EPOLLERR)) && c.pendingOut() > 0) flush(c);
}

void Reactor::wakeConn(Connection& c) {
    if (c.waiter) ready_.push_back(c.id);
}

void Reactor::markClosed(Connection& c) {
    c.closed = true;
    wakeConn(c);   // a corrotina percebe e encerra a conexão ela mesma
}

// A corrotina é a única dona do ciclo de vida da conexão: só ela chama
// destroyConnection, e só no fim. Os outros caminhos apenas marcam `closed`
// e a acordam. Assim nunca destruímos uma conexão no meio do uso.
Task Reactor::serve(Connection& c) {
    const uint64_t cid   = c.id;
    size_t         slice = 0;

    while (!c.closed) {
        if (c.pendingOut() > kOutHighWater) {   // cliente lento: para de ler
            co_await Park{*this, c, false};
            continue;
        }
        if (!c.readReady) {
            co_await Park{*this, c, false};
            continue;
        }

        std::span<uint8_t> space = c.parser.prepare();
        ssize_t n = ::recv(c.fd, space.data(), space.size(), 0);
        if (n == 0) break;   // EOF
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) { c.readReady = false; continue; }
            if (errno == EINTR) continue;
            if (errno != ECONNRESET) std::perror("recv");
            break;
        }
        c.parser.commit(size_t(n));

        proto::FrameView frame;
        for (;;) {
            auto st = c.parser.next(frame);
            if (st == proto::FrameParser::Status::NeedMore) break;
            if (st == proto::FrameParser::Status::Error) {
                std::fprintf(stderr, "[shard %u] conn %lu: LENGTH inválido, encerrando\n",
                             id_, (unsigned long)cid);
                ++stats_.rejected;
                c.closed = true;
                break;
            }
            onFrame(c, frame);
            if (c.closed) break;

            if (++slice >= kFramesPerSlice) {   // justiça entre as threads virtuais
                slice = 0;
                co_await Park{*this, c, true};
                if (c.closed) break;
            }
        }
    }

    if (c.pendingOut() > 0) flush(c);   // melhor esforço antes de fechar
    destroyConnection(cid);
    // A partir daqui `c` não existe mais.
}

void Reactor::onFrame(Connection& c, const proto::FrameView& f) {
    ++stats_.framesIn;

    // 1) HEADER -> camada inicial (estado da conexão)
    const auto d = layer_.onHeader(c.ctx, f.header);

    switch (d.verdict) {
    case ConnectionLayer::Verdict::Consumed:
        return;
    case ConnectionLayer::Verdict::Close:
        c.closed = true;
        return;
    case ConnectionLayer::Verdict::Reject:
        ++stats_.rejected;
        c.closed = true;
        return;
    case ConnectionLayer::Verdict::Forward:
        break;
    }

    // 2) PAYLOAD -> buffer do reactor escolhido (único ponto em que o payload é copiado)
    Envelope* raw = envelopePool_.acquire();   // reciclado do pool, sem malloc no caminho comum
    raw->connId      = c.id;
    raw->originShard = id_;
    raw->sessionId   = f.header.sessionId;
    raw->streamId    = f.header.streamId;
    raw->setPayload(f.payload);
    EnvelopePtr env(raw, EnvelopeDeleter{&envelopePool_});

    if (d.targetShard == id_) {
        ++stats_.payloadsLocal;
        handler_(*this, std::move(env));   // mesmo shard: chama direto, sem passar pela fila
        return;
    }
    send(d.targetShard, {ShardMessage::Kind::Payload, env.release()});
}

void Reactor::flush(Connection& c) {
    while (c.outOff < c.out.size()) {
        ssize_t n = ::send(c.fd, c.out.data() + c.outOff, c.out.size() - c.outOff, MSG_NOSIGNAL);
        if (n > 0) { c.outOff += size_t(n); continue; }
        if (n < 0 && errno == EINTR) continue;
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) break;   // espera EPOLLOUT
        markClosed(c);
        return;
    }
    if (c.outOff == c.out.size()) {
        c.out.clear();
        c.outOff = 0;
    } else if (c.outOff > 64 * 1024) {
        c.out.erase(c.out.begin(), c.out.begin() + std::ptrdiff_t(c.outOff));
        c.outOff = 0;
    }
    if (c.pendingOut() <= kOutHighWater) wakeConn(c);   // libera leitura pausada
}

Connection& Reactor::createConnection(int fd) {
    uint64_t cid;
    if (!freeIds_.empty()) {
        cid = freeIds_.back();
        freeIds_.pop_back();
    } else {
        cid = slots_.size();
        slots_.push_back(nullptr);
        slotMem_.push_back(::operator new(sizeof(Connection)));
    }
    Connection* c = new (slotMem_[cid]) Connection(cid, fd);   // reaproveita o bloco do slot
    slots_[cid] = c;
    return *c;
}

Connection* Reactor::findConnection(uint64_t connId) {
    if (connId >= slots_.size()) return nullptr;
    return slots_[connId];
}

void Reactor::destroyConnection(uint64_t connId) {
    Connection* c = slots_[connId];
    slots_[connId] = nullptr;
    c->~Connection();        // fecha o fd; respostas que chegarem depois são descartadas
    freeIds_.push_back(connId);
}

void Reactor::destroyAllConnections() {
    for (size_t i = 0; i < slots_.size(); ++i) {
        if (slots_[i]) {
            slots_[i]->~Connection();
            slots_[i] = nullptr;
        }
    }
    freeIds_.clear();
}

// ---------------------------------------------------------------------------
// Comunicação entre shards
// ---------------------------------------------------------------------------

void Reactor::send(uint32_t dst, ShardMessage msg) {
    if (dst == id_) {
        localInbox_.push_back(msg);
        return;
    }
    auto& ov = overflow_[dst];
    // Se já há overflow, entra atrás dele para não inverter a ordem.
    if (ov.empty() && mesh_.queue(id_, dst).push(msg)) {
        dirty_[dst] = true;
        return;
    }
    ov.push_back(msg);   // fila cheia: nunca bloqueia o event loop
}

void Reactor::flushOverflow() {
    for (uint32_t dst = 0; dst < overflow_.size(); ++dst) {
        auto& ov = overflow_[dst];
        while (!ov.empty() && mesh_.queue(id_, dst).push(ov.front())) {
            ov.pop_front();
            dirty_[dst] = true;
        }
    }
}

// Um único write no eventfd por destino a cada volta do loop, não um por mensagem.
void Reactor::notifyPeers() {
    for (uint32_t dst = 0; dst < dirty_.size(); ++dst) {
        if (dirty_[dst]) {
            dirty_[dst] = false;
            mesh_.wake(dst);
        }
    }
}

void Reactor::drainInbox() {
    for (uint32_t src = 0; src < mesh_.size(); ++src) {
        if (src == id_) continue;
        mesh_.queue(src, id_).consume_all([this](const ShardMessage& m) { handleMessage(m); });
    }
}

void Reactor::handleMessage(const ShardMessage& msg) {
    EnvelopePtr env(msg.env, EnvelopeDeleter{&envelopePool_});   // assume a posse
    if (msg.kind == ShardMessage::Kind::Payload) {
        // Só chega aqui o que veio de outro shard: o caminho local é tratado
        // direto em onFrame, sem passar pela fila.
        ++stats_.payloadsRemote;
        handler_(*this, std::move(env));
    } else {
        deliverReply(std::move(env));
    }
}

void Reactor::reply(EnvelopePtr env) {
    const uint32_t origin = env->originShard;
    send(origin, {ShardMessage::Kind::Reply, env.release()});
}

void Reactor::deliverReply(EnvelopePtr env) {
    Connection* cp = findConnection(env->connId);
    if (!cp || cp->closed) return;   // conexão já fechou
    Connection& c = *cp;

    proto::FrameHeader h;
    h.version   = proto::kVersion;
    h.type      = uint8_t(proto::FrameType::Reply);
    h.sessionId = env->sessionId;
    h.streamId  = env->streamId;
    proto::encodeFrame(c.out, h, env->payload());
    flush(c);
}
