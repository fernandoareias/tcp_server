#include "net/connection_listener.h"

#include "core/shard_mesh.h"

#include <csignal>
#include <cstdio>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <pthread.h>
#include <sched.h>

namespace {

std::vector<int> allowedCpus() {
    cpu_set_t set;
    CPU_ZERO(&set);
    std::vector<int> cpus;
    if (::sched_getaffinity(0, sizeof(set), &set) == 0) {
        for (int i = 0; i < CPU_SETSIZE; ++i)
            if (CPU_ISSET(i, &set)) cpus.push_back(i);
    }
    if (cpus.empty()) {
        unsigned n = std::thread::hardware_concurrency();
        for (unsigned i = 0; i < (n ? n : 1); ++i) cpus.push_back(int(i));
    }
    return cpus;
}

void pinCurrentThread(int cpu, uint32_t shard) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (int err = ::pthread_setaffinity_np(::pthread_self(), sizeof(set), &set))
        std::fprintf(stderr, "[shard %u] não consegui fixar no core %d (erro %d)\n", shard, cpu, err);

    std::string name = "shard-" + std::to_string(shard);
    ::pthread_setname_np(::pthread_self(), name.c_str());
}

}  // namespace

ConnectionListener::ConnectionListener(uint16_t port, Reactor::PayloadHandler handler)
    : port_(port), handler_(std::move(handler)) {}

int ConnectionListener::Run() {

    sigset_t sigs;
    sigemptyset(&sigs);
    sigaddset(&sigs, SIGINT);
    sigaddset(&sigs, SIGTERM);
    ::pthread_sigmask(SIG_BLOCK, &sigs, nullptr);
    std::signal(SIGPIPE, SIG_IGN);

    const std::vector<int> cpus = allowedCpus();
    const auto             n    = uint32_t(cpus.size());


    EnvelopePool                          envelopePool(size_t(n) * 8192);
    ShardMesh                             mesh(n, envelopePool);
    std::vector<std::unique_ptr<Reactor>> reactors;
    try {
        for (uint32_t i = 0; i < n; ++i) {
            reactors.push_back(std::make_unique<Reactor>(i, mesh, envelopePool, port_, handler_));
            mesh.setWakeFd(i, reactors.back()->wakeFd());
        }
    } catch (const std::system_error& e) {
        std::fprintf(stderr, "falha ao iniciar: %s\n", e.what());
        return 1;
    }

    std::vector<std::thread> threads;
    threads.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        threads.emplace_back([&r = *reactors[i], cpu = cpus[i]] {
            pinCurrentThread(cpu, r.id());
            r.run();
        });
    }

    std::printf("%u shards (1 por core) escutando na porta %u\n", n, unsigned(port_));
    std::fflush(stdout);

    int sig = 0;
    ::sigwait(&sigs, &sig);
    std::printf("\nsinal %d recebido, encerrando...\n", sig);

    for (auto& r : reactors) r->stop();
    for (auto& t : threads) t.join();

    for (auto& r : reactors) {
        const auto& s = r->stats();
        std::printf("shard %u: conexões=%lu frames=%lu payloads locais=%lu "
                    "payloads de outros shards=%lu rejeitados=%lu\n",
                    r->id(), (unsigned long)s.accepted, (unsigned long)s.framesIn,
                    (unsigned long)s.payloadsLocal, (unsigned long)s.payloadsRemote,
                    (unsigned long)s.rejected);
    }
    return 0;
}
