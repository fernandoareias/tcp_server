#ifndef THREAD_POOL_DISPATCHER_H
#define THREAD_POOL_DISPATCHER_H

#include <thread>
#include <algorithm>

class ThreadPoolDispatcher
{
public:
    ThreadPoolDispatcher();
private:
    static unsigned detectCores() {
        return std::max(1u, std::thread::hardware_concurrency());
    }
    static inline const unsigned numberCores_m = detectCores();
};

#endif
