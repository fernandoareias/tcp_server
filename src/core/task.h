#ifndef TASK_H
#define TASK_H


#include <coroutine>
#include <cstddef>
#include <exception>
#include <new>
#include <vector>

// "Thread virtual": corrotina C++20 fire-and-forget.
struct Task {
    struct promise_type {
        Task get_return_object() noexcept { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() noexcept {}
        // O corpo das corrotinas trata os próprios erros; exceção aqui é bug.
        void unhandled_exception() noexcept { std::terminate(); }

        static void* operator new(size_t sz) {
            auto& pool = framePool();
            if (!pool.blocks.empty() && pool.blocks.back().size == sz) {
                void* p = pool.blocks.back().ptr;
                pool.blocks.pop_back();
                return p;
            }
            return ::operator new(sz);
        }
        static void operator delete(void* p, size_t sz) noexcept {
            auto& pool = framePool();
            if (pool.blocks.size() < kMaxPooled) {
                pool.blocks.push_back({p, sz});
            } else {
                ::operator delete(p);
            }
        }

    private:
        struct Block { void* ptr; size_t size; };
        static constexpr size_t kMaxPooled = 4096;

        // RAII: libera os blocos retidos quando a thread do shard termina.
        struct FramePool {
            std::vector<Block> blocks;
            ~FramePool() { for (Block& b : blocks) ::operator delete(b.ptr); }
        };
        static FramePool& framePool() {
            thread_local FramePool pool;
            return pool;
        }
    };
};


#endif // TASK_H
