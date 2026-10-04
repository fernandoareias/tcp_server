#ifndef SPSC_QUEUE_H
#define SPSC_QUEUE_H


#include <atomic>
#include <cstddef>
#include <type_traits>

// 64 bytes cobre a linha de cache de praticamente todo x86_64/ARM64 atualmente (2026).
inline constexpr size_t kCacheLineSize = 64;

// Fila SPSC (single-producer/single-consumer) lock-free, sem dependências
// externas. Pensada pra ser o caminho mais rápido possível entre dois shards
// fixados em cores diferentes:
//
//  - Capacidade potência de 2: indexação por AND em vez de módulo/divisão.
//  - writeIndex_ e readIndex_ (e os caches de cada lado) em cache lines
//    separadas: produtor e consumidor nunca disputam a mesma linha.
//  - Cada lado guarda uma cópia LOCAL, não atômica, do cursor do outro lado
//    (readIndexCached_ / writeIndexCached_) e só faz um load() de verdade
//    quando essa cópia indica fila cheia/vazia. No caminho comum (fila com
//    espaço/itens), push/pop não fazem nenhum load cross-core, só o próprio
//    store no final.
//  - memory_order mínimo necessário: relaxed pro próprio cursor (só esta
//    thread escreve nele), acquire/release só na borda entre as threads.
//
// T precisa ser trivialmente copiável: a fila move bytes por atribuição, não
// há construção/destruição por elemento além disso.
template <typename T, size_t Capacity>
class SpscQueue {
    static_assert(Capacity >= 2 && (Capacity & (Capacity - 1)) == 0,
                  "Capacity precisa ser potência de 2");
    static_assert(std::is_trivially_copyable_v<T>, "T precisa ser trivialmente copiável");

    static constexpr size_t kMask = Capacity - 1;

public:
    SpscQueue() = default;
    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

    // Só o produtor chama.
    bool push(const T& v) noexcept {
        const size_t w    = writeIndex_.load(std::memory_order_relaxed);
        const size_t next = (w + 1) & kMask;
        if (next == readIndexCached_) {
            readIndexCached_ = readIndex_.load(std::memory_order_acquire);
            if (next == readIndexCached_) return false;   // fila realmente cheia
        }
        buf_[w] = v;
        writeIndex_.store(next, std::memory_order_release);
        return true;
    }

    // Só o consumidor chama.
    bool pop(T& out) noexcept {
        const size_t r = readIndex_.load(std::memory_order_relaxed);
        if (r == writeIndexCached_) {
            writeIndexCached_ = writeIndex_.load(std::memory_order_acquire);
            if (r == writeIndexCached_) return false;   // fila realmente vazia
        }
        out = buf_[r];
        readIndex_.store((r + 1) & kMask, std::memory_order_release);
        return true;
    }

    template <typename F>
    size_t consume_all(F&& f) {
        size_t       n = 0;
        size_t       r = readIndex_.load(std::memory_order_relaxed);
        for (;;) {
            if (r == writeIndexCached_) {
                writeIndexCached_ = writeIndex_.load(std::memory_order_acquire);
                if (r == writeIndexCached_) break;
            }
            f(buf_[r]);
            r = (r + 1) & kMask;
            ++n;
        }
        if (n != 0) readIndex_.store(r, std::memory_order_release);
        return n;
    }

    bool empty() const noexcept {
        return readIndex_.load(std::memory_order_acquire) ==
               writeIndex_.load(std::memory_order_acquire);
    }

private:
    alignas(kCacheLineSize) std::atomic<size_t> writeIndex_{0};
    alignas(kCacheLineSize) size_t               readIndexCached_ = 0;    // só o produtor toca

    alignas(kCacheLineSize) std::atomic<size_t> readIndex_{0};
    alignas(kCacheLineSize) size_t               writeIndexCached_ = 0;   // só o consumidor toca

    alignas(kCacheLineSize) T buf_[Capacity];
};


#endif // SPSC_QUEUE_H
