#include "concurrency/spsc_queue.h"

#include "core/shard_mesh.h"

// Instanciação explícita: SpscQueue<ShardMessage, 4096>
template class SpscQueue<ShardMessage, kShardQueueCapacity>;
