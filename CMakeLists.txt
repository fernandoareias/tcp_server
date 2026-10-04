cmake_minimum_required(VERSION 3.16)

project(tcp_server LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Threads REQUIRED)

add_executable(tcp_server
    src/main.cpp

    # core: reactor, shards e pools
    src/core/reactor.h        src/core/reactor.cpp
    src/core/shard_mesh.h
    src/core/envelope_pool.h
    src/core/task.h

    # net: conexões e listener
    src/net/connection.h
    src/net/connection_layer.h
    src/net/connection_listener.h src/net/connection_listener.cpp

    # protocol: framing

    # concurrency: filas e thread pool
    src/concurrency/spsc_queue.h        src/concurrency/spsc_queue.cpp
    src/concurrency/thread_pool_dispatcher.h src/concurrency/thread_pool_dispatcher.cpp
)

# Includes são relativos a src/, ex.: #include "core/reactor.h"
target_include_directories(tcp_server PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(tcp_server PRIVATE Threads::Threads)

include(GNUInstallDirs)
install(TARGETS tcp_server
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
