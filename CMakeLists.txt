cmake_minimum_required(VERSION 3.10)
project(PhotonDB)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra")

include_directories(${CMAKE_SOURCE_DIR}/src)

add_executable(server
    src/server.cpp
    src/hashtable.cpp
    src/zset.cpp
    src/avl.cpp
    src/heap.cpp
    src/thread_pool.cpp
    src/commands/commands.cpp
)

add_executable(photon-cli
    src/photon-cli.cpp
)