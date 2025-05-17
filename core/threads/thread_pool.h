#pragma once

#include <stddef.h>
#include <deque>
#include <pthread.h>
#include <vector>

struct Work
{
    void (*f)(void *) = NULL;
    void *arg = NULL;
};

struct ThreadPool
{
    std::vector<pthread_t> threads;
    std::deque<Work> queue;
    pthread_mutex_t mu;
    pthread_cond_t not_empty;
};

void thread_pool_init(ThreadPool *tp, size_t num_threads);
void thread_pool_queue(ThreadPool *tp, void (*f)(void *), void *arg);