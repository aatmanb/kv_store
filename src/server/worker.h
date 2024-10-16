#pragma once

#include "thread_safe_queue.h"
#include <atomic>
#include <memory>
#include <thread>
#include <functional>

/**
 * Queues required:
 * - pending_requests
 * - pending acks
 * - pending database commits
 * - pending forwards
 * 
 * fwdrpc() {
 *      receive update
 *      push update to commit_queue (commits to database)
 *      // async
 *      push update to forward_queue (forwards to next server)
 *      push update to sent queue
 *      
 * }
 * 
 */
namespace key_value_store
{
    class Worker {
    private:
        std::atomic<bool> should_terminate;
        
        ThreadSafeQueue<std::function<void()>> task_queue;

        std::thread executor;

    public:
        void pause() {
            should_terminate.store(true);
            if (executor.joinable()) {
                executor.join();
            }
        }

        void start() {
            should_terminate.store(false);
            executor = std::thread(&Worker::run, this);
        }

        void run() {
            while (!should_terminate.load()) {
                const auto& job = task_queue.dequeue();
                job();
            }
        }

        void post(const std::function<void()> &func) {
            task_queue.enqueue(func);
        }
    };
} // namespace key_value_store
