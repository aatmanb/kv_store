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

        std::mutex queue_mutex;                  // Prevents data races to the job queue
        
        std::condition_variable mutex_condition; // Allows threads to wait on new jobs or termination

        void run() {
            while (true) {
                std::function<void()> job;
                {
                    std::unique_lock<std::mutex> lock(queue_mutex);
                    mutex_condition.wait(lock, [this] {
                        return !task_queue.isEmpty() || should_terminate.load();
                    });
                    if (should_terminate.load()) {
                        return;
                    }
                    job = task_queue.dequeue();
                }
                job();
            }
        }
    public:
        Worker() {
            should_terminate.store(true);
        }

        void pause() {
            {
                std::unique_lock<std::mutex> lock(queue_mutex);
                should_terminate.store(true);
            }
            mutex_condition.notify_one();
            if (executor.joinable()) {
                executor.join();
            }
        }

        void start() {
            if (!should_terminate.load()) {
                return;
            }
            should_terminate.store(false);
            executor = std::thread(&Worker::run, this);
        }

        bool is_running() {
            return !should_terminate.load();
        }

        void post(const std::function<void()> &func) {
            {
                std::unique_lock<std::mutex> lock(queue_mutex);
                task_queue.enqueue(func);
            }
            mutex_condition.notify_one();
        }
    };
} // namespace key_value_store
