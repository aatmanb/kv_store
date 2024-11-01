#pragma once

#include <queue>
#include <atomic>
#include <memory>
#include <thread>
#include <functional>
#include <condition_variable>

namespace key_value_store
{
    class Worker {
    private:
        std::atomic<bool> should_terminate;
        
        std::queue<std::function<void()>> task_queue;

        std::thread executor;

        std::mutex queue_mutex;                  // Prevents data races to the job queue
        
        std::condition_variable mutex_condition; // Allows threads to wait on new jobs or termination

        void run() {
            while (true) {
                std::function<void()> job;
                {
                    std::unique_lock<std::mutex> lock(queue_mutex);
                    mutex_condition.wait(lock, [this] {
                        return !task_queue.empty() || should_terminate.load();
                    });
                    if (should_terminate.load()) {
                        return;
                    }
                    job = task_queue.front();
                    task_queue.pop();
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
                task_queue.push(func);
            }
            mutex_condition.notify_one();
        }
    };
} // namespace key_value_store
