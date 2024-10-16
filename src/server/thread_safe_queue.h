#pragma once

#include <iostream>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <optional>

template<typename T>
class ThreadSafeQueue {
private:
    std::queue<T> queue;                     // The underlying queue
    std::mutex mtx;                          // Mutex to protect shared access
    std::condition_variable condVar;         // Condition variable for synchronization

public:
    ThreadSafeQueue() = default;

    ThreadSafeQueue(ThreadSafeQueue &other) = default;

    ThreadSafeQueue& operator= (const ThreadSafeQueue&& other) {
        this->queue = std::move(other.queue);
        return *this;
    }

    ThreadSafeQueue(ThreadSafeQueue&& other) {
        this->queue = std::move(other.queue);
    }

    // Add an element to the queue
    void enqueue(const T& item) {
        std::lock_guard<std::mutex> lock(mtx); // Lock the queue
        queue.push(item);                      // Push the new item
        condVar.notify_one();                  // Notify one waiting thread (if any)
    }

    // Remove an element from the queue (blocking if empty)
    T dequeue() {
        std::unique_lock<std::mutex> lock(mtx); // Lock the queue
        // Wait until the queue is not empty
        condVar.wait(lock, [this]() { return !queue.empty(); });
        T item = queue.front();                 // Get the front item
        queue.pop();                            // Remove it from the queue
        return item;
    }

    // Try to remove an element from the queue (non-blocking, returns std::optional)
    std::optional<T> tryDequeue() {
        std::lock_guard<std::mutex> lock(mtx);  // Lock the queue
        if (queue.empty()) {
            return std::nullopt;                // Return empty if queue is empty
        }
        T item = queue.front();                 // Get the front item
        queue.pop();                            // Remove it from the queue
        return item;
    }

    std::optional<T> front() {
        std::lock_guard<std::mutex> lock(mtx);  // Lock the queue
        if (queue.empty()) {
            return std::nullopt;                // Return empty if queue is empty
        }
        return queue.front();                 // Get the front item
    }

    std::optional<T> back() {
        std::lock_guard<std::mutex> lock(mtx);  // Lock the queue
        if (queue.empty()) {
            return std::nullopt;                // Return empty if queue is empty
        }
        return queue.back();                 // Get the front item
    }

    // Check if the queue is empty
    bool isEmpty() {
        std::lock_guard<std::mutex> lock(mtx);  // Lock the queue
        return queue.empty();                   // Check if the queue is empty
    }

    // Get the size of the queue
    size_t size() {
        std::lock_guard<std::mutex> lock(mtx);  // Lock the queue
        return queue.size();                    // Return the size of the queue
    }
};
