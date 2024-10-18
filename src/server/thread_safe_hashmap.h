#include "tsl/robin_map.h"
#include <mutex>
#include <condition_variable>
#include <optional>

template <typename Key, typename T>
class ThreadSafeHashMap {
private:
    tsl::robin_map<Key, T> map;     // Underlying Map
    std::mutex map_mutex;           // Mutex to synchronize access
    std::condition_variable map_cv; // Condition variable for notification

public:
    // Insert or update a key-value pair and notify waiting threads
    void insert(const Key& key, const T& value) {
        std::lock_guard<std::mutex> lock(map_mutex); // Lock the map
        map[key] = value;                            // Insert the value
        map_cv.notify_one();                         // Notify a waiting thread (if any)
    }

    T operator [](const Key& key) {
        return get(key);
    }

    // Get value associated with a key
    // Returns a std::optional to indicate success or failure
    T get(const Key& key) {
        std::lock_guard<std::mutex> lock(map_mutex);
        return map[key];
    }

    // Erase a key from the map
    bool erase(const Key& key) {
        std::lock_guard<std::mutex> lock(map_mutex);
        return map.erase(key) > 0;
    }

    // Check if the map contains a key
    bool contains(const Key& key) const {
        std::lock_guard<std::mutex> lock(map_mutex);
        return map.find(key) != map.end();
    }

    // Get the size of the map
    size_t size() const {
        std::lock_guard<std::mutex> lock(map_mutex);
        return map.size();
    }

    // Wait for a key to be available in the map
    T wait_for_key(const Key& key) {
        std::unique_lock<std::mutex> lock(map_mutex);
        map_cv.wait(lock, [&] { return map.find(key) != map.end(); }); // Wait until the key is available
        return map[key];  // Return the value after waking up
    }
};

