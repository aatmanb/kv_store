#pragma once

#include <mutex>

namespace key_value_store {
    template<typename T>
    class Singleton {
    public:
        Singleton(const Singleton& other) = delete;

        Singleton& operator = (const Singleton& other) = delete;

        Singleton& operator = (Singleton&& other) = delete;

        static inline T* get_instance() {
            if (!instance) {
                std::lock_guard<std::mutex> lock {mtx};
                if (!instance) {
                    instance = new T();
                }
                return instance;
            }
            return instance;
        }

        ~Singleton() {
            delete instance;
        }
    
    protected:
        Singleton() {}

    private:
        static inline std::mutex mtx;

        static inline T* instance = nullptr; 
    };
}