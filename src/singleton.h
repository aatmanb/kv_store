#pragma once

#include <boost/thread.hpp>

namespace key_value_store {
    template<typename T>
    class Singleton {
    public:
        Singleton(const Singleton& other) = delete;

        Singleton& operator = (const Singleton& other) = delete;

        Singleton& operator = (Singleton&& other) = delete;

        static inline T* get_instance() {
            if (!instance) {
                boost::lock_guard<boost::mutex> lock;
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
        static inline boost::mutex mtx;

        static inline T* instance = nullptr; 
    };
}