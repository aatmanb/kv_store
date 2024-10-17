#ifndef utils_h__
#define utils_h__

#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>

#include <chrono>
#include <iomanip>
#include <ctime>

#include "partition_utils.h"

// Define a macro to print the current timestamp
//std::cout << std::put_time(std::localtime(&now_c), "%Y-%m-%d %H:%M:%S") << std::endl;
#define PRINT_TIMESTAMP() { \
    auto now = std::chrono::system_clock::now(); \
    std::time_t now_c = std::chrono::system_clock::to_time_t(now); \
    std::cout << std::put_time(std::localtime(&now_c), "%H:%M:%S"); \
}

//#define COUT \
    std::cout << __FILE__ << ":" << __LINE__ << " " << "[" <<  << "]" << " "

#define COUT std::cout << __FILE__ << ":" << __LINE__ << " [" << \
    [] { \
        auto now = std::chrono::system_clock::now(); \
        auto now_c = std::chrono::system_clock::to_time_t(now); \
        auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000; \
        std::ostringstream oss; \
        oss << std::put_time(std::localtime(&now_c), "%H:%M:%S") << "." << std::setfill('0') << std::setw(3) << milliseconds.count(); \
        return oss.str(); \
    }() << "] "
    //[] { \
        auto now = std::chrono::system_clock::now(); \
        auto now_c = std::chrono::system_clock::to_time_t(now); \
        auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000; \
        return std::put_time(std::localtime(&now_c), "%H:%M:%S") + std::string(".") + std::to_string(milliseconds.count()); \
    }() << "] "

char* stringToCharArray(const std::string& str);

// Function to convert a modifiable char array to std::string
std::string charArrayToString(char* charArray);

std::vector<PartitionConfig> parseConfigFile(const std::string& config_file);

struct CustomHash {
    std::size_t operator()(const std::string& key) const {
        std::size_t hash = 0;
        for (char c : key) {
            hash = (hash << 5) ^ (hash >> 2) ^ c;
        }
        return hash;
    }
};


#endif // utils_h__
