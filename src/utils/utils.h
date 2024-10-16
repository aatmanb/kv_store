#ifndef utils_h__
#define utils_h__

#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>

#include "partition_utils.h"

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
