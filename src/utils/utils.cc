#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <vector>

#include "utils.h"

// Function to convert std::string to modifiable char*
char* stringToCharArray(const std::string& str) {
    // Allocate memory for the char array (+1 for the null terminator)
    char* charArray = new char[str.length() + 1];

    std::cout << str << std::endl;    
    // Copy the contents of the string into the char array
    std::strcpy(charArray, str.c_str());
    std::cout << charArray << std::endl;    
    
    // Return the modifiable char array
    return charArray;
}

// Function to convert a modifiable char array to std::string
std::string charArrayToString(char* charArray) {
    // Create a std::string from the char array
    return std::string(charArray);
}

std::vector<PartitionConfig> parseConfigFile(const std::string& config_file) {
    std::vector<PartitionConfig> partitions;
    std::ifstream file(config_file);
    int num_partitions;
    
    if (!file.is_open()) {
        std::cerr << "Error: Could not open file " << config_file << std::endl;
        return partitions;
    }

    std::string line;
    if (std::getline(file, line)) {
        num_partitions = std::stoi(line);
    }

    for (int i=0; i<num_partitions; i++) {
        partitions.push_back(PartitionConfig(i));
    }

    while (std::getline(file, line)) {
        std::stringstream ss(line);
        std::string partition_id_str, server_id, port;
        int partition_id;
        
        // Read the partition_id, server_id, and port from the CSV
        std::getline(ss, partition_id_str, ',');
        std::getline(ss, server_id, ',');
        std::getline(ss, port, ',');

        partition_id = std::stoi(partition_id_str);

        partitions[partition_id].addServer(port);
    }

    file.close();

    for (auto partition: partitions) {
        partition.init();
    }
    
    return partitions;
}
