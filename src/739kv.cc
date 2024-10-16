#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <grpcpp/grpcpp.h>
#include "kv_store.grpc.pb.h"
#include "739kv.h"
#include "client.h"

client *client_instance = nullptr;

bool verifyKey(const std::string s) {
    if (s.length() > (key_max_len+1)) return false;
    
    for (char c: s) {
        // Ensure the character is printable ASCII and not '[' or ']'
        // printable ASCII characters range from 32 (<space>) to 126 (~)
        if (c < 32 || c > 126 || c == '[' || c == ']') {
            return false;
        }
    }

    return true;
}

bool verifyValue(std::string s) {
    if (s.length() > (value_max_len + 1)) {
        return false;
    }

    // Check each character in the string
    for (char c : s) {
        // Ensure the character is alphanumeric or one of the few allowed characters (like space)
        if (!std::isalnum(c) && c != ' ' && c != '-' && c != '_' && c != '.' && c != ',') {
            return false;
        }

        // Exclude '[' and ']' characters
        if (c == '[' || c == ']') {
            return false;
        }
    }
    return true;
}

int kv739_init(const std::string& config_file) {
    if (client_instance != nullptr) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Client already initialized" << std::endl;
        return -1;
    }

    try {
        client_instance = new client(timeout, config_file);
        return 0;
    }
    catch (const std::exception &e) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Initialization error: " << e.what() << std::endl;
        return -1;
    }
}

int kv739_shutdown() {
    if (client_instance == nullptr) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Client not initialized" << std::endl;
        return -1;
    }
//    std::cout << "Shutdown called!\n";
    delete client_instance;
    client_instance = nullptr;
    return 0;
}

int kv739_get(const std::string key, std::string &value) {
    //std::cout << __FILE__ << "[" << __LINE__ << "]" << "inside kv739_get()" << std::endl;
    int status = -1;
    
    if (client_instance == nullptr) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Client not initialized" << std::endl;
        return status;
    }

    if (!verifyKey(key)) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Invalid key: " << std::endl;
        std::cerr << key << std::endl;
        return status;
    }

    // std::cout << __FILE__ << "[" << __LINE__ << "]" << "calling get()" << std::endl;
    // std::cout << __FILE__ << "[" << __LINE__ << "]" << "key: " << key << std::endl;
    
    try {
        status = client_instance->get(key, value);
    } catch (std::exception &e) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Could not perform get(): " << e.what() << std::endl;
    }
    // std::cout << __FILE__ << "[" << __LINE__ << "]" << "response for get()" << std::endl;
    // std::cout << __FILE__ << "[" << __LINE__ << "]" << "status: " << status << std::endl; 
    //std::cout << __FILE__ << "[" << __LINE__ << "]" << "key: " << key << std::endl;
    // std::cout << __FILE__ << "[" << __LINE__ << "]" << "value: " << value << std::endl;

    return status;
}

// int kv739_get_with_duration(const std::string key, std::string &value) {
//     //std::cout << __FILE__ << "[" << __LINE__ << "]" << "inside kv739_get()" << std::endl;
//     int status = -1;
    
//     if (client_instance == nullptr) {
//         std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Client not initialized" << std::endl;
//         return status;
//     }

//     if (!verifyKey(key)) {
//         std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Invalid key: " << std::endl;
//         std::cerr << key << std::endl;
//         return status;
//     }

//     std::cout << __FILE__ << "[" << __LINE__ << "]" << "calling get()" << std::endl;
//     std::cout << __FILE__ << "[" << __LINE__ << "]" << "key: " << key << std::endl;
    
//     try {
//         status = client_instance->get(key, value);
//     } catch (std::exception &e) {
//         std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Could not perform get(): " << e.what() << std::endl;
//     }
//     std::cout << __FILE__ << "[" << __LINE__ << "]" << "response for get()" << std::endl;
//     std::cout << __FILE__ << "[" << __LINE__ << "]" << "status: " << status << std::endl; 
//     //std::cout << __FILE__ << "[" << __LINE__ << "]" << "key: " << key << std::endl;
//     std::cout << __FILE__ << "[" << __LINE__ << "]" << "value: " << value << std::endl;

//     return status;
// }

int kv739_put(const std::string key, const std::string value, std::string &old_value) {
    //std::cout << __FILE__ << "[" << __LINE__ << "]" << "inside kv739_put()" << std::endl;
    int status = -1;
    
    if (client_instance == nullptr) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Client not initialized" << std::endl;
        return status;
    }

    if (!verifyKey(key)) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Invalid key: " << std::endl;
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << key << std::endl;
        return status;
    }

    if (!verifyValue(value)) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Invalid value: " << std::endl;
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << value << std::endl;
        return status;
    }

    //std::cout << __FILE__ << "[" << __LINE__ << "]" << "calling put()" << std::endl;
    //std::cout << __FILE__ << "[" << __LINE__ << "]" << "key: " << key << std::endl;
    //std::cout << __FILE__ << "[" << __LINE__ << "]" << "value: " << value << std::endl;

    try {
        status = client_instance->put(key, value, old_value);
    } catch (std::exception &e) {
        std::cerr << __FILE__ << "[" << __LINE__ << "]" << "Could not perform put(): " << e.what() << std::endl;
    }
    
    // std::cout << __FILE__ << "[" << __LINE__ << "]" << "response for put()" << std::endl;
    // std::cout << __FILE__ << "[" << __LINE__ << "]" << "status: " << status << std::endl; 
    //std::cout << __FILE__ << "[" << __LINE__ << "]" << "key: " << key << std::endl;
    // std::cout << __FILE__ << "[" << __LINE__ << "]" << "old_value: " << old_value << std::endl;
   
    return status;
}
