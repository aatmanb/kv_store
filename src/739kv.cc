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

bool verifyKey(const std::string s) {
    if (s.length() > 128) return false;
    
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
    if (s.length() > 2048) return false;

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

int kv739_init(char *server_name) {
    if (client_instance != nullptr) {
        std::cerr << "Client already initialized" << std::endl;
        return -1;
    }

    std::string target_str = charArrayToString(server_name);

    try {
        client_instance = new client(grpc::CreateChannel(target_str, grpc::InsecureChannelCredentials()));
        return 0;
    }
    catch (const std::exception &e) {
        std::cerr << "Initialization error: " << e.what() << std::endl;
        return -1;
    }
}

int kv739_shutdown() {
    if (client_instance == nullptr) {
        std::cerr << "Client not initialized" << std::endl;
        return -1;
    }

    delete client_instance;
    client_instance = nullptr;
    return 0;
}

int kv739_get(char *key, char *value) {
    std::cout << "[lib] get()" << key << std::endl;
    int status = -1;
    
    if (client_instance == nullptr) {
        std::cerr << "Client not initialized" << std::endl;
        return status;
    }

    std::string key_str = charArrayToString(key);
    std::string value_str;

    if (!verifyKey(key_str)) {
        std::cerr << "Invalid key: " << std::endl;
        std::cerr << key_str << std::endl;
        return status;
    }

    std::cout << "key: " << key_str << std::endl;
    
    try {
        status = client_instance->get(key_str, value_str);
    } catch (std::exception &e) {
        std::cerr << "Could not perform get(): " << e.what() << std::endl;
    }
    std::cout << "[lib] response for get()" << std::endl;
    std::cout << "key: " << key_str << std::endl;
    std::cout << "value: " << value_str << std::endl;
    
    strcpy(value, stringToCharArray(value_str));

    return status;
}

int kv739_put(char *key, char *value, char *old_value) {
    std::cout << "[lib] put()" << std::endl;
    int status = -1;
    
    if (client_instance == nullptr) {
        std::cerr << "Client not initialized" << std::endl;
        return status;
    }

    std::string key_str = charArrayToString(key);
    std::string value_str = charArrayToString(value);
    std::string old_value_str;

    if (!verifyKey(key_str)) {
        std::cerr << "Invalid key: " << std::endl;
        std::cerr << key_str << std::endl;
        return status;
    }

    if (!verifyValue(value_str)) {
        std::cerr << "Invalid value: " << std::endl;
        std::cerr << value_str << std::endl;
        return status;
    }

    std::cout << "key: " << key_str << std::endl;
    std::cout << "value: " << value_str << std::endl;

    try {
        status = client_instance->put(key_str, value_str, old_value_str);
    } catch (std::exception &e) {
        std::cerr << "[lib] Could not perform put(): " << e.what() << std::endl;
    }
    
    std::cout << "[lib] response for put()" << std::endl;
    std::cout << "key: " << key_str << std::endl;
    std::cout << "old_value: " << old_value_str << std::endl;
    
    strcpy(old_value, stringToCharArray(old_value_str));    

    return status;
}