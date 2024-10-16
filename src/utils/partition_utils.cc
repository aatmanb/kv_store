#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <random>
#include <chrono>

#include "partition_utils.h"

PartitionConfig::PartitionConfig(int _id) {
    id = _id;
    current_server_idx = 0;
}

void
PartitionConfig::init() {
    std::cout << "Intializing PartitionConfig" << std::endl;
    num_servers = servers.size();

    std::cout << "partition " << id << " has " << num_servers << " servers" << std::endl;

    // First server is the head and the last is tail
    head_server = servers[0];
    tail_server = servers[num_servers-1];
    
    std::cout << "partition " << id << " head_server is " << head_server << " and tail server is " << tail_server << std::endl;

    // Shuffle the list so that each client chooses a randomz server to send request to
    std::random_device rd;
    std::mt19937 rng(rd());
    std::shuffle(servers.begin(), servers.end(), rng); 
}

void
PartitionConfig::addServer(std::string& server) {
    servers.push_back(server);
}

std::string 
PartitionConfig::getServer() {
    std::string server = servers[current_server_idx];
    current_server_idx++;

    return server;
} 
