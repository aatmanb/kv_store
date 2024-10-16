#ifndef partition_utils_h__
#define partition_utils_h__

#include<string>
#include<vector>

class PartitionConfig {

public:
    PartitionConfig(int _id);
    
    void init();
    void addServer(std::string& server);
    std::string getServer();

private:
    // Servers in this partition
    std::vector<std::string> servers;

    // used to choose the next server if the current one fails
    int current_server_idx;

    // ID of this partition
    int id;

    // number of servers in this partition
    int num_servers;

    // port number of head and tail servers
    std::string head_server;
    std::string tail_server;

};

#endif // partition_utils_h__
