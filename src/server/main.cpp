#include <iostream>

#include "new_server.h"

ABSL_FLAG(uint32_t, id, -1, "server id");
// ABSL_FLAG(std::string, db_dir, "", "directory to store the database");
ABSL_FLAG(uint16_t, port, -1, "Server port for the service");
ABSL_FLAG(uint16_t, master_port, -1, "port of master node");

// namespace key_value_store {
    int main(int argc, char** argv) {
        absl::ParseCommandLine(argc, argv);
        // std::string db_dir = absl::GetFlag(FLAGS_db_dir) + "/";
        // if (db_dir == "") {
        //     std::cerr << "Database directory unkown" << std::endl;
        //     std::exit(1);
        // }

        uint32_t id = absl::GetFlag(FLAGS_id); 
        uint16_t port = absl::GetFlag(FLAGS_port); 
        uint16_t master_port = absl::GetFlag(FLAGS_master_port); 
    
        if ((id == -1) || (master_port == -1)) {
            std::cerr << "invalid args" << std::endl;
            std::exit(1);
        }

        // key_value_store::runServer(absl::GetFlag(FLAGS_port), stringToCharArray(db_dir + "/"));
        std::string addr = absl::StrFormat("0.0.0.0:%d", port);
        std::string master_addr = absl::StrFormat("0.0.0.0:%d", master_port);
        
        key_value_store::runServer(master_addr, addr);
        return 0;
    }
// }
