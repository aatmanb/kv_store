#include <iostream>

#include "new_server.h"

ABSL_FLAG(int, id, -1, "server id");
// ABSL_FLAG(std::string, db_dir, "", "directory to store the database");
ABSL_FLAG(uint16_t, port, -1, "Server port for the service");
ABSL_FLAG(uint16_t, master_port, -1, "port of master node");
ABSL_FLAG(std::string, log_dir, "", "log directory");
ABSL_FLAG(std::string, db_dir, "", "db directory");

int main(int argc, char** argv) {
    std::cout.setf(std::ios::unitbuf);
    absl::ParseCommandLine(argc, argv);

    int id = absl::GetFlag(FLAGS_id); 
    uint16_t port = absl::GetFlag(FLAGS_port); 
    uint16_t master_port = absl::GetFlag(FLAGS_master_port); 
    std::string log_dir = absl::GetFlag(FLAGS_log_dir);
    std::string db_dir = absl::GetFlag(FLAGS_db_dir);

    if ((id == -1) || (master_port == -1)) {
        std::cerr << "invalid args" << std::endl;
        std::exit(1);
    }

    std::string addr = absl::StrFormat("0.0.0.0:%d", port);
    std::string master_addr = absl::StrFormat("0.0.0.0:%d", master_port);
    
    key_value_store::runServer(id, master_addr, addr, log_dir, db_dir);
    return 0;
}
