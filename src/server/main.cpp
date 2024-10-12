#include <iostream>

#include "server.h"


ABSL_FLAG(uint32_t, id, -1, "server id");
ABSL_FLAG(std::string, db_dir, "", "directory to store the database");
ABSL_FLAG(bool, head, false, "is this head server?");
ABSL_FLAG(bool, tail, false, "is this tail server?");
ABSL_FLAG(uint16_t, port, -1, "Server port for the service");
ABSL_FLAG(uint16_t, prev_port, -1, "port of previous node");
ABSL_FLAG(uint16_t, next_port, -1, "port of next node");
ABSL_FLAG(uint16_t, head_port, -1, "port of head node");
ABSL_FLAG(uint16_t, tail_port, -1, "port of tail node");

// namespace key_value_store {
    int main(int argc, char** argv) {
        absl::ParseCommandLine(argc, argv);
        std::string db_dir = absl::GetFlag(FLAGS_db_dir) + "/";
        if (db_dir == "") {
            std::cerr << "Database directory unkown" << std::endl;
            std::exit(1);
        }
    
        bool head = absl::GetFlag(FLAGS_head);
        bool tail = absl::GetFlag(FLAGS_tail);

        uint32_t id = absl::GetFlag(FLAGS_id); 
        uint16_t port = absl::GetFlag(FLAGS_port); 
        uint16_t prev_port = absl::GetFlag(FLAGS_prev_port); 
        uint16_t next_port = absl::GetFlag(FLAGS_next_port); 
        uint16_t head_port = absl::GetFlag(FLAGS_head_port); 
        uint16_t tail_port = absl::GetFlag(FLAGS_tail_port); 
    
        if ((id == -1) || (port == -1) || (prev_port == -1) || (next_port == -1) || (head_port == -1) || (tail_port == -1)) {
            std::cerr << "invalid args" << std::endl;
            std::exit(1);
        }

        std::string addr, prev_addr, next_addr, head_addr, tail_addr;

        // key_value_store::runServer(absl::GetFlag(FLAGS_port), stringToCharArray(db_dir + "/"));
        addr = absl::StrFormat("0.0.0.0:%d", port);
        if (prev_port != -1) {
            prev_addr = absl::StrFormat("0.0.0.0:%d", prev_port);
        }
        if (next_port != -1) {
            next_addr = absl::StrFormat("0.0.0.0:%d", next_port);
        }
        if (!head) {
            head_addr = absl::StrFormat("0.0.0.0:%d", head_port);
        }
        if (!tail) {
            tail_addr = absl::StrFormat("0.0.0.0:%d", tail_port);
        }
        
        key_value_store::runServer(id, db_dir.c_str(), head, tail, addr, prev_addr, next_addr, head_addr, tail_addr);
        return 0;
    }
// }
