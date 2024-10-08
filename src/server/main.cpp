#include <iostream>

#include "server.h"


ABSL_FLAG(uint16_t, port, 9876, "Server port for the service");
ABSL_FLAG(std::string, db_dir, "", "directory to store the database");

// namespace key_value_store {
  int main(int argc, char** argv) {
    absl::ParseCommandLine(argc, argv);
    std::string db_dir = absl::GetFlag(FLAGS_db_dir) + "/";
    if (db_dir == "") {
        std::cerr << "Database directory unkown" << std::endl;
        std::exit(1);
    }
    // key_value_store::runServer(absl::GetFlag(FLAGS_port), stringToCharArray(db_dir + "/"));
    key_value_store::runServer(absl::GetFlag(FLAGS_port), db_dir.c_str());
    return 0;
  }
// }
