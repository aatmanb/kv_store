#include "master.h"

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"

ABSL_FLAG(uint32_t, id, 1, "Server id");
ABSL_FLAG(std::string, db_dir, "", "directory to store the database");
ABSL_FLAG(std::string, config_file, "", "config file for cluster");
ABSL_FLAG(int, port, 50000, "port");
ABSL_FLAG(std::string, log_dir, "", "log directory");

int main(int argc, char** argv) {
    std::cout.setf(std::ios::unitbuf);
    absl::ParseCommandLine(argc, argv);
    std::string db_dir = absl::GetFlag(FLAGS_db_dir);
    std::string config_path = absl::GetFlag(FLAGS_config_file);
    int port = absl::GetFlag(FLAGS_port);
    std::string log_dir = absl::GetFlag(FLAGS_log_dir);
    if (db_dir == "") {
        std::cerr << "Database directory unkown" << std::endl;
        std::exit(1);
    }

    if (config_path == "") {
        std::cerr << "Config path unkown" << std::endl;
        std::exit(1);
    }
    
    if (log_dir == "") {
        std::cerr << "Log dir unkown" << std::endl;
        std::exit(1);
    }
    
    key_value_store::start_master_node(db_dir, config_path, port, log_dir);
    return 0;
}
