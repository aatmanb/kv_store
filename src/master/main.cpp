#include "master.h"

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"
#include "absl/strings/str_format.h"

int main(int argc, char** argv) {
    absl::ParseCommandLine(argc, argv);
    // std::string db_dir = absl::GetFlag(FLAGS_db_dir) + "/";
    // if (db_dir == "") {
    //     std::cerr << "Database directory unkown" << std::endl;
    //     std::exit(1);
    // }
    
    key_value_store::start_master_node();
    return 0;
}