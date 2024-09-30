#include "server.h"

// ABSL_FLAG(uint16_t, port, 50051, "Server port for the service");

// namespace key_value_store {
  int main(int argc, char** argv) {
    // absl::ParseCommandLine(argc, argv);
    key_value_store::runServer(50051);
    return 0;
  }
// }
