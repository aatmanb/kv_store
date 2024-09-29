#include "server.h"

namespace key_value_store {
  int main(int argc, char** argv) {
    absl::ParseCommandLine(argc, argv);
    runServer(absl::GetFlag(FLAGS_port));
    return 0;
  }
}
