#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include <grpcpp/grpcpp.h>
#include "kv_store.grpc.pb.h"
#include "client.h"

const uint16_t key_max_len = 128;
const uint16_t value_max_len = 2048;

const int timeout = 2;

client *client_instance = nullptr;

bool verifyKey(const std::string s);
bool verifyValue(const std::string s);

int kv739_init(const std::string server_name);
int kv739_shutdown();
int kv739_get(const std::string key, std::string &value);
int kv739_put(const std::string key, const std::string value, std::string &old_value);
