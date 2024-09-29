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
#include "utils.h"

const uint16_t key_max_len = 128;
const uint16_t value_max_len = 2048;

client *client_instance = nullptr;

bool verifyKey(char *str);
bool verifyValue(char *str);

int kv739_init(char *server_name);
int kv739_shutdown();
int kv739_get(char *key, char *value);
int kv739_put(char *key, char *value, char *old_value);
