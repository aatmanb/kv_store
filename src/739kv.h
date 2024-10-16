#pragma once

#include <iostream>
#include <memory>
#include <string>
#include <cstring>
#include <stdexcept>
#include <cstdlib>

const uint16_t key_max_len = 128;
const uint16_t value_max_len = 2048;

const int timeout = 10; // seconds
const int wait_before_retry = 1; // seconds
const int max_retry = 5;

bool verifyKey(const std::string s);
bool verifyValue(const std::string s);

int kv739_init(const std::string& config_file);
int kv739_shutdown();
int kv739_get(const std::string key, std::string &value);
int kv739_put(const std::string key, const std::string value, std::string &old_value);
