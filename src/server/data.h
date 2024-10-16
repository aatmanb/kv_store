#pragma once

#include "tsl/robin_map.h"
#include "dbutils.h"
#include <string>
#include <memory>
#include <shared_mutex>

namespace key_value_store {
    class Partition {
    public:
        Partition(std::string db_name);

        std::string get(const std::string &key) noexcept;

        std::string put(const std::string &key, const std::string &value) noexcept;

    private:
        tsl::robin_map<std::string, std::string> hash_map;

        std::string db_name;

        std::shared_mutex rw_mutex;

	    std::unique_ptr<DatabaseUtils> db_utils;
    };
}
