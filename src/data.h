#pragma once

#include "../external/tsl/robin_map.h"
#include <string>
#include <boost/thread.hpp>

namespace key_value_store {
    class Partition {
    public:
        Partition(std::string db_name);

        std::string get(const std::string &key) noexcept;

        void put(const std::string &key, const std::string &value) noexcept;

    private:
        tsl::robin_map<std::string, std::string> hash_map;

        std::string db_name;

        boost::shared_mutex rw_mutex;

        // boost::mutex write_mutex;
    };
}