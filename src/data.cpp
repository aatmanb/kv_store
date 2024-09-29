#include "data.h"
#include <mutex>

namespace key_value_store {
    Partition::Partition(std::string db_name): db_name(db_name) {}

    std::string Partition::get(const std::string &key) noexcept {
        {
            std::shared_lock<std::shared_mutex> read_lock {rw_mutex};
            if (!hash_map.count(key)) {
                return "Not found";
            }
        }
        return hash_map[key];
    }

    void Partition::put(const std::string &key, const std::string &value) noexcept {
        {
            std::unique_lock<std::shared_mutex> write_lock {rw_mutex};
            hash_map[key] = value;
            // TODO(): Write to SQLite
        }
    }


}