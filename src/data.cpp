#include "data.h"

namespace key_value_store {
    Partition::Partition(std::string db_name): db_name(db_name) {}

    std::string Partition::get(const std::string &key) noexcept {
        {
            boost::shared_lock_guard<boost::shared_mutex> read_lock {rw_mutex};
            if (!hash_map.count(key)) {
                return "Not found";
            }
        }
        return hash_map[key];
    }

    void put(const std::string &key, const std::string &value) noexcept {
        {
            boost::unique_lock_guard<boost::shared_mutex> write_lock {rw_mutex};
            hash_map[key] = value;
            // TODO(): Write to SQLite
        }
    }


}