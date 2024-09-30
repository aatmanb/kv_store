#include "data.h"
#include <mutex>

const std::string EMPTY_STRING = "";

namespace key_value_store {
    Partition::Partition(std::string db_name): db_name(db_name) {
        // TODO(): How to handle errors?
        db_utils = &(DatabaseUtils::get_instance(db_name.c_str()));
    }

    std::string Partition::get(const std::string &key) noexcept {
        {
            std::shared_lock<std::shared_mutex> read_lock {rw_mutex};
            if (hash_map.count(key)) {
                std::cout << __FILE__ << "[" << __LINE__  <<"]" << "Reading from hash map\n";
                return hash_map[key];
            }
            // Check DB
            std::string value = db_utils->get_value(key.c_str());
            if (value != EMPTY_STRING) {
                read_lock.unlock();
                {
                    std::unique_lock<std::shared_mutex> write_lock {rw_mutex};
                    hash_map[key] = value;
                }
            }
            return value;   
        }
    }

    std::string Partition::put(const std::string &key, const std::string &value) noexcept {
        {
            std::unique_lock<std::shared_mutex> write_lock {rw_mutex};
            // Update hash map and db
            hash_map[key] = value;
            std::string old_value = db_utils->put_value(key.c_str(), value.c_str());
            return old_value;
        }
    }
}