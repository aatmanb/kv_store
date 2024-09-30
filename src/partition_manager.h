#pragma once

#include "data.h"
#include "singleton.h"
#include <memory>

const std::string DB_NAME_PREFIX = "test";
const std::string DB_NAME_SUFFIX = ".db";
const int NUM_PARTITIONS = 16;
const int PRIME = 31;

namespace key_value_store {
    class PartitionManager: public Singleton<PartitionManager> {
    friend class Singleton<PartitionManager>;

    public:
        inline Partition* get_partition(const std::string &key) {
            int hash = 0;
            for (int i=0; i<std::min<int>(4, key.size()); i++) {
                hash += (int)key[i] * pow(PRIME, i);
                hash %= NUM_PARTITIONS;
            }
            return partitions[hash].get();
        }

    private:
        std::vector<std::unique_ptr<Partition>> partitions;

        PartitionManager(): Singleton<PartitionManager>() {
            for (int i=0; i<NUM_PARTITIONS; i++) {
                std::string db_name = DB_NAME_PREFIX + std::to_string(i) + DB_NAME_SUFFIX;
                partitions.emplace_back(std::move(std::make_unique<Partition>(db_name)));
            }
        }
    };
}
