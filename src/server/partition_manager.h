#pragma once

#include "data.h"
#include "singleton.h"
#include <memory>

const std::string DB_NAME_PREFIX = "test";
const std::string DB_NAME_SUFFIX = ".db";
const int NUM_PARTITIONS = 1;
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
	    // std::cout << "Partition for key: " << key << " is: " << hash << "\n";
            return partitions[hash].get();
        }

	void create_partitions() {
		return create_partitions_internal(); 
	}

	void set_db_path_directory(std::string path) {
		return set_db_path_internal(path);
	}

    private:
        std::vector<std::unique_ptr<Partition>> partitions;
	std::string db_path;

        PartitionManager(): Singleton<PartitionManager>() {}
	
	void set_db_path_internal(std::string path) {
		db_path = path;
	}

	void create_partitions_internal() {
            for (int i=0; i<NUM_PARTITIONS; i++) {
                std::string db_name = db_path + DB_NAME_PREFIX + std::to_string(i) + DB_NAME_SUFFIX;
                partitions.emplace_back(std::move(std::make_unique<Partition>(db_name)));
            }	
	}
    };
}
