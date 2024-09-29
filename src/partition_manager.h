#pragma once

#include <data.h>
#include <singleton.h>
#include <memory>

namespace key_value_store {
    class PartitionManager: public Singleton<PartitionManager> {
    friend class Singleton<PartitionManager>;

    public:
        inline Partition* get_partition(std::string &key) {
            // TODO(): hash key
            // Get partition
            return nullptr;
        }

    private:
        int num_partitions;

        std::vector<std::unique_ptr<Partition>> partitions;

        PartitionManager(int num_partitions): 
                Singleton<PartitionManager>(),
                num_partitions(num_partitions) {
            for (int i=0; i<num_partitions; i++) {
                // TODO(): Construct db name for the partition and create it
                // partitions.push_back
            }
        }
    };
}
