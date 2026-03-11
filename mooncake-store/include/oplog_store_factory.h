// mooncake-store/include/oplog_store_factory.h
#pragma once

#include <memory>
#include <string>

#include "oplog_store.h"

namespace mooncake {

enum class OpLogStoreRole {
    WRITER,  // Primary: enable batch_write + batch_update
    READER,  // Standby: read-only
};

enum class OpLogStoreType {
    ETCD,
    // Future: HDFS, S3, LOCAL_FS, ...
};

class OpLogStoreFactory {
   public:
    static std::unique_ptr<OpLogStore> Create(OpLogStoreType type,
                                              const std::string& cluster_id,
                                              OpLogStoreRole role);
};

}  // namespace mooncake
