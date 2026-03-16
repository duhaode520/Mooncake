// mooncake-store/include/oplog_store_factory.h
#pragma once

#include <algorithm>
#include <memory>
#include <stdexcept>
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

// Convert string to OpLogStoreType (case-insensitive)
inline OpLogStoreType ParseOpLogStoreType(const std::string& type_str) {
    auto lower = type_str;
    std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
    if (lower == "etcd") {
        return OpLogStoreType::ETCD;
    }
    throw std::invalid_argument("Unknown OpLogStoreType: " + type_str);
}

inline std::string OpLogStoreTypeToString(OpLogStoreType type) {
    switch (type) {
        case OpLogStoreType::ETCD:
            return "etcd";
        default:
            return "unknown";
    }
}

class OpLogStoreFactory {
   public:
    static std::unique_ptr<OpLogStore> Create(OpLogStoreType type,
                                              const std::string& cluster_id,
                                              OpLogStoreRole role);
};

}  // namespace mooncake
