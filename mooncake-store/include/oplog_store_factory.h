// mooncake-store/include/oplog_store_factory.h
#pragma once

#include <memory>
#include <string>

#include "oplog_change_notifier.h"
#include "oplog_store.h"

namespace mooncake {

enum class OpLogStoreRole {
    WRITER,  // Primary: enable batch_write + batch_update
    READER,  // Standby: read-only
};

class OpLogStoreFactory {
   public:
    static std::unique_ptr<OpLogStore> Create(const std::string& cluster_id,
                                              OpLogStoreRole role);

    // Create a change notifier backed by the given store.
    // The store must outlive the returned notifier.
    static std::unique_ptr<OpLogChangeNotifier> CreateNotifier(
        const std::string& cluster_id, OpLogStore* store);
};

}  // namespace mooncake
