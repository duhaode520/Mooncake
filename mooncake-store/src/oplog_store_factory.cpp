#include "oplog_store_factory.h"

#include <glog/logging.h>

#ifdef STORE_USE_ETCD
#include "etcd_oplog_store.h"
#endif

namespace mooncake {

std::unique_ptr<OpLogStore> OpLogStoreFactory::Create(
    OpLogStoreType type, const std::string& cluster_id, OpLogStoreRole role) {
    switch (type) {
        case OpLogStoreType::ETCD: {
#ifdef STORE_USE_ETCD
            bool batch_update = (role == OpLogStoreRole::WRITER);
            bool batch_write = (role == OpLogStoreRole::WRITER);
            auto store = std::make_unique<EtcdOpLogStore>(
                cluster_id, batch_update, batch_write);
            if (store->Init() != ErrorCode::OK) {
                LOG(ERROR)
                    << "OpLogStoreFactory: failed to init EtcdOpLogStore"
                    << ", cluster_id=" << cluster_id;
                return nullptr;
            }
            return store;
#else
            LOG(ERROR) << "OpLogStoreFactory: ETCD support not compiled in";
            return nullptr;
#endif
        }
        default:
            LOG(ERROR) << "OpLogStoreFactory: unknown store type";
            return nullptr;
    }
}

}  // namespace mooncake
