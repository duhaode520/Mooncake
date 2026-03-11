#include "oplog_store_factory.h"

#include <glog/logging.h>

#ifdef STORE_USE_ETCD
#include "etcd_oplog_change_notifier.h"
#include "etcd_oplog_store.h"
#endif

namespace mooncake {

std::unique_ptr<OpLogStore> OpLogStoreFactory::Create(
    const std::string& cluster_id, OpLogStoreRole role) {
#ifdef STORE_USE_ETCD
    bool batch_update = (role == OpLogStoreRole::WRITER);
    bool batch_write = (role == OpLogStoreRole::WRITER);
    auto store =
        std::make_unique<EtcdOpLogStore>(cluster_id, batch_update, batch_write);
    if (store->Init() != ErrorCode::OK) {
        LOG(ERROR) << "OpLogStoreFactory: failed to init EtcdOpLogStore"
                   << ", cluster_id=" << cluster_id;
        return nullptr;
    }
    return store;
#else
    (void)cluster_id;
    (void)role;
    return nullptr;
#endif
}

std::unique_ptr<OpLogChangeNotifier> OpLogStoreFactory::CreateNotifier(
    const std::string& cluster_id, OpLogStore* store) {
#ifdef STORE_USE_ETCD
    auto* etcd_store = dynamic_cast<EtcdOpLogStore*>(store);
    if (!etcd_store) {
        LOG(ERROR) << "OpLogStoreFactory::CreateNotifier requires "
                      "EtcdOpLogStore (got incompatible store type)";
        return nullptr;
    }
    return std::make_unique<EtcdOpLogChangeNotifier>(cluster_id, etcd_store);
#else
    (void)cluster_id;
    (void)store;
    return nullptr;
#endif
}

}  // namespace mooncake
