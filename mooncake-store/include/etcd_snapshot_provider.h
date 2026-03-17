#pragma once

#include "snapshot_provider.h"
#include <memory>
#include <string>

namespace mooncake {

class SerializerBackend;

class EtcdSnapshotProvider : public SnapshotProvider {
   public:
    EtcdSnapshotProvider(const std::string& etcd_endpoints,
                         const std::string& snapshot_root = "snapshots");

    // Implement interface
    bool LoadLatestSnapshot(
        const std::string& cluster_id, std::string& snapshot_id,
        uint64_t& snapshot_sequence_id,
        std::vector<std::pair<std::string, StandbyObjectMetadata>>& snapshot)
        override;

   private:
    std::string snapshot_root_;
    std::unique_ptr<SerializerBackend> backend_;
};

}  // namespace mooncake