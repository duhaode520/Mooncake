#pragma once

#include <memory>
#include <string>

#include "serialize/serializer_backend.h"
#include "snapshot_provider.h"
#include "types.h"

namespace mooncake {

// SnapshotProvider implementation backed by SerializerBackend.
//
// It follows the snapshot layout used by MasterService:
//   mooncake_master_snapshot/latest.txt
//   mooncake_master_snapshot/<snapshot_id>/manifest.txt
//   mooncake_master_snapshot/<snapshot_id>/metadata
// (and parses manifest[9] as oplog_seq_id when present)
class SerializerBackendSnapshotProvider final : public SnapshotProvider {
   public:
    SerializerBackendSnapshotProvider(
        SnapshotBackendType backend_type, const std::string& etcd_endpoints,
        BufferAllocatorType memory_allocator_type = BufferAllocatorType::OFFSET,
        std::string snapshot_root = "mooncake_master_snapshot");

    bool LoadLatestSnapshot(
        const std::string& cluster_id, std::string& snapshot_id,
        uint64_t& snapshot_sequence_id,
        std::vector<std::pair<std::string, StandbyObjectMetadata>>& snapshot)
        override;

    bool GetSnapshotSegments(
        std::vector<std::pair<Segment, UUID>>& segments) const override {
        segments = cached_segments_;
        return !segments.empty();
    }

   private:
    std::string snapshot_root_;
    std::unique_ptr<SerializerBackend> backend_;
    SnapshotBackendType backend_type_;
    std::string etcd_endpoints_;
    BufferAllocatorType memory_allocator_type_;
    std::vector<std::pair<Segment, UUID>> cached_segments_;
};

}  // namespace mooncake
