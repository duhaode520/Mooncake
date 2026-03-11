// mooncake-store/include/oplog_store.h
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "oplog_manager.h"
#include "types.h"

namespace mooncake {

// Abstract interface for OpLog persistent storage.
// Implementations: EtcdOpLogStore, (future) HdfsOpLogStore, etc.
class OpLogStore {
   public:
    virtual ~OpLogStore() = default;
    virtual ErrorCode Init() = 0;

    // Write
    virtual ErrorCode WriteOpLog(const OpLogEntry& entry,
                                 bool sync = true) = 0;

    // Read
    virtual ErrorCode ReadOpLog(uint64_t sequence_id, OpLogEntry& entry) = 0;
    virtual ErrorCode ReadOpLogSince(uint64_t start_sequence_id, size_t limit,
                                     std::vector<OpLogEntry>& entries) = 0;

    // Sequence ID management
    virtual ErrorCode GetLatestSequenceId(uint64_t& sequence_id) = 0;
    virtual ErrorCode GetMaxSequenceId(uint64_t& sequence_id) = 0;
    virtual ErrorCode UpdateLatestSequenceId(uint64_t sequence_id) = 0;

    // Snapshot
    virtual ErrorCode RecordSnapshotSequenceId(
        const std::string& snapshot_id, uint64_t sequence_id) = 0;
    virtual ErrorCode GetSnapshotSequenceId(const std::string& snapshot_id,
                                            uint64_t& sequence_id) = 0;

    // Cleanup
    virtual ErrorCode CleanupOpLogBefore(uint64_t before_sequence_id) = 0;
};

}  // namespace mooncake
