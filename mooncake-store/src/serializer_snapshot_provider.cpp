#include "serializer_snapshot_provider.h"

#include <glog/logging.h>

#include <boost/algorithm/string.hpp>
#include <msgpack.hpp>
#include <optional>
#include <utility>

#include "master_config.h"
#include "master_service.h"

namespace mooncake {

namespace {

struct LatestSnapshotCandidate {
    std::string snapshot_id;
    std::optional<uint64_t> ts;
};

std::optional<uint64_t> ParseUint64Loose(const std::string& s) {
    if (s.empty()) return std::nullopt;
    for (char c : s) {
        if (c < '0' || c > '9') return std::nullopt;
    }
    try {
        return std::stoull(s);
    } catch (...) {
        return std::nullopt;
    }
}

// latest.txt may be:
// - a single snapshot_id
// - a single line: "<ts>|<snapshot_id>"
// - multiple lines recording history; pick the newest by ts.
std::optional<LatestSnapshotCandidate> ParseLatestSnapshot(
    const std::string& content) {
    std::vector<std::string> lines;
    boost::split(lines, content, boost::is_any_of("\n"));

    std::optional<LatestSnapshotCandidate> best;
    for (auto& raw : lines) {
        std::string line = raw;
        boost::trim(line);
        if (line.empty()) continue;

        LatestSnapshotCandidate cand;
        cand.snapshot_id = line;

        auto pipe_pos = line.rfind('|');
        if (pipe_pos != std::string::npos) {
            std::string ts_str = line.substr(0, pipe_pos);
            std::string id_str = line.substr(pipe_pos + 1);
            boost::trim(ts_str);
            boost::trim(id_str);
            if (!id_str.empty()) {
                cand.snapshot_id = id_str;
            }
            cand.ts = ParseUint64Loose(ts_str);
        } else {
            cand.ts = ParseUint64Loose(line);
        }

        if (cand.snapshot_id.empty()) {
            continue;
        }

        if (!best.has_value()) {
            best = cand;
            continue;
        }

        // Prefer the candidate with a larger timestamp.
        if (cand.ts.has_value() && best->ts.has_value()) {
            if (cand.ts.value() >= best->ts.value()) {
                best = cand;
            }
            continue;
        }
        if (cand.ts.has_value() && !best->ts.has_value()) {
            best = cand;
            continue;
        }
        if (!cand.ts.has_value() && best->ts.has_value()) {
            continue;
        }

        // Neither has ts: keep the later non-empty line.
        best = cand;
    }
    return best;
}

}  // namespace

SerializerBackendSnapshotProvider::SerializerBackendSnapshotProvider(
    SnapshotBackendType backend_type, const std::string& etcd_endpoints,
        BufferAllocatorType memory_allocator_type,
    std::string snapshot_root)
        : snapshot_root_(std::move(snapshot_root)),
            backend_type_(backend_type),
            etcd_endpoints_(etcd_endpoints),
            memory_allocator_type_(memory_allocator_type) {
        backend_ = SerializerBackend::Create(backend_type_, etcd_endpoints_);
}

bool SerializerBackendSnapshotProvider::LoadLatestSnapshot(
    const std::string& cluster_id, std::string& snapshot_id,
    uint64_t& snapshot_sequence_id,
    std::vector<std::pair<std::string, StandbyObjectMetadata>>& snapshot) {
    // Current MasterService snapshot layout is global (not namespaced by cluster_id).
    (void)cluster_id;

    // 1) Resolve latest snapshot id
    std::string latest_path = snapshot_root_ + "/latest.txt";
    std::string latest_content;
    if (!backend_->DownloadString(latest_path, latest_content)) {
        LOG(WARNING) << "[Standby] Snapshot latest pointer not found: "
                     << latest_path;
        return false;
    }

    auto latest = ParseLatestSnapshot(latest_content);
    if (!latest.has_value() || latest->snapshot_id.empty()) {
        LOG(WARNING)
            << "[Standby] latest.txt is present but no valid snapshot id parsed";
        return false;
    }

    snapshot_id = latest->snapshot_id;
    if (latest->ts.has_value()) {
        LOG(INFO) << "[Standby] Latest snapshot selected: ts="
                  << latest->ts.value() << ", snapshot_id=" << snapshot_id;
    } else {
        LOG(INFO) << "[Standby] Latest snapshot selected: snapshot_id="
                  << snapshot_id;
    }

    // Reuse MasterService::RestoreState() to load and deserialize snapshot.
    // This avoids duplicating the snapshot format parsing logic here.
    // NOTE: MasterService constructor starts background threads; this provider
    // keeps the temporary instance short-lived.
    MasterServiceConfig cfg;
    cfg.enable_ha = false;
    cfg.cluster_id = std::string();
    cfg.enable_snapshot_restore = true;
    cfg.enable_snapshot_restore_clean_metadata = false;
    cfg.enable_snapshot = false;
    cfg.snapshot_backend_type = backend_type_;
    cfg.etcd_endpoints = etcd_endpoints_;
    cfg.memory_allocator = memory_allocator_type_;
    cfg.snapshot_backup_dir = "/tmp/mooncake_standby_restore";

    LOG(INFO) << "[Standby] Snapshot bootstrap: restoring via MasterService::RestoreState, "
                 "backend=" << SnapshotBackendTypeToString(backend_type_)
              << ", snapshot_root=" << snapshot_root_;

    bool restored = false;
    {
        MasterService master(cfg);
        restored = master.RestoredFromSnapshot();
        snapshot_sequence_id = master.GetOpLogLastSequenceId();
        // Export full metadata baseline for standby.
        // If memory allocator type is CACHELIB, RestoreState may be expensive.
        // Caller can choose to set memory_allocator_type_ to OFFSET in config.
        master.ExportStandbySnapshot(snapshot, snapshot_sequence_id,
                                     /*include_memory_replicas=*/true);
    }

    if (!restored) {
        LOG(WARNING) << "[Standby] Snapshot restore failed in MasterService::RestoreState";
        return false;
    }

    if (snapshot.empty()) {
        LOG(INFO) << "[Standby] Snapshot restore succeeded but exported snapshot is empty";
    }
    LOG(INFO) << "[Standby] Snapshot load complete via RestoreState: keys="
              << snapshot.size() << ", sequence_id=" << snapshot_sequence_id;
    // Even if there are no keys, the snapshot boundary (sequence_id) still matters.
    return true;
}

}  // namespace mooncake
