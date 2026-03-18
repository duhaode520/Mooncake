#include "etcd_snapshot_provider.h"
#include <glog/logging.h>
#include <boost/algorithm/string.hpp>
#include <optional>
#include "serialize/serializer_backend.h"
#include "types.h"
#include "utils/zstd_util.h"
#include <msgpack.hpp>

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
// - a single snapshot_id (often a timestamp string)
// - a single line: "<ts>|<snapshot_id>"
// - multiple lines recording history; pick the newest by ts.
std::optional<LatestSnapshotCandidate> ParseLatestSnapshot(const std::string& content) {
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

EtcdSnapshotProvider::EtcdSnapshotProvider(const std::string& etcd_endpoints,
                                           const std::string& snapshot_root)
    : snapshot_root_(snapshot_root) {
    backend_ = SerializerBackend::Create(SnapshotBackendType::ETCD, etcd_endpoints);
}

bool EtcdSnapshotProvider::LoadLatestSnapshot(
    const std::string& cluster_id, std::string& snapshot_id,
    uint64_t& snapshot_sequence_id,
    std::vector<std::pair<std::string, StandbyObjectMetadata>>& snapshot) {
    
    // 1. Find latest snapshot ID
    // Path structure: snapshot_root/cluster_id/latest.txt
    // But original MasterService code uses SNAPSHOT_ROOT + "/" + SNAPSHOT_LATEST_FILE 
    // where SNAPSHOT_ROOT is configurable. 
    // Assuming snapshot_root_ passed here is equivalent to MasterService's SNAPSHOT_ROOT.
    (void)cluster_id;  // Snapshot path is global in current MasterService layout.

    std::string latest_path = snapshot_root_ + "/latest.txt";
    std::string latest_content;
    if (!backend_->DownloadString(latest_path, latest_content)) {
        LOG(WARNING) << "[Standby] Snapshot latest pointer not found: " << latest_path;
        return false;
    }
    
    auto latest = ParseLatestSnapshot(latest_content);
    if (!latest.has_value() || latest->snapshot_id.empty()) {
        LOG(WARNING) << "[Standby] latest.txt is present but no valid snapshot id parsed";
        return false;
    }

    snapshot_id = latest->snapshot_id;
    if (latest->ts.has_value()) {
        LOG(INFO) << "[Standby] Latest snapshot selected: ts=" << latest->ts.value()
                  << ", snapshot_id=" << snapshot_id;
    } else {
        LOG(INFO) << "[Standby] Latest snapshot selected: snapshot_id=" << snapshot_id;
    }

    std::string prefix = snapshot_root_ + "/" + snapshot_id + "/";
    LOG(INFO) << "[Standby] Loading snapshot from: " << prefix;

    // 2. Download Manifest
    std::string manifest_content;
    if (!backend_->DownloadString(prefix + "manifest.txt", manifest_content)) {
        LOG(ERROR) << "[Standby] Failed to download manifest for " << snapshot_id;
        return false;
    }

    // Parse Manifest for Sequence ID
    // Format: ...|status|oplog_seq_id
    std::vector<std::string> parts;
    boost::split(parts, manifest_content, boost::is_any_of("|"));
    snapshot_sequence_id = 0;
    if (parts.size() >= 10) {
        try {
            snapshot_sequence_id = std::stoull(parts[9]);
        } catch (...) {
            LOG(WARNING) << "[Standby] Failed to parse oplog_seq_id from manifest";
        }
    } else {
        LOG(WARNING) << "[Standby] Invalid manifest format (parts=" << parts.size()
                     << "), cannot parse oplog_seq_id";
    }
    LOG(INFO) << "[Standby] Snapshot " << snapshot_id << " has sequence_id=" << snapshot_sequence_id;

    // 3. Download Metadata
    std::vector<uint8_t> meta_blob;
    if (!backend_->DownloadBuffer(prefix + "metadata", meta_blob)) {
        LOG(ERROR) << "[Standby] Failed to download metadata for " << snapshot_id;
        return false;
    }

    // 4. Deserialize (Using MessagePack)
    msgpack::object_handle oh;
    try {
        oh = msgpack::unpack(reinterpret_cast<const char*>(meta_blob.data()), meta_blob.size());
    } catch (...) {
        LOG(ERROR) << "[Standby] Failed to unpack metadata msgpack";
        return false;
    }

    const auto& obj = oh.get();
    if (obj.type != msgpack::type::MAP) return false;

    // Extract shards
    const msgpack::object* shards_obj = nullptr;
    const msgpack::object* oplog_seq_obj = nullptr;
    
    for (size_t i = 0; i < obj.via.map.size; ++i) {
        auto key_obj = obj.via.map.ptr[i].key;
        if (key_obj.type != msgpack::type::STR) continue;
        std::string key(key_obj.via.str.ptr, key_obj.via.str.size);
        
        if (key == "shards") shards_obj = &obj.via.map.ptr[i].val;
        else if (key == "oplog_sequence_id") oplog_seq_obj = &obj.via.map.ptr[i].val;
    }

    // Double check sequence id from metadata body (integrity check).
    // IMPORTANT: Do not overwrite a valid manifest seq_id with a zero value.
    if (oplog_seq_obj) {
        uint64_t meta_seq_id = oplog_seq_obj->as<uint64_t>();
        if (meta_seq_id == 0) {
            if (snapshot_sequence_id > 0) {
                LOG(WARNING) << "[Standby] Metadata oplog_sequence_id is 0; keeping manifest seq_id="
                             << snapshot_sequence_id;
            } else {
                LOG(WARNING) << "[Standby] Both manifest seq_id and metadata oplog_sequence_id are 0";
            }
        } else {
            if (snapshot_sequence_id > 0 && snapshot_sequence_id != meta_seq_id) {
                LOG(WARNING) << "[Standby] Manifest seq_id (" << snapshot_sequence_id
                             << ") mismatches metadata seq_id (" << meta_seq_id << ")";
            }
            snapshot_sequence_id = meta_seq_id;
        }
    }

    if (!shards_obj || shards_obj->type != msgpack::type::MAP) {
        LOG(ERROR) << "[Standby] Invalid shards format in snapshot";
        return false;
    }

    // Parse Shards
    snapshot.clear();
    
    for (size_t i = 0; i < shards_obj->via.map.size; ++i) {
        const auto& shard_blob_obj = shards_obj->via.map.ptr[i].val;
        // Shard data is stored as compressed binary
        if (shard_blob_obj.type != msgpack::type::BIN) continue;

        // Decompress shard
        std::vector<uint8_t> compressed(
            shard_blob_obj.via.bin.ptr,
            shard_blob_obj.via.bin.ptr + shard_blob_obj.via.bin.size);
        
        std::vector<uint8_t> shard_data;
        try {
            shard_data = zstd_decompress(compressed);
        } catch (const std::exception& e) {
             LOG(ERROR) << "[Standby] Decompression failed for shard " << i << ": " << e.what();
             continue;
        }

        msgpack::object_handle shard_oh;
        try {
             shard_oh = msgpack::unpack(
                reinterpret_cast<const char*>(shard_data.data()), shard_data.size());
        } catch (...) { continue; }

        auto shard_root = shard_oh.get();
        if (shard_root.type != msgpack::type::MAP) continue;

        const msgpack::object* meta_arr = nullptr;
        for(size_t k=0; k<shard_root.via.map.size; ++k) {
            std::string key(shard_root.via.map.ptr[k].key.via.str.ptr, 
                            shard_root.via.map.ptr[k].key.via.str.size);
            if (key == "metadata") {
                meta_arr = &shard_root.via.map.ptr[k].val;
                break;
            }
        }
        
        if (!meta_arr || meta_arr->type != msgpack::type::ARRAY) continue;
        
        // Shard Format: [key1, value_array1, key2, value_array2, ...]
        for (size_t k = 0; k < meta_arr->via.array.size; k += 2) {
            std::string key(meta_arr->via.array.ptr[k].via.str.ptr,
                            meta_arr->via.array.ptr[k].via.str.size);
            const auto& val_obj = meta_arr->via.array.ptr[k+1];
            
            // Metadata Array Format: [client_id, put_start, size, lease, soft_flag, soft_val, rep_cnt, rep1...]
            // Index:                 0          1          2     3      4          5         6        7...
            if (val_obj.type != msgpack::type::ARRAY || val_obj.via.array.size < 7) continue;
            
            StandbyObjectMetadata meta;
            auto* p = val_obj.via.array.ptr;
            
            // 0: client_id
            std::string cid_str(p[0].via.str.ptr, p[0].via.str.size);
            StringToUuid(cid_str, meta.client_id);
            // 2: size
            meta.size = p[2].as<uint64_t>();
            
            // NOTE: Detailed replica parsing is skipped here because Standby 
            // doesn't manage Replica/Buffer pointers directly from snapshots.
            // When Standby promotes to Master, it usually starts with empty memory/disk locations 
            // OR scans local disks. 
            // The snapshot metadata mainly tells us "this Key exists and belongs to Client X".
            
            meta.last_sequence_id = snapshot_sequence_id; 
            snapshot.emplace_back(key, std::move(meta));
        }
    }

    return true;
}

} // namespace mooncake