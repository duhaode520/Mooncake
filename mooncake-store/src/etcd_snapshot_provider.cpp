#include "etcd_snapshot_provider.h"
#include <glog/logging.h>
#include <boost/algorithm/string.hpp>
#include "serialize/serializer_backend.h"
#include "utils/utils.h" 
#include "utils/zstd_util.h"
#include <msgpack.hpp>

namespace mooncake {

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
    std::string latest_path = snapshot_root_ + "/latest.txt";
    std::string latest_content;
    if (!backend_->DownloadString(latest_path, latest_content)) {
        LOG(WARNING) << "[Standby] Snapshot latest pointer not found: " << latest_path;
        return false;
    }
    
    // Trim and parse ID
    std::string id_str = latest_content;
    boost::trim(id_str); 
    // Handle "ts|id" format if present
    auto pipe_pos = id_str.rfind('|');
    snapshot_id = (pipe_pos == std::string::npos) ? id_str : id_str.substr(pipe_pos + 1);
    
    if (snapshot_id.empty()) {
        return false;
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
    }
    LOG(INFO) << "[Standby] Snapshot " << snapshot_id << " has sequence_id=" << snapshot_sequence_id;

    // 3. Download Metadata
    std::vector<uint8_t> meta_blob;
    if (!backend_->DownloadBuffer(prefix + "metadata.msgpack", meta_blob)) {
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

    // Double check sequence id from metadata body (integrity check)
    if (oplog_seq_obj) {
        uint64_t meta_seq_id = oplog_seq_obj->as<uint64_t>();
        if (snapshot_sequence_id > 0 && snapshot_sequence_id != meta_seq_id) {
            LOG(WARNING) << "[Standby] Manifest seq_id (" << snapshot_sequence_id 
                         << ") mismatches metadata seq_id (" << meta_seq_id << ")";
        }
        snapshot_sequence_id = meta_seq_id;
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