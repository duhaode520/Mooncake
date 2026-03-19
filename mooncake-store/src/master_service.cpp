#include "master_service.h"

#include <cassert>
#include <cstdint>
#include <iomanip>
#include <shared_mutex>
#include <sstream>
#include <regex>
#include <unordered_set>
#include <unistd.h>
#include <sys/wait.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <fcntl.h>
#include <ylt/util/tl/expected.hpp>
#include <boost/algorithm/string.hpp>

#include "allocator.h"
#include "etcd_helper.h"
#include "oplog_store_factory.h"
#include "ha_metric_manager.h"
#include "master_metric_manager.h"
#include "metadata_store.h"  // For MetadataPayload
#include "segment.h"
#include "types.h"
#include "serialize/serializer.hpp"
#include "serialize/serializer_backend.h"
#include "utils/zstd_util.h"
#include "utils/file_util.h"
#include "utils/snapshot_logger.h"
#include "utils/crc32c_util.h"
#include "utils.h"

namespace mooncake {
// Snapshot file names
static const std::string SNAPSHOT_METADATA_FILE = "metadata";
static const std::string SNAPSHOT_SEGMENTS_FILE = "segments";
static const std::string SNAPSHOT_TASK_MANAGER_FILE = "task_manager";
static const std::string SNAPSHOT_MANIFEST_FILE = "manifest.txt";
static const std::string SNAPSHOT_LATEST_FILE = "latest.txt";
static const std::string SNAPSHOT_ROOT = "mooncake_master_snapshot";
static const std::string SNAPSHOT_BACKUP_SAVE_DIR =
    "mooncake_snapshot_save_backup";
static const std::string SNAPSHOT_BACKUP_RESTORE_DIR =
    "mooncake_snapshot_restore_backup";
static const std::string SNAPSHOT_SERIALIZER_VERSION = "1.0.0";
static const std::string SNAPSHOT_SERIALIZER_TYPE = "messagepack";
static const std::string SNAPSHOT_INDEX_FILE = "index.txt";

// Helper: trim leading/trailing whitespace
static std::string TrimWhitespace(const std::string& s) {
    auto start = s.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    auto end = s.find_last_not_of(" \t\r\n");
    return s.substr(start, end - start + 1);
}

// Helper: parse snapshot index content (one snapshot ID per line, newest first)
static std::vector<std::string> ParseSnapshotIndexContent(
    const std::string& content) {
    std::vector<std::string> ids;
    std::istringstream iss(content);
    std::string line;
    while (std::getline(iss, line)) {
        std::string trimmed = TrimWhitespace(line);
        if (!trimmed.empty()) {
            ids.push_back(trimmed);
        }
    }
    return ids;
}

// Helper: build snapshot index content from ID list
static std::string BuildSnapshotIndexContent(
    const std::vector<std::string>& ids) {
    std::string result;
    for (const auto& id : ids) {
        result += id + "\n";
    }
    return result;
}

namespace {

// A minimal allocator implementation used only to keep AllocatedBuffer handles
// "valid" after standby promotion. It does NOT own memory.
class DummyBufferAllocator final : public BufferAllocatorBase {
   public:
    DummyBufferAllocator(std::string segment_name,
                         std::string transport_endpoint)
        : segment_name_(std::move(segment_name)),
          transport_endpoint_(std::move(transport_endpoint)) {}

    std::unique_ptr<AllocatedBuffer> allocate(size_t /*size*/) override {
        return nullptr;
    }
    void deallocate(AllocatedBuffer* /*handle*/) override {
        // no-op: we don't own memory
    }
    size_t capacity() const override { return kAllocatorUnknownFreeSpace; }
    size_t size() const override { return 0; }
    std::string getSegmentName() const override { return segment_name_; }
    std::string getTransportEndpoint() const override {
        return transport_endpoint_;
    }
    size_t getLargestFreeRegion() const override {
        return kAllocatorUnknownFreeSpace;
    }

   private:
    std::string segment_name_;
    std::string transport_endpoint_;
};

static Replica ReplicaFromDescriptor(
    const Replica::Descriptor& desc,
    const std::shared_ptr<BufferAllocatorBase>& allocator_keepalive) {
    if (desc.is_memory_replica()) {
        const auto& mem = desc.get_memory_descriptor();
        const auto& bd = mem.buffer_descriptor;
        if (!allocator_keepalive) {
            // This would make the buffer handle invalid immediately (allocator
            // stored as weak_ptr in AllocatedBuffer). Callers restoring from
            // standby should always provide a keepalive allocator.
            LOG(ERROR)
                << "ReplicaFromDescriptor(memory) missing keepalive allocator, "
                << "transport_endpoint=" << bd.transport_endpoint_;
        }

        auto buf = std::make_unique<AllocatedBuffer>(
            allocator_keepalive, reinterpret_cast<void*>(bd.buffer_address_),
            static_cast<size_t>(bd.size_));
        return Replica(std::move(buf), desc.status);
    }
    if (desc.is_disk_replica()) {
        const auto& disk = desc.get_disk_descriptor();
        return Replica(disk.file_path, disk.object_size, desc.status);
    }
    const auto& ld = desc.get_local_disk_descriptor();
    return Replica(ld.client_id, ld.object_size, ld.transport_endpoint,
                   desc.status);
}
}  // namespace

MasterService::MasterService() : MasterService(MasterServiceConfig()) {}

std::string MasterService::SerializeMetadataForOpLog(
    const ObjectMetadata& metadata) const {
    MetadataPayload payload;
    payload.client_id = metadata.client_id;
    payload.size = metadata.size;

    // Extract replica descriptors - get them all at once
    const auto& replicas = metadata.GetAllReplicas();
    payload.replicas.reserve(replicas.size());
    for (const auto& replica : replicas) {
        payload.replicas.push_back(replica.get_descriptor());
    }

    // NOTE: Lease information is NOT serialized because:
    // 1. Standby does not perform eviction, so lease info is not used
    // 2. After promotion, new Primary should grant fresh leases, not restore
    // old ones

    // Serialize using struct_pack (msgpack binary format)
    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

std::string MasterService::SerializeMetadataForOpLogWithoutMemReplicas(
    const ObjectMetadata& metadata) const {
    MetadataPayload payload;
    payload.client_id = metadata.client_id;
    payload.size = metadata.size;

    const auto& replicas = metadata.GetAllReplicas();
    payload.replicas.reserve(replicas.size());
    for (const auto& replica : replicas) {
        if (replica.type() == ReplicaType::MEMORY) {
            continue;
        }
        payload.replicas.push_back(replica.get_descriptor());
    }

    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

std::string MasterService::SerializeMetadataForOpLogFromReplicaDescriptors(
    const UUID& client_id, uint64_t size,
    const std::vector<Replica::Descriptor>& replicas) const {
    MetadataPayload payload;
    payload.client_id = client_id;
    payload.size = size;
    payload.replicas = replicas;
    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

MasterService::MasterService(const MasterServiceConfig& config)
    : default_kv_lease_ttl_(config.default_kv_lease_ttl),
      default_kv_soft_pin_ttl_(config.default_kv_soft_pin_ttl),
      allow_evict_soft_pinned_objects_(config.allow_evict_soft_pinned_objects),
      eviction_ratio_(config.eviction_ratio),
      eviction_high_watermark_ratio_(config.eviction_high_watermark_ratio),
      view_version_(config.view_version),
      client_live_ttl_sec_(config.client_live_ttl_sec),
      enable_ha_(config.enable_ha),
      enable_offload_(config.enable_offload),
      cluster_id_(config.cluster_id),
      oplog_store_type_(config.oplog_store_type),
      oplog_store_root_dir_(config.oplog_store_root_dir),
      oplog_poll_interval_ms_(config.oplog_poll_interval_ms),
      root_fs_dir_(config.root_fs_dir),
      global_file_segment_size_(config.global_file_segment_size),
      enable_disk_eviction_(config.enable_disk_eviction),
      quota_bytes_(config.quota_bytes),
      segment_manager_(config.memory_allocator, config.enable_cxl),
      memory_allocator_type_(config.memory_allocator),
      allocation_strategy_(
          CreateAllocationStrategy(config.allocation_strategy_type)),
      enable_snapshot_restore_(config.enable_snapshot_restore),
      enable_snapshot_restore_clean_metadata_(
          config.enable_snapshot_restore_clean_metadata),
      enable_snapshot_(config.enable_snapshot),
      snapshot_backup_dir_(config.snapshot_backup_dir),
      snapshot_interval_seconds_(config.snapshot_interval_seconds),
      snapshot_child_timeout_seconds_(config.snapshot_child_timeout_seconds),
      snapshot_retention_count_(config.snapshot_retention_count),
      etcd_endpoints_(config.etcd_endpoints),
      put_start_discard_timeout_sec_(config.put_start_discard_timeout_sec),
      put_start_release_timeout_sec_(config.put_start_release_timeout_sec),
      task_manager_(config.task_manager_config),
      cxl_path_(config.cxl_path),
      cxl_size_(config.cxl_size),
      enable_cxl_(config.enable_cxl) {
    if (enable_snapshot_ || enable_snapshot_restore_) {
        try {
            snapshot_backend_type_ = config.snapshot_backend_type;
            snapshot_backend_ = SerializerBackend::Create(snapshot_backend_type_, config.etcd_endpoints);
        } catch (const std::exception& e) {
            LOG(ERROR) << "Failed to create snapshot backend: " << e.what();
            throw std::runtime_error(
                fmt::format("Failed to create snapshot backend: {}", e.what()));
        }
        if (!snapshot_backup_dir_.empty()) {
            use_snapshot_backup_dir_ = true;
        }
    }

    if (enable_snapshot_restore_) {
        RestoreState();
    }
    if (enable_snapshot_ && snapshot_retention_count_ == 0) {
        LOG(ERROR) << "snapshot_retention_count must be greater than 0";
        throw std::invalid_argument("snapshot_retention_count must be > 0");
    }
    if (eviction_ratio_ < 0.0 || eviction_ratio_ > 1.0) {
        LOG(ERROR) << "Eviction ratio must be between 0.0 and 1.0, "
                   << "current value: " << eviction_ratio_;
        throw std::invalid_argument("Invalid eviction ratio");
    }
    if (eviction_high_watermark_ratio_ < 0.0 ||
        eviction_high_watermark_ratio_ > 1.0) {
        LOG(ERROR)
            << "Eviction high watermark ratio must be between 0.0 and 1.0, "
            << "current value: " << eviction_high_watermark_ratio_;
        throw std::invalid_argument("Invalid eviction high watermark ratio");
    }

    if (put_start_release_timeout_sec_ <= put_start_discard_timeout_sec_) {
        LOG(ERROR) << "put_start_release_timeout="
                   << put_start_release_timeout_sec_.count()
                   << " must be larger than put_start_discard_timeout_sec="
                   << put_start_discard_timeout_sec_.count();
        throw std::invalid_argument(
            "put_start_release_timeout must be larger than "
            "put_start_discard_timeout_sec");
    }

    eviction_running_ = true;
    eviction_thread_ = std::thread(&MasterService::EvictionThreadFunc, this);
    VLOG(1) << "action=start_eviction_thread";

    // Start client monitor thread in all modes so TTL/heartbeat works
    client_monitor_running_ = true;
    client_monitor_thread_ =
        std::thread(&MasterService::ClientMonitorFunc, this);
    VLOG(1) << "action=start_client_monitor_thread";

    // Start task cleanup thread
    task_cleanup_running_ = true;
    task_cleanup_thread_ =
        std::thread(&MasterService::TaskCleanupThreadFunc, this);
    VLOG(1) << "action=start_task_cleanup_thread";

    if (!root_fs_dir_.empty()) {
        use_disk_replica_ = true;
        MasterMetricManager::instance().inc_total_file_capacity(
            global_file_segment_size_);
    }

    if (enable_snapshot_) {
        if (memory_allocator_type_ == BufferAllocatorType::OFFSET) {
            snapshot_running_ = true;
            snapshot_thread_ =
                std::thread(&MasterService::SnapshotThreadFunc, this);
        }
    }

    if (enable_cxl_) {
        allocation_strategy_ = std::make_shared<CxlAllocationStrategy>();
        segment_manager_.initializeCxlAllocator(cxl_path_, cxl_size_);
        VLOG(1) << "action=start_cxl_global_allocator";
    }
    // Initialize OpLogStore if HA is enabled.
    // Uses OpLogStoreFactory to decouple from concrete store implementations.
    if (enable_ha_ && !cluster_id_.empty()) {
        // Try to create OpLogStore - if backend is not connected, operations
        // will fail but we can still use memory buffer as fallback.
        auto oplog_store = OpLogStoreFactory::Create(
            oplog_store_type_, cluster_id_, OpLogStoreRole::WRITER,
            oplog_store_root_dir_, oplog_poll_interval_ms_);
        if (!oplog_store) {
            LOG(WARNING) << "OpLogStore creation failed for cluster_id="
                         << cluster_id_
                         << ", OpLog will fall back to memory buffer";
        } else {
            // Fence against restart/promotion regressions: initialize
            // OpLogManager to the maximum existing sequence_id so we
            // don't collide/overwrite.
            uint64_t max_seq = 0;
            if (oplog_store->GetMaxSequenceId(max_seq) == ErrorCode::OK) {
                oplog_manager_.SetInitialSequenceId(max_seq);
            }
            oplog_manager_.SetOpLogStore(
                std::shared_ptr<OpLogStore>(std::move(oplog_store)));
            LOG(INFO) << "OpLogStore initialized for cluster_id="
                      << cluster_id_;
        }
    } else if (enable_ha_) {
        LOG(WARNING) << "HA mode enabled but cluster_id is empty, "
                        "OpLog will only be stored in memory buffer";
    }

    // Start pending durable mutation retry thread (HA only).
#ifdef STORE_USE_ETCD
    if (enable_ha_) {
        pending_mutations_running_.store(true);
        pending_mutations_thread_ =
            std::thread(&MasterService::PendingMutationWorker, this);
    }
#endif
}

// Helper function to append an OpLog entry.
// In the current etcd-based design:
// - OpLogManager always appends to its in-memory buffer
// - If EtcdOpLogStore is configured (HA mode), OpLogManager also writes to etcd
//   synchronously (best-effort; see OpLogManager::Append).
void MasterService::AppendOpLogAndNotify(OpType type, const std::string& key,
                                         const std::string& payload) {
    oplog_manager_.Append(type, key, payload);
}

auto MasterService::AppendOpLogAndNotifyDurable(OpType type,
                                                const std::string& key,
                                                const std::string& payload)
    -> tl::expected<uint64_t, ErrorCode> {
#ifdef STORE_USE_ETCD
    // In HA mode, EtcdOpLogStore should have been configured into OpLogManager.
    // For safety, treat missing store as an error for durable ops.
    // Best-effort synchronous retries to absorb transient etcd blips.
    //
    // IMPORTANT:
    // sequence_id must be allocated ONCE (pre-allocation) and retried with the
    // same OpLogEntry, otherwise multiple attempts would allocate multiple
    // sequence_ids for a single logical operation.
    const OpLogEntry entry = oplog_manager_.AllocateEntry(type, key, payload);
    ErrorCode err = PersistOpLogEntryWithSyncRetries(entry);
    if (err == ErrorCode::OK) {
        return entry.sequence_id;
    }
    return tl::make_unexpected(err);
#else
    (void)type;
    (void)key;
    (void)payload;
    return tl::make_unexpected(ErrorCode::ETCD_OPERATION_ERROR);
#endif
}

void MasterService::RestoreFromStandbySnapshot(
    const std::vector<std::pair<std::string, StandbyObjectMetadata>>& snapshot,
    uint64_t initial_oplog_sequence_id) {
    // 1) Ensure OpLog sequence continues without regression after failover.
    // Prefer reading the true max seq from etcd (stronger than
    // standby_last_seq), fall back to caller-provided
    // initial_oplog_sequence_id.
    uint64_t start_seq = initial_oplog_sequence_id;
    if (enable_ha_ && !cluster_id_.empty()) {
        auto store = OpLogStoreFactory::Create(
            oplog_store_type_, cluster_id_, OpLogStoreRole::READER,
            oplog_store_root_dir_, oplog_poll_interval_ms_);
        if (store) {
            uint64_t max_seq = 0;
            if (store->GetMaxSequenceId(max_seq) == ErrorCode::OK) {
                start_seq = std::max(start_seq, max_seq);
            }
        }
    }
    oplog_manager_.SetInitialSequenceId(start_seq);

    // 2) Restore metadata entries.
    // Keep dummy allocators alive for restored memory replicas. AllocatedBuffer
    // only holds a weak_ptr to allocator, so without this keepalive map the
    // allocator would expire immediately and transport_endpoint_ would be lost.
    standby_allocator_keepalive_.clear();
    auto get_keepalive_allocator = [this](const std::string& transport_endpoint)
        -> std::shared_ptr<BufferAllocatorBase> {
        auto it = standby_allocator_keepalive_.find(transport_endpoint);
        if (it != standby_allocator_keepalive_.end()) {
            return it->second;
        }
        auto alloc = std::make_shared<DummyBufferAllocator>(
            /*segment_name=*/std::string(), transport_endpoint);
        standby_allocator_keepalive_.emplace(transport_endpoint, alloc);
        return alloc;
    };

    const auto now = std::chrono::system_clock::now();
    size_t restored = 0;
    for (const auto& kv : snapshot) {
        const std::string& key = kv.first;
        const StandbyObjectMetadata& sm = kv.second;

        std::vector<Replica> replicas;
        replicas.reserve(sm.replicas.size());
        for (const auto& rd : sm.replicas) {
            if (rd.is_memory_replica()) {
                const auto& bd = rd.get_memory_descriptor().buffer_descriptor;
                replicas.emplace_back(ReplicaFromDescriptor(
                    rd, get_keepalive_allocator(bd.transport_endpoint_)));
            } else {
                replicas.emplace_back(ReplicaFromDescriptor(rd, nullptr));
            }
        }

        // NOTE: Lease information is NOT restored because:
        // 1. Standby does not use lease info (no eviction)
        // 2. New Primary should grant fresh leases after promotion
        // 3. Restoring old lease TTLs could cause immediate eviction if they're
        // expired
        const bool enable_soft_pin =
            false;  // Will be set by new Primary if needed

        const size_t shard_idx = getShardIndex(key);
        MetadataShardAccessorRW shard(this, shard_idx);

        // Overwrite existing key if any.
        shard->metadata.erase(key);
        auto [it, inserted] = shard->metadata.emplace(
            std::piecewise_construct, std::forward_as_tuple(key),
            std::forward_as_tuple(sm.client_id, now,
                                  static_cast<size_t>(sm.size),
                                  std::move(replicas), enable_soft_pin));
        (void)inserted;

        // Lease will be granted by new Primary when objects are accessed
        // (via GetReplicaList, ExistKey, etc.)

        // Objects restored from PUT_END are expected to be completed.
        shard->processing_keys.erase(key);

        restored++;
    }

    LOG(INFO) << "Restored metadata from standby snapshot: restored_keys="
              << restored
              << ", initial_oplog_sequence_id=" << initial_oplog_sequence_id;
}

void MasterService::ExportStandbySnapshot(
    std::vector<std::pair<std::string, StandbyObjectMetadata>>& out,
    uint64_t last_sequence_id, bool include_memory_replicas) const {
    out.clear();
    for (size_t shard_idx = 0; shard_idx < kNumShards; ++shard_idx) {
        MetadataShardAccessorRO shard(this, shard_idx);
        for (const auto& kv : shard->metadata) {
            const std::string& key = kv.first;
            const ObjectMetadata& om = kv.second;

            StandbyObjectMetadata sm;
            sm.client_id = om.client_id;
            sm.size = static_cast<uint64_t>(om.size);
            sm.last_sequence_id = last_sequence_id;

            const auto& reps = om.GetAllReplicas();
            sm.replicas.reserve(reps.size());
            for (const auto& rep : reps) {
                if (!include_memory_replicas && rep.is_memory_replica()) {
                    continue;
                }
                sm.replicas.emplace_back(rep.get_descriptor());
            }

            out.emplace_back(key, std::move(sm));
        }
    }
}

uint64_t MasterService::GetOpLogLastSequenceId() const {
    return oplog_manager_.GetLastSequenceId();
}

bool MasterService::RestoredFromSnapshot() const {
    return restored_from_snapshot_;
}

MasterService::~MasterService() {
    // Stop and join the threads
    eviction_running_ = false;
    client_monitor_running_ = false;
    snapshot_running_ = false;
    task_cleanup_running_ = false;

    // Wake sleepers so join() doesn't block for long sleep intervals.
    task_cleanup_cv_.notify_all();

    if (eviction_thread_.joinable()) {
        eviction_thread_.join();
    }
    if (client_monitor_thread_.joinable()) {
        client_monitor_thread_.join();
    }
    if (snapshot_thread_.joinable()) {
        snapshot_thread_.join();
    }
    if (task_cleanup_thread_.joinable()) {
        task_cleanup_thread_.join();
    }
    // Stop snapshot daemon
    StopSnapshotDaemon();

#ifdef STORE_USE_ETCD
    if (pending_mutations_running_.load()) {
        pending_mutations_running_.store(false);
        pending_mutations_cv_.notify_all();
        if (pending_mutations_thread_.joinable()) {
            pending_mutations_thread_.join();
        }
    }
#endif
}

void MasterService::EnqueuePendingMutation(PendingMutation m) {
    m.attempt = 0;
    m.next_retry_at = std::chrono::steady_clock::now();
    size_t queue_size = 0;
    {
        std::lock_guard<std::mutex> lg(pending_mutations_mutex_);
        if (pending_mutations_.size() >= kMaxPendingMutations) {
            // Queue full: drop oldest mutation to prevent unbounded growth.
            // Log warning for monitoring.
            LOG(WARNING) << "PendingMutation queue full (size="
                         << pending_mutations_.size()
                         << "), dropping oldest mutation. key="
                         << pending_mutations_.front().key << ", seq="
                         << pending_mutations_.front().oplog_entry.sequence_id;
            pending_mutations_.pop_front();
        }
        pending_mutations_.push_back(std::move(m));
        queue_size = pending_mutations_.size();
    }
    HAMetricManager::instance().set_pending_mutation_queue_size(
        static_cast<int64_t>(queue_size));
    pending_mutations_cv_.notify_one();
}

ErrorCode MasterService::PersistOpLogEntryWithSyncRetries(
    const OpLogEntry& entry) const {
#ifdef STORE_USE_ETCD
    static constexpr int kSyncRetries = 3;
    static constexpr int kBaseBackoffMs = 20;
    ErrorCode persist_err = ErrorCode::ETCD_OPERATION_ERROR;

    auto start_time = std::chrono::steady_clock::now();

    for (int attempt = 0; attempt < kSyncRetries; ++attempt) {
        persist_err = oplog_manager_.PersistEntry(entry);
        if (persist_err == ErrorCode::OK) {
            break;
        }
        if (attempt > 0) {
            HAMetricManager::instance().inc_oplog_etcd_write_retries();
        }
        std::this_thread::sleep_for(
            std::chrono::milliseconds(kBaseBackoffMs * (1 << attempt)));
    }

    auto end_time = std::chrono::steady_clock::now();
    auto latency_us = std::chrono::duration_cast<std::chrono::microseconds>(
                          end_time - start_time)
                          .count();
    HAMetricManager::instance().observe_oplog_etcd_write_latency_us(latency_us);

    if (persist_err == ErrorCode::OK) {
        HAMetricManager::instance().set_oplog_last_sequence_id(
            static_cast<int64_t>(entry.sequence_id));
    } else {
        HAMetricManager::instance().inc_oplog_etcd_write_failures();
    }

    return persist_err;
#else
    (void)entry;
    return ErrorCode::ETCD_OPERATION_ERROR;
#endif
}

void MasterService::EnqueueRetryOnPersistFailure(
    const char* ctx, const OpLogEntry& entry, ErrorCode persist_err,
    PendingMutationKind kind, const std::string& segment_name) {
#ifdef STORE_USE_ETCD
    LOG(ERROR) << ctx
               << ": failed to persist OpLog to etcd, key=" << entry.object_key
               << ", seq=" << entry.sequence_id << ", err=" << persist_err
               << ". Enqueue retry.";
    EnqueuePendingMutation(PendingMutation{kind, entry.object_key, segment_name,
                                           /*oplog_entry=*/entry});
#else
    (void)ctx;
    (void)entry;
    (void)persist_err;
    (void)kind;
    (void)segment_name;
#endif
}

void MasterService::AppendOrPersistOrEnqueue(const char* ctx, OpType type,
                                             const std::string& key,
                                             const std::string& payload,
                                             PendingMutationKind kind,
                                             const std::string& segment_name) {
#ifdef STORE_USE_ETCD
    if (enable_ha_) {
        const OpLogEntry entry =
            oplog_manager_.AllocateEntry(type, key, payload);
        ErrorCode persist_err = PersistOpLogEntryWithSyncRetries(entry);
        if (persist_err != ErrorCode::OK) {
            EnqueueRetryOnPersistFailure(ctx, entry, persist_err, kind,
                                         segment_name);
        }
    } else {
        AppendOpLogAndNotify(type, key, payload);
    }
#else
    // No etcd support at compile time:
    // - non-HA: keep best-effort in-memory OpLog for debugging/consistency
    // - HA: no-op (constructor already warns)
    if (!enable_ha_) {
        AppendOpLogAndNotify(type, key, payload);
    }
    (void)ctx;
    (void)kind;
    (void)segment_name;
#endif
}

void MasterService::AppendOrPersistOrEnqueueLazy(
    const char* ctx, OpType type, const std::string& key,
    const std::function<std::string()>& payload_factory,
    PendingMutationKind kind, const std::string& segment_name) {
    std::string payload;
    bool payload_ready = false;
    auto get_payload = [&]() -> const std::string& {
        if (!payload_ready) {
            payload = payload_factory ? payload_factory() : std::string();
            payload_ready = true;
        }
        return payload;
    };

#ifdef STORE_USE_ETCD
    if (enable_ha_) {
        const OpLogEntry entry =
            oplog_manager_.AllocateEntry(type, key, get_payload());
        ErrorCode persist_err = PersistOpLogEntryWithSyncRetries(entry);
        if (persist_err != ErrorCode::OK) {
            EnqueueRetryOnPersistFailure(ctx, entry, persist_err, kind,
                                         segment_name);
        }
    } else {
        AppendOpLogAndNotify(type, key, get_payload());
    }
#else
    // No etcd support at compile time:
    // - non-HA: keep best-effort in-memory OpLog for debugging/consistency
    // - HA: no-op (constructor already warns)
    if (!enable_ha_) {
        AppendOpLogAndNotify(type, key, get_payload());
    }
    (void)ctx;
    (void)kind;
    (void)segment_name;
#endif
}

// Return true if processed successfully (done), false if should retry later.
bool MasterService::ProcessPendingMutationOnce(PendingMutation& m) {
#ifndef STORE_USE_ETCD
    (void)m;
    return true;
#else
    const auto now = std::chrono::steady_clock::now();
    if (m.next_retry_at > now) {
        return false;
    }

    // Retrier responsibility:
    // only persist the original pre-allocated OpLogEntry (fixed sequence_id) to
    // etcd. Do NOT mutate local metadata here because the caller may have
    // already moved on.
    if (m.oplog_entry.sequence_id == 0) {
        LOG(WARNING)
            << "PendingMutation has no pre-allocated OpLogEntry, drop. key="
            << m.key << ", kind=" << static_cast<int>(m.kind);
        return true;
    }

    ErrorCode err = oplog_manager_.PersistEntry(m.oplog_entry);
    if (err != ErrorCode::OK) {
        return false;
    }
    return true;
#endif
}

void MasterService::PendingMutationWorker() {
#ifndef STORE_USE_ETCD
    return;
#else
    while (pending_mutations_running_.load()) {
        PendingMutation m;
        bool has_item = false;
        {
            std::unique_lock<std::mutex> lk(pending_mutations_mutex_);
            pending_mutations_cv_.wait_for(
                lk, std::chrono::milliseconds(200), [&] {
                    return !pending_mutations_running_.load() ||
                           !pending_mutations_.empty();
                });
            if (!pending_mutations_running_.load()) {
                break;
            }
            if (pending_mutations_.empty()) {
                continue;
            }
            m = std::move(pending_mutations_.front());
            pending_mutations_.pop_front();
            has_item = true;
        }
        if (!has_item) {
            continue;
        }

        const bool done = ProcessPendingMutationOnce(m);
        if (done) {
            continue;
        }

        // Retry with exponential backoff (cap at 30s).
        m.attempt++;
        const uint32_t exp = std::min<uint32_t>(m.attempt, 8);
        const auto delay = std::chrono::milliseconds(200u * (1u << exp));
        const auto capped = std::min(delay, std::chrono::milliseconds(30000));
        m.next_retry_at = std::chrono::steady_clock::now() + capped;

        {
            std::lock_guard<std::mutex> lg(pending_mutations_mutex_);
            pending_mutations_.push_back(std::move(m));
        }
        pending_mutations_cv_.notify_one();
    }
#endif
}

auto MasterService::MountSegment(const Segment& segment, const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    // Tell the client monitor thread to start timing for this client. To
    // avoid the following undesired situations, this message must be sent
    // after locking the segment mutex and before the mounting operation
    // completes:
    // 1. Sending the message before the lock: the client expires and
    // unmouting invokes before this mounting are completed, which prevents
    // this segment being able to be unmounted forever;
    // 2. Sending the message after mounting the segment: After mounting
    // this segment, when trying to push id to the queue, the queue is
    // already full. However, at this point, the message must be sent,
    // otherwise this client cannot be monitored and expired.
    {
        PodUUID pod_client_id;
        pod_client_id.first = client_id.first;
        pod_client_id.second = client_id.second;
        if (!client_ping_queue_.push(pod_client_id)) {
            LOG(ERROR) << "segment_name=" << segment.name
                       << ", error=client_ping_queue_full";
            return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
        }
    }

    LOG(INFO) << "client_id=" << client_id
              << ", action=mount_segment, segment_name=" << segment.name;

    auto err = segment_access.MountSegment(segment, client_id);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

auto MasterService::ReMountSegment(const std::vector<Segment>& segments,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    std::unique_lock<std::shared_mutex> lock(client_mutex_);
    if (ok_client_.contains(client_id)) {
        LOG(WARNING) << "client_id=" << client_id
                     << ", warn=client_already_remounted";
        // Return OK because this is an idempotent operation
        return {};
    }

    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    // Tell the client monitor thread to start timing for this client. To
    // avoid the following undesired situations, this message must be sent
    // after locking the segment mutex or client mutex and before the remounting
    // operation completes:
    // 1. Sending the message before the lock: the client expires and
    // unmouting invokes before this remounting are completed, which prevents
    // this segment being able to be unmounted forever;
    // 2. Sending the message after remounting the segments: After remounting
    // these segments, when trying to push id to the queue, the queue is
    // already full. However, at this point, the message must be sent,
    // otherwise this client cannot be monitored and expired.
    PodUUID pod_client_id;
    pod_client_id.first = client_id.first;
    pod_client_id.second = client_id.second;
    if (!client_ping_queue_.push(pod_client_id)) {
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=client_ping_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    ErrorCode err = segment_access.ReMountSegment(segments, client_id);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }

    // Change the client status to OK
    ok_client_.insert(client_id);
    MasterMetricManager::instance().inc_active_clients();

    return {};
}

void MasterService::ClearInvalidHandles() {
    for (auto& shard : metadata_shards_) {
        SharedMutexLocker lock(&shard.mutex);
        auto it = shard.metadata.begin();
        while (it != shard.metadata.end()) {
            // CleanupStaleHandles may remove MEMORY replicas whose allocator
            // has become invalid (segment unmounted). If key remains valid (has
            // disk replicas), Standby must receive an updated metadata payload
            // that excludes those MEMORY replicas (Scheme A).
            if (CleanupStaleHandles(it->second)) {
                // No replicas remain after cleanup -> key should be deleted.
                // Also erase from processing_keys, replication_tasks, and offloading_tasks.
#ifdef STORE_USE_ETCD
                if (enable_ha_) {
                    AppendOrPersistOrEnqueue(
                        "ClearInvalidHandles(REMOVE)", OpType::REMOVE,
                        it->first, std::string(),
                        PendingMutationKind::EVICT_MEM_REPLICAS);
                } else {
                    AppendOpLogAndNotify(OpType::REMOVE, it->first);
                }
#else
                if (!enable_ha_) {
                    AppendOpLogAndNotify(OpType::REMOVE, it->first);
                }
#endif
                shard.processing_keys.erase(it->first);
                shard.replication_tasks.erase(it->first);
                shard.offloading_tasks.erase(it->first);
                it = shard.metadata.erase(it);
            } else {
                // Still has some replicas. If HA is enabled, publish updated
                // metadata WITHOUT MEMORY replicas (safe superset update).
#ifdef STORE_USE_ETCD
                if (enable_ha_) {
                    AppendOrPersistOrEnqueueLazy(
                        "ClearInvalidHandles(PUT_END)", OpType::PUT_END,
                        it->first,
                        [&]() {
                            return SerializeMetadataForOpLogWithoutMemReplicas(
                                it->second);
                        },
                        PendingMutationKind::EVICT_MEM_REPLICAS);
                }
#endif
                ++it;
            }
        }
    }
}

void MasterService::TaskCleanupThreadFunc() {
    LOG(INFO) << "Task cleanup thread started";
    while (task_cleanup_running_) {
        // Wait for the next cleanup interval, but allow fast shutdown.
        {
            std::unique_lock<std::mutex> lk(task_cleanup_mutex_);
            task_cleanup_cv_.wait_for(
                lk, std::chrono::milliseconds(kTaskCleanupThreadSleepMs),
                [&] { return !task_cleanup_running_.load(); });
        }

        if (!task_cleanup_running_) {
            break;
        }

        std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
        auto write_access = task_manager_.get_write_access();
        write_access.prune_expired_tasks();
        write_access.prune_finished_tasks();
    }
    LOG(INFO) << "Task cleanup thread stopped";
}

auto MasterService::UnmountSegment(const UUID& segment_id,
                                   const UUID& client_id)
    -> tl::expected<void, ErrorCode> {
    size_t metrics_dec_capacity = 0;  // to update the metrics

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    // 1. Prepare to unmount the segment by deleting its allocator
    {
        ScopedSegmentAccess segment_access =
            segment_manager_.getSegmentAccess();
        ErrorCode err = segment_access.PrepareUnmountSegment(
            segment_id, metrics_dec_capacity);
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            // Return OK because this is an idempotent operation
            return {};
        }
        if (err != ErrorCode::OK) {
            return tl::make_unexpected(err);
        }
    }  // Release the segment mutex before long-running step 2 and avoid
       // deadlocks

    // 2. Remove the metadata of the related objects
    ClearInvalidHandles();

    // 3. Commit the unmount operation
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    auto err = segment_access.CommitUnmountSegment(segment_id, client_id,
                                                   metrics_dec_capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

auto MasterService::ExistKey(const std::string& key)
    -> tl::expected<bool, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRO accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return false;
    }

    auto& metadata = accessor.Get();
    if (metadata.HasReplica(&Replica::fn_is_completed)) {
        // Grant a lease to the object as it may be further used by the
        // client.
        metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);
        // Note: LEASE_RENEW is not recorded in OpLog since Standby does not
        // perform eviction. Standby will receive DELETE events from Primary
        // when objects are evicted.
        return true;
    }

    return false;
}

std::vector<tl::expected<bool, ErrorCode>> MasterService::BatchExistKey(
    const std::vector<std::string>& keys) {
    std::vector<tl::expected<bool, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(ExistKey(key));
    }
    return results;
}

auto MasterService::GetAllKeys()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::vector<std::string> all_keys;
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRO shard(this, i);
        for (const auto& item : shard->metadata) {
            all_keys.push_back(item.first);
        }
    }
    return all_keys;
}

auto MasterService::GetAllSegments()
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<std::string> all_segments;
    auto err = segment_access.GetAllSegments(all_segments);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return all_segments;
}

auto MasterService::QuerySegments(const std::string& segment)
    -> tl::expected<std::pair<size_t, size_t>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    size_t used, capacity;
    auto err = segment_access.QuerySegments(segment, used, capacity);
    if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return std::make_pair(used, capacity);
}

auto MasterService::QueryIp(const UUID& client_id)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();
    std::vector<Segment> segments;
    ErrorCode err = segment_access.GetClientSegments(client_id, segments);
    if (err != ErrorCode::OK) {
        if (err == ErrorCode::SEGMENT_NOT_FOUND) {
            VLOG(1) << "QueryIp: client_id=" << client_id
                    << " not found or has no segments";
            return tl::make_unexpected(ErrorCode::CLIENT_NOT_FOUND);
        }

        LOG(ERROR) << "QueryIp: failed to get segments for client_id="
                   << client_id << ", error=" << toString(err);

        return tl::make_unexpected(err);
    }

    std::unordered_set<std::string> unique_ips;
    unique_ips.reserve(segments.size());
    for (const auto& segment : segments) {
        if (!segment.te_endpoint.empty()) {
            size_t colon_pos = segment.te_endpoint.find(':');
            if (colon_pos != std::string::npos) {
                std::string ip = segment.te_endpoint.substr(0, colon_pos);
                unique_ips.emplace(ip);
            } else {
                unique_ips.emplace(segment.te_endpoint);
            }
        }
    }

    if (unique_ips.empty()) {
        LOG(WARNING) << "QueryIp: client_id=" << client_id
                     << " has no valid IP addresses";
        return {};
    }
    std::vector<std::string> result(unique_ips.begin(), unique_ips.end());
    return result;
}

auto MasterService::BatchQueryIp(const std::vector<UUID>& client_ids)
    -> tl::expected<
        std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>,
        ErrorCode> {
    std::unordered_map<UUID, std::vector<std::string>, boost::hash<UUID>>
        results;
    results.reserve(client_ids.size());
    for (const auto& client_id : client_ids) {
        auto ip_result = QueryIp(client_id);
        if (ip_result.has_value()) {
            results.emplace(client_id, std::move(ip_result.value()));
        }
    }
    return results;
}

auto MasterService::BatchReplicaClear(
    const std::vector<std::string>& object_keys, const UUID& client_id,
    const std::string& segment_name)
    -> tl::expected<std::vector<std::string>, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    std::vector<std::string> cleared_keys;
    cleared_keys.reserve(object_keys.size());
    const bool clear_all_segments = segment_name.empty();

    for (const auto& key : object_keys) {
        if (key.empty()) {
            LOG(WARNING) << "BatchReplicaClear: empty key, skipping";
            continue;
        }
        MetadataAccessorRW accessor(this, key);
        if (!accessor.Exists()) {
            LOG(WARNING) << "BatchReplicaClear: key=" << key
                         << " not found, skipping";
            continue;
        }

        auto& metadata = accessor.Get();

        // Security check: Ensure the requesting client owns the object.
        if (metadata.client_id != client_id) {
            LOG(WARNING) << "BatchReplicaClear: key=" << key
                         << " belongs to different client_id="
                         << metadata.client_id << ", expected=" << client_id
                         << ", skipping";
            continue;
        }

        // Safety check: Do not clear an object that has an active lease.
        if (!metadata.IsLeaseExpired()) {
            LOG(WARNING) << "BatchReplicaClear: key=" << key
                         << " has active lease, skipping";
            continue;
        }

        if (clear_all_segments) {
            // Check if all replicas are complete. Incomplete replicas could
            // indicate an ongoing Put operation, and clearing during this time
            // could lead to an inconsistent state or interfere with the write.
            if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
                LOG(WARNING) << "BatchReplicaClear: key=" << key
                             << " has incomplete replicas, skipping";
                continue;
            }

            metadata.VisitReplicas(
                &Replica::fn_is_completed, [](Replica& replica) {
                    if (replica.is_memory_replica()) {
                        MasterMetricManager::instance().dec_mem_cache_nums();
                    } else if (replica.is_disk_replica()) {
                        MasterMetricManager::instance().dec_file_cache_nums();
                    }
                });
            // HA safety (Scheme A):
            // This operation may free/reuse MEMORY replicas. Persist REMOVE to
            // etcd BEFORE actually erasing local metadata.
#ifdef STORE_USE_ETCD
            if (enable_ha_) {
                AppendOrPersistOrEnqueue(
                    "BatchReplicaClear(all)", OpType::REMOVE, key,
                    std::string(), PendingMutationKind::CLEAR_ALL_REPLICAS);
            } else {
                AppendOpLogAndNotify(OpType::REMOVE, key);
            }
#else
            if (!enable_ha_) {
                AppendOpLogAndNotify(OpType::REMOVE, key);
            }
#endif

            // Erase the entire metadata (all replicas will be deallocated)
            accessor.Erase();
            cleared_keys.emplace_back(key);
            VLOG(1) << "BatchReplicaClear: successfully cleared all replicas "
                       "for key="
                    << key << " for client_id=" << client_id;
        } else {
            // Clear only replicas on the specified segment_name
            const auto match_replica_on_segment =
                [&](const Replica& replica) -> bool {
                if (!replica.is_completed()) {
                    return false;
                }
                const auto segment_names = replica.get_segment_names();
                for (const auto& seg_name : segment_names) {
                    if (seg_name.has_value() &&
                        seg_name.value() == segment_name) {
                        return true;
                    }
                }
                return false;
            };

            const bool has_replica_on_segment =
                metadata.HasReplica(match_replica_on_segment);

            if (!has_replica_on_segment) {
                LOG(WARNING)
                    << "BatchReplicaClear: key=" << key
                    << " has no replica on segment_name=" << segment_name
                    << ", skipping";
                continue;
            }

            // HA safety (Scheme A):
            // Removing replicas may free/reuse MEMORY replicas. Persist updated
            // metadata BEFORE mutating local replicas (which may free memory).
#ifdef STORE_USE_ETCD
            if (enable_ha_) {
                // Build the remaining replica descriptor list after removal.
                std::vector<Replica::Descriptor> remaining;
                remaining.reserve(metadata.CountReplicas());
                metadata.VisitReplicas(
                    [](const Replica&) { return true; },
                    [&](const Replica& replica) {
                        if (match_replica_on_segment(replica)) {
                            return;
                        }
                        remaining.emplace_back(replica.get_descriptor());
                    });

                if (remaining.empty()) {
                    AppendOrPersistOrEnqueue(
                        "BatchReplicaClear(partial REMOVE)", OpType::REMOVE,
                        key, std::string(),
                        PendingMutationKind::CLEAR_REPLICAS_ON_SEGMENT,
                        segment_name);
                } else {
                    const std::string payload =
                        SerializeMetadataForOpLogFromReplicaDescriptors(
                            metadata.client_id,
                            static_cast<uint64_t>(metadata.size), remaining);
                    AppendOrPersistOrEnqueue(
                        "BatchReplicaClear(partial PUT_END)", OpType::PUT_END,
                        key, payload,
                        PendingMutationKind::CLEAR_REPLICAS_ON_SEGMENT,
                        segment_name);
                }
            }
#endif

            // Apply local mutation and metrics (after HA persistence attempt).
            metadata.VisitReplicas(
                match_replica_on_segment, [](Replica& replica) {
                    if (replica.is_memory_replica()) {
                        MasterMetricManager::instance().dec_mem_cache_nums();
                    } else if (replica.is_disk_replica()) {
                        MasterMetricManager::instance().dec_file_cache_nums();
                    }
                });

            metadata.EraseReplicas(match_replica_on_segment);

            // If no valid replicas remain, erase the entire metadata
            if (!metadata.IsValid()) {
#ifndef STORE_USE_ETCD
                // Non-HA: keep old behavior; HA already persisted REMOVE above.
                if (!enable_ha_) {
                    AppendOpLogAndNotify(OpType::REMOVE, key);
                }
#endif
                accessor.Erase();
            } else {
#ifndef STORE_USE_ETCD
                // Non-HA: best-effort update to keep future behavior
                // consistent.
                if (!enable_ha_) {
                    const std::string payload =
                        SerializeMetadataForOpLog(metadata);
                    AppendOpLogAndNotify(OpType::PUT_END, key, payload);
                }
#endif
            }

            cleared_keys.emplace_back(key);
            VLOG(1) << "BatchReplicaClear: successfully cleared replicas on "
                       "segment_name="
                    << segment_name << " for key=" << key
                    << " for client_id=" << client_id;
        }
    }

    return cleared_keys;
}

auto MasterService::GetReplicaListByRegex(const std::string& regex_pattern)
    -> tl::expected<
        std::unordered_map<std::string, std::vector<Replica::Descriptor>>,
        ErrorCode> {
    std::unordered_map<std::string, std::vector<Replica::Descriptor>> results;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRO shard(this, i);

        for (const auto& [key, metadata] : shard->metadata) {
            if (std::regex_search(key, pattern)) {
                std::vector<Replica::Descriptor> replica_list;
                metadata.VisitReplicas(
                    &Replica::fn_is_completed,
                    [&replica_list](const Replica& replica) {
                        replica_list.emplace_back(replica.get_descriptor());
                    });

                if (replica_list.empty()) {
                    LOG(WARNING)
                        << "key=" << key
                        << " matched by regex, but has no complete replicas.";
                    continue;
                }

                results.emplace(key, std::move(replica_list));
                metadata.GrantLease(default_kv_lease_ttl_,
                                    default_kv_soft_pin_ttl_);
                // Note: LEASE_RENEW is not recorded in OpLog since Standby does
                // not perform eviction. Standby will receive DELETE events from
                // Primary when objects are evicted.
            }
        }
    }

    return results;
}

auto MasterService::GetReplicaList(const std::string& key)
    -> tl::expected<GetReplicaListResponse, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRO accessor(this, key);

    MasterMetricManager::instance().inc_total_get_nums();

    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }
    const auto& metadata = accessor.Get();

    std::vector<Replica::Descriptor> replica_list;
    metadata.VisitReplicas(
        &Replica::fn_is_completed, [&replica_list](const Replica& replica) {
            replica_list.emplace_back(replica.get_descriptor());
        });

    if (replica_list.empty()) {
        LOG(WARNING) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }

    if (replica_list[0].is_memory_replica()) {
        MasterMetricManager::instance().inc_mem_cache_hit_nums();
    } else if (replica_list[0].is_disk_replica()) {
        MasterMetricManager::instance().inc_file_cache_hit_nums();
    }
    MasterMetricManager::instance().inc_valid_get_nums();
    // Grant a lease to the object so it will not be removed
    // when the client is reading it.
    metadata.GrantLease(default_kv_lease_ttl_, default_kv_soft_pin_ttl_);
    // Note: LEASE_RENEW is not recorded in OpLog since Standby does not
    // perform eviction. Standby will receive DELETE events from Primary
    // when objects are evicted.

    return GetReplicaListResponse(std::move(replica_list),
                                  default_kv_lease_ttl_);
}

auto MasterService::PutStart(const UUID& client_id, const std::string& key,
                             const uint64_t slice_length,
                             const ReplicateConfig& config)
    -> tl::expected<std::vector<Replica::Descriptor>, ErrorCode> {
    if (config.replica_num == 0 || key.empty() || slice_length == 0) {
        LOG(ERROR) << "key=" << key << ", replica_num=" << config.replica_num
                   << ", slice_length=" << slice_length
                   << ", key_size=" << key.size() << ", error=invalid_params";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    // Validate slice lengths
    uint64_t total_length = 0;
    if ((memory_allocator_type_ == BufferAllocatorType::CACHELIB) &&
        (slice_length > kMaxSliceSize)) {
        LOG(ERROR) << "key=" << key << ", slice_length=" << slice_length
                   << ", max_size=" << kMaxSliceSize
                   << ", error=invalid_slice_size";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    total_length += slice_length;

    VLOG(1) << "key=" << key << ", value_length=" << total_length
            << ", slice_length=" << slice_length << ", config=" << config
            << ", action=put_start_begin";

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    // Lock the shard and check if object already exists
    MetadataShardAccessorRW shard(this, getShardIndex(key));

    const auto now = std::chrono::system_clock::now();
    auto it = shard->metadata.find(key);
    if (it != shard->metadata.end() && !CleanupStaleHandles(it->second)) {
        auto& metadata = it->second;

        // Safety: do not overwrite keys that have an ongoing replication task.
        if (shard->replication_tasks.find(key) !=
            shard->replication_tasks.end()) {
            LOG(WARNING) << "key=" << key
                         << ", error=object_has_replication_task";
            return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
        }
        // If the object's PutStart expired and has not completed any
        // replicas, we can discard it and allow the new PutStart to
        // go.
        if (!metadata.HasReplica(&Replica::fn_is_completed) &&
            metadata.put_start_time + put_start_discard_timeout_sec_ < now) {
            auto replicas = metadata.PopReplicas(&Replica::fn_is_processing);
            if (!replicas.empty()) {
                std::lock_guard lock(discarded_replicas_mutex_);
                discarded_replicas_.emplace_back(
                    std::move(replicas),
                    metadata.put_start_time + put_start_release_timeout_sec_);
            }
            shard->processing_keys.erase(key);
            shard->metadata.erase(it);
        } else if (metadata.HasReplica(&Replica::fn_is_completed)) {
            // Allow overwriting existing key even if lease is still active.
            // This enables update operations. Note that overwriting an object
            // with an active lease may cause clients reading it to see stale
            // data, but this is acceptable for update operations.
            if (!metadata.IsLeaseExpired()) {
                // Lease is still active, log a warning but allow overwrite
                auto remaining_lease_sec =
                    std::chrono::duration_cast<std::chrono::seconds>(
                        metadata.lease_timeout - now)
                        .count();
                LOG(WARNING)
                    << "key=" << key
                    << ", info=overwriting_object_with_active_lease"
                    << ", remaining_lease_sec=" << remaining_lease_sec
                    << " (clients reading this key may see stale data)";
            } else {
                // Lease has expired, this is normal
                LOG(INFO) << "key=" << key
                          << ", info=overwriting_expired_object"
                          << ", lease_expired_at="
                          << std::chrono::duration_cast<std::chrono::seconds>(
                                 metadata.lease_timeout.time_since_epoch())
                                 .count();
            }
            // Remove the old object to allow new PutStart
#ifdef STORE_USE_ETCD
            if (enable_ha_) {
                // Scheme A safety: overwriting may free/reuse MEMORY replicas.
                // Persist REMOVE to etcd BEFORE erasing local metadata.
                AppendOrPersistOrEnqueue(
                    "PutStart(overwrite REMOVE)", OpType::REMOVE, key,
                    std::string(), PendingMutationKind::CLEAR_ALL_REPLICAS);
            }
#endif
            shard->processing_keys.erase(key);
            shard->metadata.erase(it);
        } else {
            // Object exists but has not completed replicas and PutStart has not
            // expired
            LOG(INFO) << "key=" << key << ", info=object_already_exists"
                      << ", has_completed_replicas=false"
                      << ", put_start_time="
                      << std::chrono::duration_cast<std::chrono::seconds>(
                             metadata.put_start_time.time_since_epoch())
                             .count();
            return tl::make_unexpected(ErrorCode::OBJECT_ALREADY_EXISTS);
        }
    }

    // Allocate replicas
    std::vector<Replica> replicas;
    {
        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        std::vector<std::string> preferred_segments;
        if (!config.preferred_segment.empty()) {
            preferred_segments.push_back(config.preferred_segment);
        } else if (!config.preferred_segments.empty()) {
            preferred_segments = config.preferred_segments;
        }

        auto allocation_result = allocation_strategy_->Allocate(
            allocator_manager, slice_length, config.replica_num,
            preferred_segments);

        if (!allocation_result.has_value()) {
            VLOG(1) << "Failed to allocate all replicas for key=" << key
                    << ", error: " << allocation_result.error();
            if (allocation_result.error() == ErrorCode::INVALID_PARAMS) {
                return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
            }
            need_eviction_ = true;
            return tl::make_unexpected(ErrorCode::NO_AVAILABLE_HANDLE);
        }

        replicas = std::move(allocation_result.value());
    }

    // If disk replica is enabled, allocate a disk replica
    if (use_disk_replica_) {
        // Allocate a file path for the disk replica
        std::string file_path =
            ResolvePathFromKey(key, root_fs_dir_, cluster_id_);
        replicas.emplace_back(file_path, total_length,
                              ReplicaStatus::PROCESSING);
    }

    std::vector<Replica::Descriptor> replica_list;
    replica_list.reserve(replicas.size());
    for (const auto& replica : replicas) {
        replica_list.emplace_back(replica.get_descriptor());
    }

    // No need to set lease here. The object will not be evicted until
    // PutEnd is called.
    shard->metadata.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, now, total_length, std::move(replicas),
                              config.with_soft_pin));
    // Also insert the metadata into processing set for monitoring.
    shard->processing_keys.insert(key);

    return replica_list;
}

auto MasterService::PutEnd(const UUID& client_id, const std::string& key,
                           ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (client_id != metadata.client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to PutEnd key " << key
                   << ", was PutStart-ed by " << metadata.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    metadata.VisitReplicas(
        [replica_type](const Replica& replica) {
            return replica.type() == replica_type;
        },
        [](Replica& replica) { replica.mark_complete(); });

    if (enable_offload_) {
        auto& shard = accessor.GetShard();
        metadata.VisitReplicas(
            &Replica::fn_is_completed, [this, &key, &shard](Replica& replica) {
                auto result = PushOffloadingQueue(key, replica);
                if (result) {
                    replica.inc_refcnt();
                    shard->offloading_tasks.emplace(
                        key, OffloadingTask{replica.id(),
                                            std::chrono::system_clock::now()});
                }
            });
    }

    // If the object is completed, remove it from the processing set.
    if (metadata.AllReplicas(&Replica::fn_is_completed) &&
        accessor.InProcessing()) {
        accessor.EraseFromProcessing();
    }

    if (replica_type == ReplicaType::MEMORY) {
        MasterMetricManager::instance().inc_mem_cache_nums();
    } else if (replica_type == ReplicaType::DISK) {
        MasterMetricManager::instance().inc_file_cache_nums();
    }
    // 1. Set lease timeout to now, indicating that the object has no lease
    // at beginning. 2. If this object has soft pin enabled, set it to be soft
    // pinned.
    metadata.GrantLease(0, default_kv_soft_pin_ttl_);

    // Record OpLog entry for PUT_END so that standbys can replay this change.
    // Serialize metadata (replicas, size, lease) to payload so Standby can
    // restore complete metadata when promoted to Primary.
    std::string metadata_payload = SerializeMetadataForOpLog(metadata);
#ifdef STORE_USE_ETCD
    if (enable_ha_) {
        AppendOrPersistOrEnqueue("PutEnd", OpType::PUT_END, key,
                                 metadata_payload,
                                 PendingMutationKind::EVICT_MEM_REPLICAS);
    } else {
        AppendOpLogAndNotify(OpType::PUT_END, key, metadata_payload);
    }
#else
    AppendOpLogAndNotify(OpType::PUT_END, key, metadata_payload);
#endif

    return {};
}

auto MasterService::AddReplica(const UUID& client_id, const std::string& key,
                               Replica& replica)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        accessor.Create(
            client_id,
            replica.get_descriptor().get_local_disk_descriptor().object_size,
            std::vector<Replica>{}, false);
    }
    auto& metadata = accessor.Get();
    if (replica.type() != ReplicaType::LOCAL_DISK) {
        LOG(ERROR) << "Invalid replica type: " << replica.type()
                   << ". Expected ReplicaType::LOCAL_DISK.";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (!metadata.HasReplica(&Replica::fn_is_local_disk_replica)) {
        std::vector<Replica> replicas;
        replicas.emplace_back(std::move(replica));
        metadata.AddReplicas(std::move(replicas));
        return {};
    }

    metadata.VisitReplicas(
        [client_id](const Replica& rep) {
            return rep.type() == ReplicaType::LOCAL_DISK &&
                   rep.get_descriptor().get_local_disk_descriptor().client_id ==
                       client_id;
        },
        [&replica](Replica& rep) {
            rep.get_descriptor()
                .get_local_disk_descriptor()
                .transport_endpoint = replica.get_descriptor()
                                          .get_local_disk_descriptor()
                                          .transport_endpoint;
            rep.get_descriptor().get_local_disk_descriptor().object_size =
                replica.get_descriptor()
                    .get_local_disk_descriptor()
                    .object_size;
        });
    return {};
}

auto MasterService::PutRevoke(const UUID& client_id, const std::string& key,
                              ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();
    if (client_id != metadata.client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to PutRevoke key "
                   << key << ", was PutStart-ed by " << metadata.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    auto processing_rep =
        metadata.GetFirstReplica([replica_type](const Replica& replica) {
            return replica.type() == replica_type && !replica.is_processing();
        });
    if (processing_rep != nullptr) {
        LOG(ERROR) << "key=" << key << ", status=" << processing_rep->status()
                   << ", error=invalid_replica_status";
        return tl::make_unexpected(ErrorCode::INVALID_WRITE);
    }

    // HA behavior:
    // Do NOT block subsequent ops for the same key if etcd write fails.
    // We allocate sequence_id once and retry persisting this OpLogEntry
    // asynchronously if needed.
#ifdef STORE_USE_ETCD
    if (enable_ha_) {
        AppendOrPersistOrEnqueue("PutRevoke", OpType::PUT_REVOKE, key,
                                 std::string(),
                                 PendingMutationKind::EVICT_MEM_REPLICAS);
    } else {
        AppendOpLogAndNotify(OpType::PUT_REVOKE, key);
    }
#else
    if (!enable_ha_) {
        AppendOpLogAndNotify(OpType::PUT_REVOKE, key);
    }
#endif

    if (replica_type == ReplicaType::MEMORY) {
        MasterMetricManager::instance().dec_mem_cache_nums();
    } else if (replica_type == ReplicaType::DISK) {
        MasterMetricManager::instance().dec_file_cache_nums();
    }

    metadata.EraseReplicas([replica_type](const Replica& replica) {
        return replica.type() == replica_type;
    });

    // If the object is completed, remove it from the processing set.
    if (metadata.AllReplicas(&Replica::fn_is_completed) &&
        accessor.InProcessing()) {
        accessor.EraseFromProcessing();
    }

    if (metadata.IsValid() == false) {
        accessor.Erase();
    }

    return {};
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutEnd(
    const UUID& client_id, const std::vector<std::string>& keys) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(PutEnd(client_id, key, ReplicaType::MEMORY));
    }
    return results;
}

std::vector<tl::expected<void, ErrorCode>> MasterService::BatchPutRevoke(
    const UUID& client_id, const std::vector<std::string>& keys) {
    std::vector<tl::expected<void, ErrorCode>> results;
    results.reserve(keys.size());
    for (const auto& key : keys) {
        results.emplace_back(PutRevoke(client_id, key, ReplicaType::MEMORY));
    }
    return results;
}

auto MasterService::EvictDiskReplica(const UUID& client_id,
                                     const std::string& key,
                                     ReplicaType replica_type)
    -> tl::expected<void, ErrorCode> {
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(INFO) << "key=" << key << ", info=object_not_found_for_eviction";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();

    if (replica_type == ReplicaType::DISK) {
        metadata.EraseReplicas(
            [](const Replica& replica) { return replica.is_disk_replica(); });
        MasterMetricManager::instance().dec_file_cache_nums();
    } else if (replica_type == ReplicaType::LOCAL_DISK) {
        metadata.EraseReplicas([&client_id](const Replica& replica) {
            return replica.is_local_disk_replica() &&
                   replica.get_descriptor()
                           .get_local_disk_descriptor()
                           .client_id == client_id;
        });
    } else {
        LOG(ERROR) << "key=" << key
                   << ", error=invalid_replica_type_for_eviction";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    if (!metadata.IsValid()) {
        accessor.Erase();
    }
    return {};
}

tl::expected<CopyStartResponse, ErrorCode> MasterService::CopyStart(
    const UUID& client_id, const std::string& key,
    const std::string& src_segment,
    const std::vector<std::string>& tgt_segments) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", object not found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << " already has an ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    auto& metadata = accessor.Get();
    auto source = metadata.GetReplicaBySegmentName(src_segment);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", src_segment=" << src_segment
                   << ", replica not found or not valid";
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    std::vector<Replica> replicas;
    replicas.reserve(tgt_segments.size());
    {
        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        for (auto& tgt_segment : tgt_segments) {
            if (metadata.GetReplicaBySegmentName(tgt_segment) != nullptr) {
                // Skip used segments.
                continue;
            }

            auto replica = allocation_strategy_->AllocateFrom(
                allocator_manager, metadata.size, tgt_segment);
            if (!replica.has_value()) {
                LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                           << ", failed to allocate replica";
                return tl::make_unexpected(replica.error());
            }
            replicas.push_back(std::move(*replica));
        }
    }

    CopyStartResponse response;
    response.targets.reserve(replicas.size());
    std::vector<ReplicaID> replica_ids;
    replica_ids.reserve(replicas.size());

    response.source = source->get_descriptor();
    for (const auto& replica : replicas) {
        replica_ids.push_back(replica.id());
        response.targets.emplace_back(replica.get_descriptor());
    }

    // Create replication task for tracking.
    auto& shard = accessor.GetShard();
    shard->replication_tasks.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, std::chrono::system_clock::now(),
                              ReplicationTask::Type::COPY, source->id(),
                              std::move(replica_ids)));

    // Increase source refcnt to protect it from eviction.
    source->inc_refcnt();

    // Add replicas to the object.
    // DO NOT ACCESS source AFTER THIS !!!
    metadata.AddReplicas(std::move(replicas));

    return response;
}

tl::expected<void, ErrorCode> MasterService::CopyEnd(const UUID& client_id,
                                                     const std::string& key) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to CopyEnd key "
                   << key << ", was CopyStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::COPY) {
        LOG(ERROR) << "Ongoing replication task type is MOVE instead of COPY";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", source_id=" << source_id
                   << ", status=" << (source == nullptr ? "nullptr" : "invalid")
                   << ", copy source becomes invalid during data transfer";
        // Discard target replicas and clear the replication task.
        metadata.EraseReplicas([&task](const Replica& replica) {
            return std::find(task.replica_ids.begin(), task.replica_ids.end(),
                             replica.id()) != task.replica_ids.end();
        });
        accessor.EraseReplicationTask();
        if (!metadata.IsValid()) {
            // Remove the object if it does not have any replicas.
            accessor.Erase();
        }
        return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
    }

    // Decrement source reference count
    source->dec_refcnt();

    // Mark all replica_ids as complete
    bool all_complete = true;
    for (const auto& replica_id : task.replica_ids) {
        auto replica = metadata.GetReplicaByID(replica_id);
        if (replica == nullptr || replica->has_invalid_mem_handle()) {
            LOG(WARNING)
                << "key=" << key << ", replica_id=" << replica_id
                << ", copy target becomes invalid during data transfer";
            all_complete = false;
        } else {
            replica->mark_complete();
        }
    }

    accessor.EraseReplicationTask();

    return all_complete ? tl::expected<void, ErrorCode>()
                        : tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
}

tl::expected<void, ErrorCode> MasterService::CopyRevoke(
    const UUID& client_id, const std::string& key) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to CopyRevoke key "
                   << key << ", was CopyStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::COPY) {
        LOG(ERROR) << "Ongoing replication task type is MOVE instead of COPY";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr) {
        LOG(WARNING) << "key=" << key << ", source_id=" << source_id
                     << ", copy source not found during revoke";
    } else {
        // Decrement source reference count
        source->dec_refcnt();
    }

    // Erase all replica_ids
    for (const auto& replica_id : task.replica_ids) {
        metadata.EraseReplicaByID(replica_id);
    }

    accessor.EraseReplicationTask();

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }

    return {};
}

tl::expected<MoveStartResponse, ErrorCode> MasterService::MoveStart(
    const UUID& client_id, const std::string& key,
    const std::string& src_segment, const std::string& tgt_segment) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    if (src_segment == tgt_segment) {
        LOG(ERROR) << "key=" << key << ", move_tgt=" << tgt_segment
                   << " cannot be the same as move_src=" << src_segment;
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", object not found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << " already has an ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    auto& metadata = accessor.Get();
    auto source = metadata.GetReplicaBySegmentName(src_segment);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", src_segment=" << src_segment
                   << ", replica not found or not completed";
        return tl::make_unexpected(ErrorCode::REPLICA_NOT_FOUND);
    }

    std::vector<Replica> replicas;
    if (metadata.GetReplicaBySegmentName(tgt_segment) == nullptr) {
        ScopedAllocatorAccess allocator_access =
            segment_manager_.getAllocatorAccess();
        const auto& allocator_manager = allocator_access.getAllocatorManager();

        auto replica = allocation_strategy_->AllocateFrom(
            allocator_manager, metadata.size, tgt_segment);
        if (!replica.has_value()) {
            LOG(ERROR) << "key=" << key << ", tgt_segment=" << tgt_segment
                       << ", failed to allocate replica";
            return tl::make_unexpected(replica.error());
        }
        replicas.push_back(std::move(*replica));
    }

    MoveStartResponse response;
    std::vector<ReplicaID> replica_ids;

    response.source = source->get_descriptor();
    if (!replicas.empty()) {
        replica_ids.push_back(replicas[0].id());
        response.target = replicas[0].get_descriptor();
    } else {
        response.target = std::nullopt;
    }

    // Create replication task for tracking.
    auto& shard = accessor.GetShard();
    shard->replication_tasks.emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(client_id, std::chrono::system_clock::now(),
                              ReplicationTask::Type::MOVE, source->id(),
                              std::move(replica_ids)));

    // Increase source refcnt to protect it from eviction.
    source->inc_refcnt();

    // Add replicas to the object.
    // DO NOT ACCESS source AFTER THIS !!!
    metadata.AddReplicas(std::move(replicas));

    return response;
}

tl::expected<void, ErrorCode> MasterService::MoveEnd(const UUID& client_id,
                                                     const std::string& key) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to MoveEnd key "
                   << key << ", was MoveStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::MOVE) {
        LOG(ERROR) << "Ongoing replication task type is COPY instead of MOVE";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr || !source->is_completed() ||
        source->has_invalid_mem_handle()) {
        LOG(ERROR) << "key=" << key << ", source_id=" << source_id
                   << ", status=" << (source == nullptr ? "nullptr" : "invalid")
                   << ", move source becomes invalid during data transfer";
        // Discard target replica and clear the replication task.
        metadata.EraseReplicas([&task](const Replica& replica) {
            return std::find(task.replica_ids.begin(), task.replica_ids.end(),
                             replica.id()) != task.replica_ids.end();
        });
        accessor.EraseReplicationTask();
        if (!metadata.IsValid()) {
            // Remove the object if it does not have any replicas.
            accessor.Erase();
        }
        return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
    }

    // Decrement source reference count
    source->dec_refcnt();

    // If the move target has already existed on MoveStart, task.replica_ids
    // will be empty. Thus we need to check whether we have replica_ids to
    // process.
    if (!task.replica_ids.empty()) {
        auto replica_id = task.replica_ids[0];
        auto replica = metadata.GetReplicaByID(replica_id);
        if (replica == nullptr || replica->has_invalid_mem_handle()) {
            LOG(WARNING)
                << "key=" << key << ", replica_id=" << replica_id
                << ", move target becomes invalid during data transfer";
            accessor.EraseReplicationTask();
            return tl::make_unexpected(ErrorCode::REPLICA_IS_GONE);
        }

        // Mark replica as complete
        replica->mark_complete();
    }

    // Remove the source replica and release its space later.
    auto source_replica =
        metadata.PopReplicas([&source_id](const Replica& replica) {
            return replica.id() == source_id;
        });
    if (!source_replica.empty()) {
        std::lock_guard lock(discarded_replicas_mutex_);
        discarded_replicas_.emplace_back(
            std::move(source_replica),
            std::chrono::system_clock::now() + put_start_release_timeout_sec_);
    }

    accessor.EraseReplicationTask();

    return {};
}

tl::expected<void, ErrorCode> MasterService::MoveRevoke(
    const UUID& client_id, const std::string& key) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        LOG(ERROR) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (!accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key
                   << ", error=object has no ongoing replication task";
        return tl::make_unexpected(ErrorCode::OBJECT_NO_REPLICATION_TASK);
    }

    auto& task = accessor.GetReplicationTask();
    if (task.client_id != client_id) {
        LOG(ERROR) << "Illegal client " << client_id << " to MoveRevoke key "
                   << key << ", was MoveStart-ed by " << task.client_id;
        return tl::make_unexpected(ErrorCode::ILLEGAL_CLIENT);
    }

    if (task.type != ReplicationTask::Type::MOVE) {
        LOG(ERROR) << "Ongoing replication task type is COPY instead of MOVE";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    auto& metadata = accessor.Get();
    auto source_id = task.source_id;
    auto source = metadata.GetReplicaByID(source_id);
    if (source == nullptr) {
        LOG(WARNING) << "key=" << key << ", source_id=" << source_id
                     << ", move source not found during revoke";
    } else {
        // Decrement source reference count
        source->dec_refcnt();
    }

    // Erase all replica_ids (in MOVE operation, there should be at most one)
    for (const auto& replica_id : task.replica_ids) {
        metadata.EraseReplicaByID(replica_id);
    }

    accessor.EraseReplicationTask();

    if (!metadata.IsValid()) {
        // Remove the object if it does not have any replicas.
        accessor.Erase();
    }

    return {};
}

auto MasterService::Remove(const std::string& key, bool force)
    -> tl::expected<void, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRW accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", error=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    auto& metadata = accessor.Get();

    if (!force && !metadata.IsLeaseExpired()) {
        VLOG(1) << "key=" << key << ", error=object_has_lease";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_LEASE);
    }

    /**
     * The reason the force operation here does not bypass the replica
     * check is that put operations (which could also be copy or move)
     * and remove operations might be happening concurrently, making it
     * extremely dangerous to perform a direct removal at this point.
     */
    if (!metadata.AllReplicas(&Replica::fn_is_completed)) {
        LOG(ERROR) << "key=" << key << ", error=replica_not_ready";
        return tl::make_unexpected(ErrorCode::REPLICA_IS_NOT_READY);
    }
    if (accessor.HasReplicationTask()) {
        LOG(ERROR) << "key=" << key << ", error=object_has_replication_task";
        return tl::make_unexpected(ErrorCode::OBJECT_HAS_REPLICATION_TASK);
    }

    // HA behavior:
    // If etcd write fails, enqueue retry but still proceed with local remove.
    // Standby will handle gaps via timeout + late-arrival policy.
#ifdef STORE_USE_ETCD
    if (enable_ha_) {
        AppendOrPersistOrEnqueue("Remove", OpType::REMOVE, key, std::string(),
                                 PendingMutationKind::CLEAR_ALL_REPLICAS);
    } else {
        AppendOpLogAndNotify(OpType::REMOVE, key);
    }
#else
    if (!enable_ha_) {
        AppendOpLogAndNotify(OpType::REMOVE, key);
    }
#endif

    // Remove object metadata (may deallocate memory replicas)
    accessor.Erase();

    return {};
}

auto MasterService::RemoveByRegex(const std::string& regex_pattern, bool force)
    -> tl::expected<long, ErrorCode> {
    long removed_count = 0;
    std::regex pattern;

    try {
        pattern = std::regex(regex_pattern, std::regex::ECMAScript);
    } catch (const std::regex_error& e) {
        LOG(ERROR) << "Invalid regex pattern: " << regex_pattern
                   << ", error: " << e.what();
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    for (size_t i = 0; i < kNumShards; ++i) {
        MetadataShardAccessorRW shard(this, i);

        for (auto it = shard->metadata.begin(); it != shard->metadata.end();) {
            if (std::regex_search(it->first, pattern)) {
                if (!force && !it->second.IsLeaseExpired()) {
                    VLOG(1) << "key=" << it->first
                            << " matched by regex, but has lease. Skipping "
                            << "removal.";
                    ++it;
                    continue;
                }
                /**
                 * The reason the force operation here does not bypass the
                 * replica check is that put operations (which could also be
                 * copy or move) and remove operations might be happening
                 * concurrently, making it extremely dangerous to perform a
                 * direct removal at this point.
                 */
                if (!it->second.AllReplicas(&Replica::fn_is_completed)) {
                    LOG(WARNING) << "key=" << it->first
                                 << " matched by regex, but not all replicas "
                                    "are complete. Skipping removal.";
                    ++it;
                    continue;
                }
                if (metadata_shards_[i].replication_tasks.contains(it->first)) {
                    LOG(WARNING) << "key=" << it->first
                                 << ", matched by regex, but has replication "
                                    "task. Skipping removal.";
                    ++it;
                    continue;
                }

                VLOG(1) << "key=" << it->first
                        << " matched by regex. Removing.";
                it = shard->metadata.erase(it);
                removed_count++;
            } else {
                ++it;
            }
        }
    }

    VLOG(1) << "action=remove_by_regex, pattern=" << regex_pattern
            << ", removed_count=" << removed_count;
    return removed_count;
}

long MasterService::RemoveAll(bool force) {
    long removed_count = 0;
    uint64_t total_freed_size = 0;
    // Store the current time to avoid repeatedly
    // calling std::chrono::steady_clock::now()
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto now = std::chrono::system_clock::now();

    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRW shard(this, i);
        if (shard->metadata.empty()) {
            continue;
        }

        // Only remove completed objects with expired leases (unless force=true)
        auto it = shard->metadata.begin();
        while (it != shard->metadata.end()) {
            /**
             * The reason the force operation here does not bypass the replica
             * check is that put operations (which could also be copy or move)
             * and remove operations might be happening concurrently, making it
             * extremely dangerous to perform a direct removal at this point.
             */
            if ((force || it->second.IsLeaseExpired(now)) &&
                it->second.AllReplicas(&Replica::fn_is_completed) &&
                !shard->replication_tasks.contains(it->first)) {
                auto mem_rep_count =
                    it->second.CountReplicas(&Replica::fn_is_memory_replica);
                total_freed_size += it->second.size * mem_rep_count;
                it = shard->metadata.erase(it);
                removed_count++;
            } else {
                ++it;
            }
        }
    }

    VLOG(1) << "action=remove_all_objects"
            << ", removed_count=" << removed_count
            << ", total_freed_size=" << total_freed_size;
    return removed_count;
}

bool MasterService::CleanupStaleHandles(ObjectMetadata& metadata) {
    // Remove those with invalid allocators
    metadata.EraseReplicas([](const Replica& replica) {
        return replica.has_invalid_mem_handle();
    });

    // Return true if no valid replicas remain after cleanup
    return !metadata.IsValid();
}

size_t MasterService::GetKeyCount() const {
    size_t total = 0;
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRO shard(this, i);
        total += shard->metadata.size();
    }
    return total;
}

auto MasterService::Ping(const UUID& client_id)
    -> tl::expected<PingResponse, ErrorCode> {
    std::shared_lock<std::shared_mutex> lock(client_mutex_);
    ClientStatus client_status;
    auto it = ok_client_.find(client_id);
    if (it != ok_client_.end()) {
        client_status = ClientStatus::OK;
    } else {
        client_status = ClientStatus::NEED_REMOUNT;
    }
    PodUUID pod_client_id = {client_id.first, client_id.second};
    if (!client_ping_queue_.push(pod_client_id)) {
        // Queue is full
        LOG(ERROR) << "client_id=" << client_id
                   << ", error=client_ping_queue_full";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return PingResponse(view_version_, client_status);
}

tl::expected<std::string, ErrorCode> MasterService::GetFsdir() const {
    if (root_fs_dir_.empty() || cluster_id_.empty()) {
        LOG(INFO)
            << "Storage root directory or cluster ID is not set. persisting "
               "data is disabled.";
        return std::string();
    }
    return root_fs_dir_ + "/" + cluster_id_;
}

tl::expected<GetStorageConfigResponse, ErrorCode>
MasterService::GetStorageConfig() const {
    if (root_fs_dir_.empty() || cluster_id_.empty()) {
        LOG(INFO)
            << "Storage root directory or cluster ID is not set. persisting "
               "data is disabled.";
        return GetStorageConfigResponse("", enable_disk_eviction_,
                                        quota_bytes_);
    }
    std::string fsdir = root_fs_dir_ + "/" + cluster_id_;
    return GetStorageConfigResponse(fsdir, enable_disk_eviction_, quota_bytes_);
}

auto MasterService::MountLocalDiskSegment(const UUID& client_id,
                                          bool enable_offloading)
    -> tl::expected<void, ErrorCode> {
    if (!enable_offload_) {
        LOG(ERROR) << "	The offload functionality is not enabled";
        return tl::make_unexpected(ErrorCode::UNABLE_OFFLOAD);
    }
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    ScopedSegmentAccess segment_access = segment_manager_.getSegmentAccess();

    auto err =
        segment_access.MountLocalDiskSegment(client_id, enable_offloading);
    if (err == ErrorCode::SEGMENT_ALREADY_EXISTS) {
        // Return OK because this is an idempotent operation
        return {};
    } else if (err != ErrorCode::OK) {
        return tl::make_unexpected(err);
    }
    return {};
}

auto MasterService::OffloadObjectHeartbeat(const UUID& client_id,
                                           bool enable_offloading)
    -> tl::expected<std::unordered_map<std::string, int64_t>, ErrorCode> {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    ScopedLocalDiskSegmentAccess local_disk_segment_access =
        segment_manager_.getLocalDiskSegmentAccess();
    auto& client_local_disk_segment =
        local_disk_segment_access.getClientLocalDiskSegment();
    auto local_disk_segment_it = client_local_disk_segment.find(client_id);
    if (local_disk_segment_it == client_local_disk_segment.end()) {
        LOG(ERROR) << "Local disk segment not found with client id = "
                   << client_id;
        return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
    }
    MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
    local_disk_segment_it->second->enable_offloading = enable_offloading;
    if (enable_offloading) {
        return std::move(local_disk_segment_it->second->offloading_objects);
    }
    return {};
}

auto MasterService::NotifyOffloadSuccess(
    const UUID& client_id, const std::vector<std::string>& keys,
    const std::vector<StorageObjectMetadata>& metadatas)
    -> tl::expected<void, ErrorCode> {
    for (size_t i = 0; i < keys.size(); ++i) {
        const auto& key = keys[i];
        const auto& metadata = metadatas[i];

        // Release refcnt and clear offloading task.
        {
            MetadataAccessorRW accessor(this, key);
            if (accessor.Exists()) {
                auto& obj_metadata = accessor.Get();
                auto& shard = accessor.GetShard();
                auto task_it = shard->offloading_tasks.find(key);
                if (task_it != shard->offloading_tasks.end()) {
                    auto source =
                        obj_metadata.GetReplicaByID(task_it->second.source_id);
                    if (source != nullptr) {
                        source->dec_refcnt();
                    }
                    shard->offloading_tasks.erase(task_it);
                }
            }
        }

        // Add LOCAL_DISK replica.
        Replica replica(client_id, metadata.data_size,
                        metadata.transport_endpoint, ReplicaStatus::COMPLETE);
        auto res = AddReplica(client_id, key, replica);
        if (!res && res.error() != ErrorCode::OBJECT_NOT_FOUND) {
            LOG(ERROR) << "Failed to add replica: error=" << res.error()
                       << ", client_id=" << client_id << ", key=" << key;
            return tl::make_unexpected(res.error());
        }
    }
    return {};
}

tl::expected<void, ErrorCode> MasterService::PushOffloadingQueue(
    const std::string& key, Replica& replica) {
    const auto& segment_names = replica.get_segment_names();
    if (segment_names.empty()) {
        return {};
    }
    for (const auto& segment_name_it : segment_names) {
        if (!segment_name_it.has_value()) {
            continue;
        }
        ScopedLocalDiskSegmentAccess local_disk_segment_access =
            segment_manager_.getLocalDiskSegmentAccess();
        const auto& client_by_name =
            local_disk_segment_access.getClientByName();
        auto client_id_it = client_by_name.find(segment_name_it.value());
        if (client_id_it == client_by_name.end()) {
            LOG(ERROR) << "Segment " << segment_name_it.value() << " not found";
            return tl::make_unexpected(ErrorCode::SEGMENT_NOT_FOUND);
        }
        auto& client_local_disk_segment =
            local_disk_segment_access.getClientLocalDiskSegment();
        auto local_disk_segment_it =
            client_local_disk_segment.find(client_id_it->second);
        if (local_disk_segment_it == client_local_disk_segment.end()) {
            return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
        }
        MutexLocker locker(&local_disk_segment_it->second->offloading_mutex_);
        if (!local_disk_segment_it->second->enable_offloading) {
            return tl::make_unexpected(ErrorCode::UNABLE_OFFLOADING);
        }
        if (local_disk_segment_it->second->offloading_objects.size() >=
            offloading_queue_limit_) {
            return tl::make_unexpected(ErrorCode::KEYS_ULTRA_LIMIT);
        }
        local_disk_segment_it->second->offloading_objects.emplace(
            key, replica.get_descriptor()
                     .get_memory_descriptor()
                     .buffer_descriptor.size_);
    }
    return {};
}

void MasterService::EvictionThreadFunc() {
    VLOG(1) << "action=eviction_thread_started";

    auto last_discard_time = std::chrono::system_clock::now();
    while (eviction_running_) {
        const auto now = std::chrono::system_clock::now();
        double used_ratio =
            MasterMetricManager::instance().get_global_mem_used_ratio();
        if (used_ratio > eviction_high_watermark_ratio_ ||
            (need_eviction_ && eviction_ratio_ > 0.0)) {
            LOG(INFO) << "[EVICT-TRIGGER] memory_ratio=" << used_ratio
                      << " high_watermark=" << eviction_high_watermark_ratio_
                      << " need_eviction=" << need_eviction_
                      << " eviction_ratio=" << eviction_ratio_;
            double evict_ratio_target = std::max(
                eviction_ratio_,
                used_ratio - eviction_high_watermark_ratio_ + eviction_ratio_);
            double evict_ratio_lowerbound =
                std::max(evict_ratio_target * 0.5,
                         used_ratio - eviction_high_watermark_ratio_);
            BatchEvict(evict_ratio_target, evict_ratio_lowerbound);
            LOG(INFO) << "[EVICT-DONE] BatchEvict execution completed.";
            last_discard_time = now;
        } else if (now - last_discard_time > put_start_release_timeout_sec_) {
            // Try discarding expired processing keys and ongoing replication
            // tasks if we have not done this for a long time.
            {
                std::shared_lock<std::shared_mutex> shared_lock(
                    snapshot_mutex_);
                for (size_t i = 0; i < kNumShards; i++) {
                    MetadataShardAccessorRW shard(this, i);
                    DiscardExpiredProcessingReplicas(shard, now);
                }
                ReleaseExpiredDiscardedReplicas(now);
            }
            last_discard_time = now;
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kEvictionThreadSleepMs));
    }

    VLOG(1) << "action=eviction_thread_stopped";
}

void MasterService::DiscardExpiredProcessingReplicas(
    MetadataShardAccessorRW& shard,
    const std::chrono::system_clock::time_point& now) {
    std::list<DiscardedReplicas> discarded_replicas;

    // Part 1: Discard expired PutStart operations.
    for (auto key_it = shard->processing_keys.begin();
         key_it != shard->processing_keys.end();) {
        auto it = shard->metadata.find(*key_it);
        if (it == shard->metadata.end()) {
            // The key has been removed from metadata. This should be
            // impossible.
            LOG(ERROR) << "Key " << *key_it
                       << " was removed while in processing";
            key_it = shard->processing_keys.erase(key_it);
            continue;
        }

        auto& metadata = it->second;
        // If the object is not valid or not in processing state, just
        // remove it from the processing set.
        if (!metadata.IsValid() ||
            metadata.AllReplicas(&Replica::fn_is_completed)) {
            if (!metadata.IsValid()) {
                shard->metadata.erase(it);
            }
            key_it = shard->processing_keys.erase(key_it);
            continue;
        }

        // If the object's PutStart timedout, discard and release it's
        // space. Note that instead of releasing the space directly, we
        // insert the replicas into the discarded list so that the
        // discarding and releasing operations can be recorded in
        // statistics.
        const auto ttl =
            metadata.put_start_time + put_start_release_timeout_sec_;
        if (ttl < now) {
            auto replicas = metadata.PopReplicas(&Replica::fn_is_processing);
            if (!replicas.empty()) {
                discarded_replicas.emplace_back(std::move(replicas), ttl);
            }

            if (!metadata.IsValid()) {
                // All replicas of this object are discarded, just
                // remove the whole object.
                shard->metadata.erase(it);
            }

            key_it = shard->processing_keys.erase(key_it);
            continue;
        }

        key_it++;
    }

    // Part 2: Discard expired CopyStart/MoveStart operations.
    for (auto task_it = shard->replication_tasks.begin();
         task_it != shard->replication_tasks.end();) {
        auto metadata_it = shard->metadata.find(task_it->first);
        if (metadata_it == shard->metadata.end()) {
            // The key has been removed from metadata. This should be
            // impossible.
            LOG(ERROR) << "Key " << task_it->first
                       << " was removed with ongoing replication task";
            task_it = shard->replication_tasks.erase(task_it);
            continue;
        }

        const auto ttl =
            task_it->second.start_time + put_start_release_timeout_sec_;
        if (ttl > now) {
            // The task is not expired, skip it.
            task_it++;
            continue;
        }

        auto& metadata = metadata_it->second;

        // Release source refcnt.
        auto source = metadata.GetReplicaByID(task_it->second.source_id);
        if (source != nullptr) {
            source->dec_refcnt();
        }

        // Discard allocated replicas.
        auto& replica_ids = task_it->second.replica_ids;
        auto replicas =
            metadata.PopReplicas([&replica_ids](const Replica& replica) {
                auto it = std::find(replica_ids.begin(), replica_ids.end(),
                                    replica.id());
                return it != replica_ids.end();
            });
        if (!replicas.empty()) {
            discarded_replicas.emplace_back(std::move(replicas), ttl);
        }

        // Check whether the object is still valid.
        if (!metadata.IsValid()) {
            shard->metadata.erase(metadata_it);
        }

        task_it = shard->replication_tasks.erase(task_it);
    }

    // Part 3: Discard expired offloading operations.
    for (auto task_it = shard->offloading_tasks.begin();
         task_it != shard->offloading_tasks.end();) {
        const auto ttl =
            task_it->second.start_time + put_start_release_timeout_sec_;
        if (ttl > now) {
            task_it++;
            continue;
        }

        auto metadata_it = shard->metadata.find(task_it->first);
        if (metadata_it != shard->metadata.end()) {
            auto source =
                metadata_it->second.GetReplicaByID(task_it->second.source_id);
            if (source != nullptr) {
                source->dec_refcnt();
            }
        }

        LOG(WARNING) << "Offloading task expired for key: " << task_it->first;
        task_it = shard->offloading_tasks.erase(task_it);
    }

    if (!discarded_replicas.empty()) {
        std::lock_guard lock(discarded_replicas_mutex_);
        discarded_replicas_.splice(discarded_replicas_.end(),
                                   std::move(discarded_replicas));
    }
}

uint64_t MasterService::ReleaseExpiredDiscardedReplicas(
    const std::chrono::system_clock::time_point& now) {
    uint64_t released_cnt = 0;
    std::lock_guard lock(discarded_replicas_mutex_);
    discarded_replicas_.remove_if(
        [&now, &released_cnt](const DiscardedReplicas& item) {
            const bool expired = item.isExpired(now);
            if (expired && item.memSize() > 0) {
                released_cnt++;
            }
            return expired;
        });
    return released_cnt;
}

void MasterService::SnapshotThreadFunc() {
    LOG(INFO) << "[Snapshot] snapshot_thread started";
    while (snapshot_running_) {
        std::this_thread::sleep_for(
            std::chrono::seconds(snapshot_interval_seconds_));
        if (!enable_snapshot_) {
            // Snapshot is disabled
            LOG(INFO)
                << "[Snapshot] Snapshot is disabled, waiting for next cycle";
            continue;
        }
        // Fork a child process to save current state

        std::string snapshot_id =
            FormatTimestamp(std::chrono::system_clock::now());
        // For ETCD backend with daemon, do snapshot in parent process (no fork
        // needed)
        if (snapshot_backend_type_ == SnapshotBackendType::ETCD &&
            snapshot_daemon_pid_ != -1) {
            LOG(INFO) << "[Snapshot] Starting snapshot in parent process via "
                         "daemon, snapshot_id="
                      << snapshot_id;

            uint64_t snapshot_seq_id = 0;
            // Serialize in parent process (PersistState manages snapshot lock)
            auto result = PersistState(snapshot_id, &snapshot_seq_id);

            if (!result) {
                LOG(ERROR) << "[Snapshot] Failed to persist state via daemon, "
                              "snapshot_id="
                           << snapshot_id
                           << ", error=" << result.error().message;
                MasterMetricManager::instance().inc_snapshot_fail();
            } else {
                LOG(INFO) << "[Snapshot] Successfully persisted state via "
                             "daemon, snapshot_id="
                          << snapshot_id;
                MasterMetricManager::instance().inc_snapshot_success();

                if (snapshot_seq_id > 0) {
                    LOG(INFO) << "[Snapshot] Triggering OpLog cleanup before "
                                 "seq_id="
                              << snapshot_seq_id;
                    auto cleanup_res =
                        oplog_manager_.CleanupOpLogBefore(snapshot_seq_id);
                    if (cleanup_res != ErrorCode::OK) {
                        LOG(WARNING) << "[Snapshot] Failed to cleanup OpLog: "
                                     << cleanup_res;
                    }
                }
            }
            continue;
        }

        // Fallback: Fork a child process to save current state (for non-ETCD or
        // no daemon)
        LOG(INFO) << "[Snapshot] Preparing to fork child process, snapshot_id="
                  << snapshot_id;

        // Create pipe for child process logging
        int log_pipe[2];
        if (pipe(log_pipe) == -1) {
            LOG(ERROR) << "[Snapshot] Failed to create log pipe: "
                       << strerror(errno) << ", snapshot_id=" << snapshot_id;
            continue;
        }

        pid_t pid;
        uint64_t fork_seq_id = 0;
        {
            std::unique_lock<std::shared_mutex> lock(snapshot_mutex_);
            fork_seq_id = oplog_manager_.GetLastSequenceId();
            LOG(INFO) << "[Snapshot] Locking snapshot mutex, snapshot_id="
                      << snapshot_id;
            pid = fork();
        }
        if (pid == -1) {
            // Fork failed
            LOG(ERROR) << "[Snapshot] Failed to fork child process for state "
                          "persistence: "
                       << strerror(errno) << ", snapshot_id=" << snapshot_id;
            close(log_pipe[0]);
            close(log_pipe[1]);
        } else if (pid == 0) {
            // Child process
            // Close read end, set write end for logging
            close(log_pipe[0]);
            g_snapshot_log_pipe_fd = log_pipe[1];

            // Save current state using the configured persistence mechanism
            SNAP_LOG_INFO("[Snapshot] Child process started, snapshot_id={}",
                          snapshot_id);
            auto result = PersistState(snapshot_id);
            if (!result) {
                SNAP_LOG_ERROR(
                    "[Snapshot] Child process failed to persist state, "
                    "snapshot_id={},code={},msg={}",
                    snapshot_id, toString(result.error().code),
                    result.error().message);
                close(log_pipe[1]);
                _exit(1);  // Exit child process with error
            }
            SNAP_LOG_INFO(
                "[Snapshot] Child process successfully persisted state, "
                "snapshot_id={}",
                snapshot_id);

            close(log_pipe[1]);
            _exit(0);  // Exit child process successfully
        } else {
            // Parent process
            // Close write end, pass read end to wait function
            close(log_pipe[1]);
            bool success = WaitForSnapshotChild(pid, snapshot_id, log_pipe[0]);
            close(log_pipe[0]);
            if (success && fork_seq_id > 0) {
                LOG(INFO) << "[Snapshot] Triggering OpLog cleanup before "
                             "seq_id="
                          << fork_seq_id;
                auto cleanup_res =
                    oplog_manager_.CleanupOpLogBefore(fork_seq_id);
                if (cleanup_res != ErrorCode::OK) {
                    LOG(WARNING) << "[Snapshot] Failed to cleanup OpLog: "
                                 << cleanup_res;
                }
            }
        }
    }
    LOG(INFO) << "[Snapshot] snapshot_thread stopped";
}

bool MasterService::WaitForSnapshotChild(pid_t pid,
                                         const std::string& snapshot_id,
                                         int log_pipe_fd) {
    // Default 5 minute timeout
    const int64_t timeout_seconds = snapshot_child_timeout_seconds_;

    LOG(INFO)
        << "[Snapshot] waiting for child process to complete, snapshot_id="
        << snapshot_id << ", child_pid=" << pid
        << ", timeout=" << timeout_seconds << "s";

    // Set pipe to non-blocking mode
    int flags = fcntl(log_pipe_fd, F_GETFL, 0);
    if (flags == -1 || fcntl(log_pipe_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
        LOG(WARNING) << "[Snapshot] Failed to set pipe non-blocking: "
                     << strerror(errno);
    }

    // Buffer for reading child logs
    char buf[4096];
    std::string log_buffer;

    // Helper lambda to read and output child logs
    auto flush_child_logs = [&]() {
        while (true) {
            ssize_t n = read(log_pipe_fd, buf, sizeof(buf) - 1);
            if (n > 0) {
                buf[n] = '\0';
                log_buffer += buf;
                // Output complete lines
                size_t pos;
                while ((pos = log_buffer.find('\n')) != std::string::npos) {
                    std::string line = log_buffer.substr(0, pos);
                    log_buffer.erase(0, pos + 1);
                    if (!line.empty()) {
                        LOG(INFO) << "[Snapshot:Child] " << line;
                    }
                }
            } else {
                break;
            }
        }
    };

    // Record start time
    auto start_time = std::chrono::steady_clock::now();

    // Use non-blocking polling to wait
    while (true) {
        // Read child logs first
        flush_child_logs();

        int status;
        pid_t result = waitpid(pid, &status, WNOHANG);

        if (result == -1) {
            LOG(ERROR) << "[Snapshot] Failed to wait for child process: "
                       << strerror(errno) << ", snapshot_id=" << snapshot_id
                       << ", child_pid=" << pid;
            MasterMetricManager::instance().inc_snapshot_fail();
            return false;
        } else if (result == 0) {
            // Child process is still running
            auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                               std::chrono::steady_clock::now() - start_time)
                               .count();

            if (elapsed >= timeout_seconds) {
                // Timeout handling - flush remaining logs before killing
                flush_child_logs();
                if (!log_buffer.empty()) {
                    LOG(INFO) << "[Snapshot:Child] " << log_buffer;
                }
                HandleChildTimeout(pid, snapshot_id);
                MasterMetricManager::instance().inc_snapshot_fail();
                return false;
            }

            // Brief sleep before checking again
            std::this_thread::sleep_for(std::chrono::seconds(2));
        } else {
            // Child process has exited
            // Flush remaining logs from child
            flush_child_logs();
            // Output any remaining incomplete line
            if (!log_buffer.empty()) {
                LOG(INFO) << "[Snapshot:Child] " << log_buffer;
            }

            bool success = HandleChildExit(pid, status, snapshot_id);
            auto elapsed =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - start_time)
                    .count();
            MasterMetricManager::instance().set_snapshot_duration_ms(elapsed);
            return success;
        }
    }
}

void MasterService::HandleChildTimeout(pid_t pid,
                                       const std::string& snapshot_id) {
    LOG(WARNING) << "[Snapshot] Child process timeout, snapshot_id="
                 << snapshot_id << ", child_pid=" << pid
                 << ", killing child process";

    // Try to gracefully terminate the child process
    if (kill(pid, SIGTERM) == 0) {
        // Wait a few seconds to see if it exits gracefully
        std::this_thread::sleep_for(std::chrono::seconds(5));

        // Check if it has exited
        int status;
        if (waitpid(pid, &status, WNOHANG) == 0) {
            // Child process still not exited, force kill
            LOG(WARNING) << "[Snapshot] Child process still running, force "
                            "killing, snapshot_id="
                         << snapshot_id << ", child_pid=" << pid;
            kill(pid, SIGKILL);

            // Wait for force termination to complete
            waitpid(pid, &status, 0);
            LOG(WARNING)
                << "[Snapshot] Child process force killed, snapshot_id="
                << snapshot_id << ", child_pid=" << pid;
        } else {
            LOG(INFO) << "[Snapshot] Child process terminated gracefully after "
                         "SIGTERM, snapshot_id="
                      << snapshot_id << ", child_pid=" << pid;
        }
    } else {
        LOG(ERROR) << "[Snapshot] Failed to send SIGTERM to child process, "
                      "snapshot_id="
                   << snapshot_id << ", child_pid=" << pid
                   << ", error=" << strerror(errno);
    }
}

bool MasterService::HandleChildExit(pid_t pid, int status,
                                    const std::string& snapshot_id) {
    if (WIFEXITED(status)) {
        int exit_code = WEXITSTATUS(status);
        if (exit_code != 0) {
            LOG(ERROR) << "[Snapshot] Child process exited with error code: "
                       << exit_code << ", snapshot_id=" << snapshot_id
                       << ", child_pid=" << pid;
            MasterMetricManager::instance().inc_snapshot_fail();
            return false;
        } else {
            LOG(INFO) << "[Snapshot] Child process successfully persisted "
                         "state, snapshot_id="
                      << snapshot_id << ", child_pid=" << pid;
            MasterMetricManager::instance().inc_snapshot_success();
            return true;
        }
    } else if (WIFSIGNALED(status)) {
        int signal = WTERMSIG(status);
        LOG(ERROR) << "[Snapshot] Child process terminated by signal: "
                   << signal << ", snapshot_id=" << snapshot_id
                   << ", child_pid=" << pid;
        MasterMetricManager::instance().inc_snapshot_fail();
        return false;
    }
    return false;
}

tl::expected<void, SerializationError> MasterService::PersistState(
    const std::string& snapshot_id, uint64_t* out_seq_id) {
    try {
        auto SNAPSHOT_SERIALIZER_TYPE = "messagepack";

        SNAP_LOG_INFO(
            "[Snapshot] action=persisting_state start, snapshot_id={}, "
            "serializer_type={}, version={}",
            snapshot_id, SNAPSHOT_SERIALIZER_TYPE, SNAPSHOT_SERIALIZER_VERSION);
        uint64_t last_seq_id = 0;
        std::vector<uint8_t> serialized_metadata;
        std::vector<uint8_t> serialized_segment;
        std::vector<uint8_t> serialized_task_manager;
        {
            std::unique_lock<std::shared_mutex> lock(snapshot_mutex_);
            last_seq_id = oplog_manager_.GetLastSequenceId();

            MetadataSerializer metadata_serializer(this);
            SegmentSerializer segment_serializer(&segment_manager_);
            TaskManagerSerializer task_manager_serializer(&task_manager_);

            auto metadata_result = metadata_serializer.Serialize();
            if (!metadata_result) {
                SNAP_LOG_ERROR(
                    "[Snapshot] metadata serialization failed, snapshot_id={}, "
                    "code={}, msg={}",
                    snapshot_id, static_cast<int>(metadata_result.error().code),
                    metadata_result.error().message);

                return tl::make_unexpected(metadata_result.error());
            }
            SNAP_LOG_INFO(
                "[Snapshot] metadata serialization_successful, snapshot_id={}",
                snapshot_id);

            auto segment_result = segment_serializer.Serialize();
            if (!segment_result) {
                SNAP_LOG_ERROR(
                    "[Snapshot] segment serialization failed, snapshot_id={}, "
                    "code={}, msg={}",
                    snapshot_id, static_cast<int>(segment_result.error().code),
                    segment_result.error().message);
                return tl::make_unexpected(segment_result.error());
            }
            SNAP_LOG_INFO(
                "[Snapshot] segment serialization_successful, snapshot_id={}",
                snapshot_id);

            auto task_manager_result = task_manager_serializer.Serialize();
            if (!task_manager_result) {
                SNAP_LOG_ERROR(
                    "[Snapshot] task manager serialization failed, snapshot_id={}, "
                    "code={}, msg={}",
                    snapshot_id, static_cast<int>(task_manager_result.error().code),
                    task_manager_result.error().message);
                return tl::make_unexpected(task_manager_result.error());
            }
            SNAP_LOG_INFO(
                "[Snapshot] task manager serialization_successful, snapshot_id={}",
                snapshot_id);

            serialized_metadata = std::move(metadata_result.value());
            serialized_segment = std::move(segment_result.value());
            serialized_task_manager = std::move(task_manager_result.value());
        }

        if (out_seq_id) {
            *out_seq_id = last_seq_id;
        }

        // Create storage path prefix
        std::string path_prefix = SNAPSHOT_ROOT + "/" + snapshot_id + "/";

        SNAP_LOG_INFO("[Snapshot] Backend info: {}",
                      snapshot_backend_->GetConnectionInfo());

        // Prepare file paths
        std::string metadata_path = path_prefix + SNAPSHOT_METADATA_FILE;
        std::string segment_path = path_prefix + SNAPSHOT_SEGMENTS_FILE;
        std::string task_manager_path =
            path_prefix + SNAPSHOT_TASK_MANAGER_FILE;
        std::string manifest_path = path_prefix + SNAPSHOT_MANIFEST_FILE;
        std::string latest_path = SNAPSHOT_ROOT + "/" + SNAPSHOT_LATEST_FILE;

        // Prepare manifest
        uint32_t meta_crc = Crc32c(serialized_metadata);
        uint32_t seg_crc = Crc32c(serialized_segment);
        uint64_t meta_size = static_cast<uint64_t>(serialized_metadata.size());
        uint64_t seg_size = static_cast<uint64_t>(serialized_segment.size());
        auto timestamp =
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::system_clock::now().time_since_epoch())
                .count();
        
        // Format: protocol|version|snapshot_id|meta_size|meta_crc|seg_size|seg_crc|timestamp|status|oplog_seq_id
        std::string manifest_content =
            fmt::format("{}|{}|{}|{}|{}|{}|{}|{}|{}|{}", SNAPSHOT_SERIALIZER_TYPE,
                        SNAPSHOT_SERIALIZER_VERSION, snapshot_id, meta_size,
                        meta_crc, seg_size, seg_crc, timestamp, "complete", last_seq_id);
        std::vector<uint8_t> manifest_bytes(
            manifest_content.data(),
            manifest_content.data() + manifest_content.size());

        // Prepare latest marker
        std::string latest_content = snapshot_id;

        // For ETCD backend with daemon, use batch upload for better performance
        if (snapshot_backend_type_ == SnapshotBackendType::ETCD &&
            snapshot_daemon_pid_ != -1) {
            SNAP_LOG_INFO(
                "[Snapshot] Using daemon batch upload, snapshot_id={}",
                snapshot_id);

            fs::path staging_dir =
                fs::path(snapshot_backup_dir_) / "staging" / snapshot_id;
            auto dir_result = FileUtil::EnsureDirExists(staging_dir.string());
            if (!dir_result) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::PERSISTENT_FAIL,
                    "Failed to create staging dir: " + dir_result.error()));
            }

            std::string metadata_file =
                (staging_dir / SNAPSHOT_METADATA_FILE).string();
            std::string segments_file =
                (staging_dir / SNAPSHOT_SEGMENTS_FILE).string();
            std::string task_manager_file =
                (staging_dir / SNAPSHOT_TASK_MANAGER_FILE).string();
            std::string manifest_file =
                (staging_dir / SNAPSHOT_MANIFEST_FILE).string();
            std::string latest_file =
                (staging_dir / SNAPSHOT_LATEST_FILE).string();

            auto save_result =
                FileUtil::SaveBinaryToFile(serialized_metadata, metadata_file);
            if (!save_result) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::PERSISTENT_FAIL,
                    "Failed to save metadata to staging file: " +
                        save_result.error()));
            }
            save_result =
                FileUtil::SaveBinaryToFile(serialized_segment, segments_file);
            if (!save_result) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::PERSISTENT_FAIL,
                    "Failed to save segments to staging file: " +
                        save_result.error()));
            }
            save_result = FileUtil::SaveBinaryToFile(
                serialized_task_manager, task_manager_file);
            if (!save_result) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::PERSISTENT_FAIL,
                    "Failed to save task manager to staging file: " +
                        save_result.error()));
            }
            save_result =
                FileUtil::SaveBinaryToFile(manifest_bytes, manifest_file);
            if (!save_result) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::PERSISTENT_FAIL,
                    "Failed to save manifest to staging file: " +
                        save_result.error()));
            }
            auto save_str_result =
                FileUtil::SaveStringToFile(latest_content, latest_file);
            if (!save_str_result) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::PERSISTENT_FAIL,
                    "Failed to save latest to staging file: " +
                        save_str_result.error()));
            }

            // Release large buffers before uploading.
            std::vector<uint8_t>().swap(serialized_metadata);
            std::vector<uint8_t>().swap(serialized_segment);
            std::vector<uint8_t>().swap(serialized_task_manager);

            // Prepare all files for batch upload (key + local file path).
            std::vector<std::pair<std::string, std::string>> files;
            files.reserve(5);
            files.emplace_back(metadata_path, metadata_file);
            files.emplace_back(segment_path, segments_file);
            files.emplace_back(task_manager_path, task_manager_file);
            files.emplace_back(manifest_path, manifest_file);
            files.emplace_back(latest_path, latest_file);

            // Batch upload all files
            auto upload_result = UploadViaDaemon(files, snapshot_id);
            if (!upload_result) {
                SNAP_LOG_ERROR(
                    "[Snapshot] Daemon upload failed, keeping staging dir: {}",
                    staging_dir.string());
                return upload_result;
            }

            std::error_code ec;
            fs::remove_all(staging_dir, ec);
            if (ec) {
                LOG(WARNING)
                    << "[Snapshot] Failed to remove staging dir: "
                    << staging_dir.string() << ", error=" << ec.message();
            }
        } else {
            // Individual uploads for LOCAL/S3 backends (non-atomic fallback)
            // Upload core snapshot files first
            auto upload_result =
                UploadSnapshotFile(serialized_metadata, metadata_path,
                                   SNAPSHOT_METADATA_FILE, snapshot_id);
            if (!upload_result) {
                return tl::make_unexpected(upload_result.error());
            }

            upload_result =
                UploadSnapshotFile(serialized_segment, segment_path,
                                   SNAPSHOT_SEGMENTS_FILE, snapshot_id);
            if (!upload_result) {
                return tl::make_unexpected(upload_result.error());
            }

            upload_result = UploadSnapshotFile(
                serialized_task_manager, task_manager_path,
                SNAPSHOT_TASK_MANAGER_FILE, snapshot_id);
            if (!upload_result) {
                return tl::make_unexpected(upload_result.error());
            }

            upload_result =
                UploadSnapshotFile(manifest_bytes, manifest_path,
                                   SNAPSHOT_MANIFEST_FILE, snapshot_id);
            if (!upload_result) {
                return tl::make_unexpected(upload_result.error());
            }

            // Update latest marker last (only if core files succeeded)
            auto result =
                snapshot_backend_->UploadString(latest_path, latest_content);
            if (!result) {
                SNAP_LOG_ERROR(
                    "[Snapshot] latest update failed, snapshot_id={}, file={}",
                    snapshot_id, latest_path);
                auto save_path = fs::path(snapshot_backup_dir_) / "save" /
                                 SNAPSHOT_LATEST_FILE;
                auto save_result =
                    FileUtil::SaveStringToFile(latest_content, save_path);
                if (!save_result) {
                    SNAP_LOG_ERROR(
                        "[Snapshot] save latest to disk failed, "
                        "snapshot_id={}, "
                        "content={}, file={}",
                        snapshot_id, latest_content, save_path.string());
                }
                return tl::make_unexpected(
                    SerializationError(ErrorCode::PERSISTENT_FAIL,
                                       fmt::format("latest update failed")));
            }
            SNAP_LOG_INFO("[Snapshot] Upload latest success, snapshot_id={}",
                          snapshot_id);
        }

        CleanupOldSnapshot(snapshot_retention_count_, snapshot_id);
        SNAP_LOG_INFO("[Snapshot] action=persisting_state end, snapshot_id={}",
                      snapshot_id);
    } catch (const std::exception& e) {
        SNAP_LOG_ERROR(
            "[Snapshot] Exception during state persistent, snapshot_id={}, "
            "error={}",
            snapshot_id, e.what());
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL,
            fmt::format("Exception during state persistent: {}", e.what())));
    } catch (...) {
        SNAP_LOG_ERROR(
            "[Snapshot] Unknown exception during state persistent, "
            "snapshot_id={}",
            snapshot_id);
        return tl::make_unexpected(
            SerializationError(ErrorCode::PERSISTENT_FAIL,
                               "Unknown exception during state persistent"));
    }
    return {};
}

tl::expected<void, SerializationError> MasterService::UploadSnapshotFile(
    const std::vector<uint8_t>& data, const std::string& path,
    const std::string& local_filename, const std::string& snapshot_id) {
    SNAP_LOG_INFO("[Snapshot] Uploading {} to: {}, snapshot_id={}",
                  local_filename, path, snapshot_id);

    std::string error_msg;
    auto upload_result = snapshot_backend_->UploadBuffer(path, data);
    if (!upload_result) {
        SNAP_LOG_ERROR(
            "[Snapshot] {} upload failed, snapshot_id={}, file={}, error={}",
            local_filename, snapshot_id, path, upload_result.error());

        // Upload failed, save locally for manual recovery in exception
        // scenarios
        if (use_snapshot_backup_dir_) {
            auto save_path = fs::path(snapshot_backup_dir_) /
                             SNAPSHOT_BACKUP_SAVE_DIR / local_filename;
            auto save_result = FileUtil::SaveBinaryToFile(data, save_path);
            if (!save_result) {
                SNAP_LOG_ERROR(
                    "[Snapshot] save {} to disk failed, snapshot_id={}, "
                    "file={}",
                    local_filename, snapshot_id, save_path.string());
            }
        }

        error_msg.append(local_filename)
            .append(" upload ")
            .append(path)
            .append(" failed; ");
        return tl::make_unexpected(
            SerializationError(ErrorCode::PERSISTENT_FAIL, error_msg));
    } else {
        SNAP_LOG_INFO("[Snapshot] Upload {} success: {}, snapshot_id={}",
                      local_filename, path, snapshot_id);
    }

    return {};
}

void MasterService::CleanupOldSnapshot(int keep_count,
                                       const std::string& snapshot_id) {
    // 1. Read existing index to get snapshot list
    std::string index_path = SNAPSHOT_ROOT + "/" + SNAPSHOT_INDEX_FILE;
    std::string index_content;
    std::vector<std::string> existing_ids;
    if (snapshot_backend_->DownloadString(index_path, index_content)) {
        existing_ids = ParseSnapshotIndexContent(index_content);
        SNAP_LOG_INFO(
            "[Snapshot] Loaded snapshot index, count={}, snapshot_id={}",
            existing_ids.size(), snapshot_id);
    } else {
        SNAP_LOG_INFO(
            "[Snapshot] index not found, creating new index, "
            "snapshot_id={}",
            snapshot_id);
    }

    std::vector<std::string> new_ids;
    new_ids.reserve(existing_ids.size() + 1);
    std::unordered_set<std::string> seen;
    seen.insert(snapshot_id);
    new_ids.push_back(snapshot_id);

    for (const auto& id : existing_ids) {
        if (id.empty() || seen.count(id) != 0) {
            continue;
        }
        seen.insert(id);
        new_ids.push_back(id);
    }

    std::vector<std::string> to_delete;
    if (static_cast<int>(new_ids.size()) > keep_count) {
        to_delete.assign(new_ids.begin() + keep_count, new_ids.end());
        new_ids.resize(keep_count);
    }

    auto upload_result = snapshot_backend_->UploadString(
        index_path, BuildSnapshotIndexContent(new_ids));
    if (!upload_result) {
        SNAP_LOG_ERROR(
            "[Snapshot] index update failed, snapshot_id={}, file={}, will "
            "still try to delete old snapshots",
            snapshot_id, index_path);
        // Continue to delete old snapshots even if index update fails
        // to avoid space leak
    }

    for (const auto& old_id : to_delete) {
        if (old_id == snapshot_id) {
            continue;
        }
        std::string base_prefix = SNAPSHOT_ROOT + "/" + old_id + "/";
        std::string metadata_path = base_prefix + SNAPSHOT_METADATA_FILE;
        std::string segments_path = base_prefix + SNAPSHOT_SEGMENTS_FILE;
        std::string task_manager_path =
            base_prefix + SNAPSHOT_TASK_MANAGER_FILE;
        std::string manifest_path = base_prefix + SNAPSHOT_MANIFEST_FILE;

        auto delete_result =
            snapshot_backend_->DeleteObjectsWithPrefix(metadata_path);
        if (!delete_result) {
            SNAP_LOG_ERROR("[Snapshot] Failed to delete {}, snapshot_id={}",
                           metadata_path, snapshot_id);
        }

        delete_result =
            snapshot_backend_->DeleteObjectsWithPrefix(segments_path);
        if (!delete_result) {
            SNAP_LOG_ERROR("[Snapshot] Failed to delete {}, snapshot_id={}",
                           segments_path, snapshot_id);
        }

        delete_result =
            snapshot_backend_->DeleteObjectsWithPrefix(task_manager_path);
        if (!delete_result) {
            SNAP_LOG_ERROR("[Snapshot] Failed to delete {}, snapshot_id={}",
                           task_manager_path, snapshot_id);
        }

        delete_result =
            snapshot_backend_->DeleteObjectsWithPrefix(manifest_path);
        if (!delete_result) {
            SNAP_LOG_ERROR("[Snapshot] Failed to delete {}, snapshot_id={}",
                           manifest_path, snapshot_id);
        }
    }
}

void MasterService::RestoreState() {
    try {
        restored_from_snapshot_ = false;
        restored_snapshot_id_.clear();

        auto now = std::chrono::system_clock::now();

        LOG(INFO) << "[Restore] Backend info: "
                  << snapshot_backend_->GetConnectionInfo();

        std::string latest_path = SNAPSHOT_ROOT + "/" + SNAPSHOT_LATEST_FILE;
        std::string latest_content;
        std::vector<std::string> candidates;
        std::unordered_set<std::string> seen;
        if (!snapshot_backend_->DownloadString(latest_path, latest_content)) {
            LOG(WARNING) << "[Restore] latest.txt not found, trying index";
        } else {
            std::string trimmed_latest = TrimWhitespace(latest_content);
            if (!trimmed_latest.empty()) {
                auto pipe_pos = trimmed_latest.rfind('|');
                std::string latest_id =
                    (pipe_pos == std::string::npos)
                        ? trimmed_latest
                        : trimmed_latest.substr(pipe_pos + 1);
                if (!latest_id.empty() && seen.insert(latest_id).second) {
                    candidates.push_back(latest_id);
                }
            }
        }

        std::string index_path = SNAPSHOT_ROOT + "/" + SNAPSHOT_INDEX_FILE;
        std::string index_content;
        if (snapshot_backend_->DownloadString(index_path, index_content)) {
            auto index_ids = ParseSnapshotIndexContent(index_content);
            for (const auto& id : index_ids) {
                if (!id.empty() && seen.insert(id).second) {
                    candidates.push_back(id);
                }
            }
        }

        if (candidates.empty()) {
            LOG(ERROR)
                << "[Restore] No previous snapshot found, starting fresh";
            return;
        }

        bool restored = false;
        std::string restored_snapshot_id;
        for (const auto& state_id : candidates) {
            std::string path_prefix = SNAPSHOT_ROOT + "/" + state_id + "/";

            // 2. Download manifest.txt to parse protocol version info
            std::string manifest_path = path_prefix + SNAPSHOT_MANIFEST_FILE;
            std::string manifest_content;
            if (!snapshot_backend_->DownloadString(manifest_path,
                                                   manifest_content)) {
                LOG(ERROR) << "[Restore] Failed to download manifest file: "
                           << manifest_path;
                continue;
            }

            // Format:
            // protocol|version|snapshot_id|meta_size|meta_crc|seg_size|seg_crc|ts|complete
            std::vector<std::string> parts;
            boost::split(parts, manifest_content, boost::is_any_of("|"));

            std::string protocol_type;  // Protocol type
            std::string version;        // Version
            bool has_checksum = false;
            uint64_t expected_meta_size = 0;
            uint32_t expected_meta_crc = 0;
            uint64_t expected_seg_size = 0;
            uint32_t expected_seg_crc = 0;
            uint64_t snapshot_oplog_seq_id = 0;

            if (parts.size() >= 3) {
                protocol_type = parts[0];
                version = parts[1];
            } else {
                LOG(ERROR) << "[Restore] Invalid manifest format: "
                           << manifest_path;
                continue;
            }

            if (parts.size() >= 9) {
                try {
                    expected_meta_size = std::stoull(parts[3]);
                    expected_meta_crc =
                        static_cast<uint32_t>(std::stoul(parts[4]));
                    expected_seg_size = std::stoull(parts[5]);
                    expected_seg_crc =
                        static_cast<uint32_t>(std::stoul(parts[6]));
                    std::string status = TrimWhitespace(parts[8]);
                    if (!status.empty() && status != "complete" &&
                        status != "1") {
                        LOG(ERROR) << "[Restore] Snapshot status not complete: "
                                   << status << ", snapshot_id=" << state_id;
                        continue;
                    }
                    has_checksum = true;
                } catch (const std::exception& e) {
                    LOG(ERROR) << "[Restore] Failed to parse manifest fields "
                                  "for snapshot "
                               << state_id << ": " << e.what();
                    continue;
                }
            }

            if (parts.size() >= 10) {
                try {
                    snapshot_oplog_seq_id = std::stoull(parts[9]);
                } catch (...) {
                    LOG(WARNING) << "[Restore] Failed to parse oplog seq id "
                                    "from manifest";
                }
            }

            LOG(INFO) << "[Restore] Restoring state from snapshot: " << state_id
                      << " version: " << version
                      << " protocol: " << protocol_type
                      << " oplog_seq: " << snapshot_oplog_seq_id;

            // 3. Download metadata
            std::string metadata_path = path_prefix + SNAPSHOT_METADATA_FILE;
            std::vector<uint8_t> metadata_content;
            auto download_result = snapshot_backend_->DownloadBuffer(
                metadata_path, metadata_content);
            if (!download_result) {
                LOG(ERROR) << "[Restore] Failed to download metadata file: "
                           << metadata_path
                           << " error=" << download_result.error();
                continue;
            }

            // 4. Download segments
            std::string segments_path = path_prefix + SNAPSHOT_SEGMENTS_FILE;
            std::vector<uint8_t> segments_content;
            download_result = snapshot_backend_->DownloadBuffer(
                segments_path, segments_content);
            if (!download_result) {
                LOG(ERROR) << "[Restore] Failed to download segments file: "
                           << segments_path
                           << " error=" << download_result.error();
                continue;
            }

            // 4.5. Download task manager
            std::string task_manager_path =
                path_prefix + SNAPSHOT_TASK_MANAGER_FILE;
            std::vector<uint8_t> task_manager_content;
            download_result = snapshot_backend_->DownloadBuffer(
                task_manager_path, task_manager_content);
            if (!download_result) {
                LOG(WARNING)
                    << "[Restore] Failed to download task manager file: "
                    << task_manager_path
                    << " error=" << download_result.error()
                    << " (may not exist in older snapshots, skipping)";
                task_manager_content.clear();
            }

            if (has_checksum) {
                if (metadata_content.size() != expected_meta_size ||
                    Crc32c(metadata_content) != expected_meta_crc) {
                    LOG(ERROR)
                        << "[Restore] Metadata checksum mismatch for snapshot: "
                        << state_id;
                    continue;
                }
                if (segments_content.size() != expected_seg_size ||
                    Crc32c(segments_content) != expected_seg_crc) {
                    LOG(ERROR)
                        << "[Restore] Segments checksum mismatch for snapshot: "
                        << state_id;
                    continue;
                }
            }

            auto save_result = FileUtil::SaveStringToFile(
                manifest_content, fs::path(snapshot_backup_dir_) / SNAPSHOT_BACKUP_RESTORE_DIR /
                                      SNAPSHOT_MANIFEST_FILE);
            if (!save_result) {
                LOG(ERROR) << "[Restore] Failed to save manifest to file: "
                           << save_result.error();
            }
            save_result = FileUtil::SaveBinaryToFile(
                metadata_content, fs::path(snapshot_backup_dir_) / SNAPSHOT_BACKUP_RESTORE_DIR /
                                      SNAPSHOT_METADATA_FILE);
            if (!save_result) {
                LOG(ERROR) << "[Restore] Failed to save metadata to file: "
                           << save_result.error();
            }
            save_result = FileUtil::SaveBinaryToFile(
                segments_content, fs::path(snapshot_backup_dir_) / SNAPSHOT_BACKUP_RESTORE_DIR /
                                      SNAPSHOT_SEGMENTS_FILE);
            if (!save_result) {
                LOG(ERROR) << "[Restore] Failed to save segments to file: "
                           << save_result.error();
            }
            if (!task_manager_content.empty()) {
                save_result = FileUtil::SaveBinaryToFile(
                    task_manager_content,
                    fs::path(snapshot_backup_dir_) / SNAPSHOT_BACKUP_RESTORE_DIR /
                        SNAPSHOT_TASK_MANAGER_FILE);
                if (!save_result) {
                    LOG(ERROR)
                        << "[Restore] Failed to save task manager to file: "
                        << save_result.error();
                }
            }

            // 5. Deserialize state
            SegmentSerializer segment_serializer(&segment_manager_);
            MetadataSerializer metadata_serializer(this);
            TaskManagerSerializer task_manager_serializer(&task_manager_);

            auto segments_result =
                segment_serializer.Deserialize(segments_content);
            if (!segments_result) {
                LOG(ERROR) << "[Restore] Failed to deserialize segments: "
                           << segments_result.error().code << " - "
                           << segments_result.error().message;
                segment_serializer.Reset();
                continue;
            }

            auto metadata_result =
                metadata_serializer.Deserialize(metadata_content);
            if (!metadata_result) {
                LOG(ERROR) << "[Restore] Failed to deserialize metadata: "
                           << metadata_result.error().code;
                metadata_serializer.Reset();
                segment_serializer.Reset();
                continue;
            }

            LOG(INFO) << "[Restore] Deserialize metadata success";

            if (!task_manager_content.empty()) {
                auto task_manager_result =
                    task_manager_serializer.Deserialize(task_manager_content);
                if (!task_manager_result) {
                    LOG(ERROR)
                        << "[Restore] Failed to deserialize task manager: "
                        << task_manager_result.error().code << " - "
                        << task_manager_result.error().message;
                    task_manager_serializer.Reset();
                    metadata_serializer.Reset();
                    segment_serializer.Reset();
                    continue;
                }
                LOG(INFO) << "[Restore] Deserialize task manager success";
            }

            restored = true;
            restored_snapshot_id = state_id;
            break;
        }

        if (!restored) {
            LOG(ERROR) << "[Restore] Failed to restore from any snapshot, "
                          "starting fresh";
            return;
        }

        std::vector<std::string> segment_names;
        {
            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            segment_access.GetAllSegmentNames(segment_names);
        }

        {
            // After deserialization, iterate through metadata_shards_ to clean
            // up non-ready metadata
            const bool env_skip_cleanup = std::getenv(
                "MOONCAKE_MASTER_SERVICE_SNAPSHOT_TEST_SKIP_CLEANUP");
            const bool skip_cleanup =
                env_skip_cleanup || !enable_snapshot_restore_clean_metadata_;
            if (!skip_cleanup) {
                for (auto& shard : metadata_shards_) {
                    for (auto it = shard.metadata.begin();
                         it != shard.metadata.end();) {
                        if (it->second.HasDiffRepStatus(
                                ReplicaStatus::COMPLETE) ||
                            (it->second.IsLeaseExpired() &&
                             !it->second.IsSoftPinned(now))) {
                            VLOG(1)
                                << "clear metadata key=" << it->first
                                << " ,lease_timeout="
                                << std::chrono::duration_cast<
                                       std::chrono::milliseconds>(
                                       it->second.lease_timeout
                                           .time_since_epoch())
                                       .count()
                                << " ,soft_pin_timeout="
                                << (it->second.soft_pin_timeout.has_value()
                                        ? std::to_string(
                                              std::chrono::duration_cast<
                                                  std::chrono::milliseconds>(
                                                  it->second.soft_pin_timeout
                                                      .value()
                                                      .time_since_epoch())
                                                  .count())
                                        : "null");
                            it = shard.metadata.erase(it);
                        } else {
                            ++it;
                        }
                    }
                }
            }

            // Restore memory usage
            // Step 1: Reset memory usage
            MasterMetricManager::instance().reset_allocated_mem_size();
            for (auto& segment_name : segment_names) {
                MasterMetricManager::instance()
                    .reset_segment_allocated_mem_size(segment_name);
            }

            for (auto& shard : metadata_shards_) {
                for (auto it = shard.metadata.begin();
                     it != shard.metadata.end();) {
                    for (auto& replica : it->second.GetAllReplicas()) {
                        if (!replica.get_descriptor().is_memory_replica()) {
                            continue;
                        }
                        auto temp_segment_names = replica.get_segment_names();
                        if (temp_segment_names.empty()) {
                            continue;
                        }

                        std::string temp_segment_name;
                        if (temp_segment_names[0].has_value()) {
                            temp_segment_name = temp_segment_names[0].value();
                        }

                        auto buffer_descriptor = replica.get_descriptor()
                                                     .get_memory_descriptor()
                                                     .buffer_descriptor;
                        MasterMetricManager::instance().inc_allocated_mem_size(
                            temp_segment_name,
                            static_cast<int64_t>(buffer_descriptor.size_));
                    }
                    ++it;
                }
            }

            LOG(INFO)
                << "[Restore] Total allocated size after restore: "
                << MasterMetricManager::instance().get_allocated_mem_size();
        }

        {
            // Reset total capacity
            MasterMetricManager::instance().reset_total_mem_capacity();
            for (auto& segment_name : segment_names) {
                MasterMetricManager::instance()
                    .reset_segment_total_mem_capacity(segment_name);
            }

            ScopedSegmentAccess segment_access =
                segment_manager_.getSegmentAccess();
            std::vector<std::pair<Segment, UUID>> unready_segments;

            // Get all unready segments and their corresponding client_ids
            if (segment_access.GetUnreadySegments(unready_segments) ==
                ErrorCode::OK) {
                // Remove all unready segments
                for (const auto& [segment, client_id] : unready_segments) {
                    UnmountSegment(segment.id, client_id);
                }
            }

            std::vector<std::pair<Segment, UUID>> all_segments;
            auto err = segment_access.GetAllSegments(all_segments);

            if (err == ErrorCode::OK) {
                int64_t total_size = 0;
                for (const auto& [segment, client_id] : all_segments) {
                    Ping(client_id);  // Add to heartbeat monitoring

                    // Add client to ok_client_ to avoid unnecessary remount
                    // after restore
                    {
                        std::unique_lock<std::shared_mutex> lock(client_mutex_);
                        ok_client_.insert(client_id);
                    }

                    total_size += static_cast<int64_t>(segment.size);
                    // Restore segment usage
                    MasterMetricManager::instance().inc_total_mem_capacity(
                        segment.name, segment.size);
                }
                LOG(INFO) << "[Restore] Total capacity size after restore: "
                          << total_size;
            } else {
                LOG(ERROR) << "[Restore] Failed to get all segments, error: "
                           << err;
            }
        }

        LOG(INFO) << "[Restore] Successfully restored state from snapshot: "
                  << restored_snapshot_id;

        restored_from_snapshot_ = true;
        restored_snapshot_id_ = restored_snapshot_id;

    } catch (const std::exception& e) {
        LOG(ERROR) << "[Restore] Exception during state restoration: "
                   << e.what();
    } catch (...) {
        LOG(ERROR) << "[Restore] Unknown exception during state restoration";
    }
}
void MasterService::BatchEvict(double evict_ratio_target,
                               double evict_ratio_lowerbound) {
    if (evict_ratio_target < evict_ratio_lowerbound) {
        LOG(ERROR) << "evict_ratio_target=" << evict_ratio_target
                   << ", evict_ratio_lowerbound=" << evict_ratio_lowerbound
                   << ", error=invalid_params";
        evict_ratio_lowerbound = evict_ratio_target;
    }

    auto now = std::chrono::system_clock::now();
    long evicted_count = 0;
    long object_count = 0;
    uint64_t total_freed_size = 0;

    // Candidates for second pass eviction
    std::vector<std::chrono::system_clock::time_point> no_pin_objects;
    std::vector<std::chrono::system_clock::time_point> soft_pin_objects;

    auto can_evict_replicas = [](const ObjectMetadata& metadata) {
        return metadata.HasReplica([](const Replica& replica) {
            return replica.is_memory_replica() && replica.is_completed() &&
                   replica.get_refcnt() == 0;
        });
    };

    auto evict_replicas = [](ObjectMetadata& metadata) {
        return metadata.EraseReplicas([](const Replica& replica) {
            return replica.is_memory_replica() && replica.is_completed() &&
                   replica.get_refcnt() == 0;
        });
    };

    // Randomly select a starting shard to avoid imbalance eviction between
    // shards. No need to use expensive random_device here.
    size_t start_idx = rand() % kNumShards;
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);

    // First pass: evict objects without soft pin and lease expired
    for (size_t i = 0; i < kNumShards; i++) {
        MetadataShardAccessorRW shard(this, (start_idx + i) % kNumShards);

        // Discard expired processing keys first so that they won't be counted
        // in later evictions.
        DiscardExpiredProcessingReplicas(shard, now);

        // object_count must be updated at beginning as it will be used later
        // to compute ideal_evict_num
        object_count += shard->metadata.size();

        // To achieve evicted_count / object_count = evict_ratio_target,
        // ideally how many object should be evicted in this shard
        const long ideal_evict_num =
            std::ceil(object_count * evict_ratio_target) - evicted_count;

        std::vector<std::chrono::system_clock::time_point>
            candidates;  // can be removed
        for (auto it = shard->metadata.begin(); it != shard->metadata.end();
             it++) {
            // Skip objects that are not expired or have incomplete replicas
            if (!it->second.IsLeaseExpired(now) ||
                !can_evict_replicas(it->second)) {
                continue;
            }
            if (!it->second.IsSoftPinned(now)) {
                if (ideal_evict_num > 0) {
                    // first pass candidates
                    candidates.push_back(it->second.lease_timeout);
                } else {
                    // No need to evict any object in this shard, put to
                    // second pass candidates
                    no_pin_objects.push_back(it->second.lease_timeout);
                }
            } else if (allow_evict_soft_pinned_objects_) {
                // second pass candidates, only if
                // allow_evict_soft_pinned_objects_ is true
                soft_pin_objects.push_back(it->second.lease_timeout);
            }
        }

        if (ideal_evict_num > 0 && !candidates.empty()) {
            long evict_num = std::min(ideal_evict_num, (long)candidates.size());
            long shard_evicted_count =
                0;  // number of objects evicted from this shard
            std::nth_element(candidates.begin(),
                             candidates.begin() + (evict_num - 1),
                             candidates.end());
            auto target_timeout = candidates[evict_num - 1];
            // Evict objects with lease timeout less than or equal to target.
            auto it = shard->metadata.begin();
            while (it != shard->metadata.end()) {
                // Skip objects that are not allowed to be evicted in the first
                // pass
                if (!it->second.IsLeaseExpired(now) ||
                    it->second.IsSoftPinned(now) ||
                    !can_evict_replicas(it->second)) {
                    ++it;
                    continue;
                }
                if (it->second.lease_timeout <= target_timeout) {
                    // Evict this object (MEMORY replicas only).
                    //
                    // Scheme A:
                    // - If key remains valid after removing MEMORY replicas,
                    //   durably persist a PUT_END carrying the updated metadata
                    //   (without MEMORY replicas) before freeing memory.
                    // - If key becomes invalid (only had MEMORY replicas),
                    //   durably persist REMOVE before freeing memory.
                    if (enable_ha_) {
                        const bool has_non_mem_replica =
                            it->second.HasReplica([](const Replica& r) {
                                return r.type() != ReplicaType::MEMORY;
                            });
                        if (has_non_mem_replica) {
                            AppendOrPersistOrEnqueueLazy(
                                "BatchEvict(PUT_END)", OpType::PUT_END,
                                it->first,
                                [&]() {
                                    return SerializeMetadataForOpLogWithoutMemReplicas(
                                        it->second);
                                },
                                PendingMutationKind::EVICT_MEM_REPLICAS);
                        } else {
                            AppendOrPersistOrEnqueue(
                                "BatchEvict(REMOVE)", OpType::REMOVE, it->first,
                                std::string(),
                                PendingMutationKind::EVICT_MEM_REPLICAS);
                        }
                    }

                    total_freed_size +=
                        it->second.size * evict_replicas(it->second);

                    if (it->second.IsValid() == false) {
                        it = shard->metadata.erase(it);
                    } else {
                        ++it;
                    }
                    shard_evicted_count++;
                } else {
                    // second pass candidates
                    no_pin_objects.push_back(it->second.lease_timeout);
                    ++it;
                }
            }
            evicted_count += shard_evicted_count;
        }
    }

    // Try releasing discarded replicas before we decide whether to do the
    // second pass.
    uint64_t released_discarded_cnt = ReleaseExpiredDiscardedReplicas(now);

    // The ideal number of objects to evict in the second pass
    long target_evict_num = std::ceil(object_count * evict_ratio_lowerbound) -
                            evicted_count - released_discarded_cnt;
    // The actual number of objects we can evict in the second pass
    target_evict_num =
        std::min(target_evict_num,
                 (long)no_pin_objects.size() + (long)soft_pin_objects.size());

    // Do second pass eviction only if 1). there are candidates that can be
    // evicted AND 2). The evicted number in the first pass is less than
    // evict_ratio_lowerbound.
    if (target_evict_num > 0) {
        // If 1). there are enough candidates without soft pin OR 2). soft pin
        // candidates are empty, then do second pass A. Otherwise, do second
        // pass B. Note that the second condition is ensured implicitly by the
        // calculation of target_evict_num.
        if (target_evict_num <= static_cast<long>(no_pin_objects.size())) {
            // Second pass A: only evict objects without soft pin. The following
            // code is error-prone if target_evict_num > no_pin_objects.size().

            std::nth_element(no_pin_objects.begin(),
                             no_pin_objects.begin() + (target_evict_num - 1),
                             no_pin_objects.end());
            auto target_timeout = no_pin_objects[target_evict_num - 1];

            // Evict objects with lease timeout less than or equal to target.
            // Stop when the target is reached.
            for (size_t i = 0; i < kNumShards && target_evict_num > 0; i++) {
                MetadataShardAccessorRW shard(this,
                                              (start_idx + i) % kNumShards);
                auto it = shard->metadata.begin();
                while (it != shard->metadata.end() && target_evict_num > 0) {
                    if (it->second.lease_timeout <= target_timeout &&
                        !it->second.IsSoftPinned(now) &&
                        can_evict_replicas(it->second)) {
                        // Evict this object (MEMORY replicas only). See Scheme
                        // A above.
                        if (enable_ha_) {
                            const bool has_non_mem_replica =
                                it->second.HasReplica([](const Replica& r) {
                                    return r.type() != ReplicaType::MEMORY;
                                });
                            if (has_non_mem_replica) {
                                AppendOrPersistOrEnqueueLazy(
                                    "BatchEvict(PUT_END)", OpType::PUT_END,
                                    it->first,
                                    [&]() {
                                        return SerializeMetadataForOpLogWithoutMemReplicas(
                                            it->second);
                                    },
                                    PendingMutationKind::EVICT_MEM_REPLICAS);
                            } else {
                                AppendOrPersistOrEnqueue(
                                    "BatchEvict(REMOVE)", OpType::REMOVE,
                                    it->first, std::string(),
                                    PendingMutationKind::EVICT_MEM_REPLICAS);
                            }
                        }

                        total_freed_size +=
                            it->second.size * evict_replicas(it->second);
                        if (it->second.IsValid() == false) {
                            it = shard->metadata.erase(it);
                        } else {
                            ++it;
                        }
                        evicted_count++;
                        target_evict_num--;
                    } else {
                        ++it;
                    }
                }
            }
        } else if (!soft_pin_objects.empty()) {
            // Second pass B: Prioritize evicting objects without soft pin, but
            // also allow to evict soft pinned objects. The following code is
            // error-prone if the soft pin objects are empty.

            const long soft_pin_evict_num =
                target_evict_num - static_cast<long>(no_pin_objects.size());
            // For soft pin objects, prioritize to evict the ones with smaller
            // lease timeout.
            std::nth_element(
                soft_pin_objects.begin(),
                soft_pin_objects.begin() + (soft_pin_evict_num - 1),
                soft_pin_objects.end());
            auto soft_target_timeout = soft_pin_objects[soft_pin_evict_num - 1];

            // Stop when the target is reached.
            for (size_t i = 0; i < kNumShards && target_evict_num > 0; i++) {
                MetadataShardAccessorRW shard(this,
                                              (start_idx + i) % kNumShards);

                auto it = shard->metadata.begin();
                while (it != shard->metadata.end() && target_evict_num > 0) {
                    // Skip objects that are not expired or have incomplete
                    // replicas
                    if (!it->second.IsLeaseExpired(now) ||
                        !can_evict_replicas(it->second)) {
                        ++it;
                        continue;
                    }
                    // Evict objects with 1). no soft pin OR 2). with soft pin
                    // and lease timeout less than or equal to target.
                    if (!it->second.IsSoftPinned(now) ||
                        it->second.lease_timeout <= soft_target_timeout) {
                        if (enable_ha_) {
                            const bool has_non_mem_replica =
                                it->second.HasReplica([](const Replica& r) {
                                    return r.type() != ReplicaType::MEMORY;
                                });
                            if (has_non_mem_replica) {
                                AppendOrPersistOrEnqueueLazy(
                                    "BatchEvict(PUT_END)", OpType::PUT_END,
                                    it->first,
                                    [&]() {
                                        return SerializeMetadataForOpLogWithoutMemReplicas(
                                            it->second);
                                    },
                                    PendingMutationKind::EVICT_MEM_REPLICAS);
                            } else {
                                AppendOrPersistOrEnqueue(
                                    "BatchEvict(REMOVE)", OpType::REMOVE,
                                    it->first, std::string(),
                                    PendingMutationKind::EVICT_MEM_REPLICAS);
                            }
                        }

                        total_freed_size +=
                            it->second.size * evict_replicas(it->second);
                        if (it->second.IsValid() == false) {
                            it = shard->metadata.erase(it);
                        } else {
                            ++it;
                        }
                        evicted_count++;
                        target_evict_num--;
                    } else {
                        ++it;
                    }
                }
            }
        } else {
            // This should not happen.
            LOG(ERROR) << "Error in second pass eviction: target_evict_num="
                       << target_evict_num
                       << ", no_pin_objects.size()=" << no_pin_objects.size()
                       << ", soft_pin_objects.size()="
                       << soft_pin_objects.size()
                       << ", evicted_count=" << evicted_count
                       << ", object_count=" << object_count
                       << ", evict_ratio_target=" << evict_ratio_target
                       << ", evict_ratio_lowerbound=" << evict_ratio_lowerbound;
        }
    }

    if (evicted_count > 0 || released_discarded_cnt > 0) {
        need_eviction_ = false;
        MasterMetricManager::instance().inc_eviction_success(evicted_count,
                                                             total_freed_size);
    } else {
        if (object_count == 0) {
            // No objects to evict, no need to check again
            need_eviction_ = false;
        }
        MasterMetricManager::instance().inc_eviction_fail();
    }
    VLOG(1) << "action=evict_objects" << ", evicted_count=" << evicted_count
            << ", total_freed_size=" << total_freed_size;
}

void MasterService::ClientMonitorFunc() {
    std::unordered_map<UUID, std::chrono::steady_clock::time_point,
                       boost::hash<UUID>>
        client_ttl;
    while (client_monitor_running_) {
        auto now = std::chrono::steady_clock::now();

        // Update the client ttl
        PodUUID pod_client_id;
        while (client_ping_queue_.pop(pod_client_id)) {
            UUID client_id = {pod_client_id.first, pod_client_id.second};
            client_ttl[client_id] =
                now + std::chrono::seconds(client_live_ttl_sec_);
        }

        // Find out expired clients
        std::vector<UUID> expired_clients;
        for (auto it = client_ttl.begin(); it != client_ttl.end();) {
            auto remaining_sec =
                std::chrono::duration_cast<std::chrono::seconds>(it->second -
                                                                 now)
                    .count();
            if (it->second < now) {
                LOG(INFO) << "client_id=" << it->first
                          << ", action=client_expired"
                          << ", ttl_sec=" << client_live_ttl_sec_
                          << ", last_ping_was_sec_ago=" << -remaining_sec;
                expired_clients.push_back(it->first);
                it = client_ttl.erase(it);
            } else {
                // Log warning if TTL is getting close to expiration (within 10
                // seconds)
                if (remaining_sec < 10 && remaining_sec > 0) {
                    LOG(WARNING)
                        << "client_id=" << it->first << ", action=ttl_low"
                        << ", remaining_sec=" << remaining_sec
                        << ", ttl_sec=" << client_live_ttl_sec_
                        << " (client may expire soon if no ping received)";
                }
                ++it;
            }
        }

        // Update the client status to NEED_REMOUNT
        if (!expired_clients.empty()) {
            // Record which segments are unmounted, will be used in the commit
            // phase.
            std::vector<UUID> unmount_segments;
            std::vector<size_t> dec_capacities;
            std::vector<UUID> client_ids;
            std::vector<std::string> segment_names;
            std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
            {
                // Lock client_mutex and segment_mutex
                std::unique_lock<std::shared_mutex> lock(client_mutex_);
                for (auto& client_id : expired_clients) {
                    auto it = ok_client_.find(client_id);
                    if (it != ok_client_.end()) {
                        ok_client_.erase(it);
                        MasterMetricManager::instance().dec_active_clients();
                    }
                }

                ScopedSegmentAccess segment_access =
                    segment_manager_.getSegmentAccess();
                for (auto& client_id : expired_clients) {
                    std::vector<Segment> segments;
                    segment_access.GetClientSegments(client_id, segments);
                    for (auto& seg : segments) {
                        size_t metrics_dec_capacity = 0;
                        if (segment_access.PrepareUnmountSegment(
                                seg.id, metrics_dec_capacity) ==
                            ErrorCode::OK) {
                            unmount_segments.push_back(seg.id);
                            dec_capacities.push_back(metrics_dec_capacity);
                            client_ids.push_back(client_id);
                            segment_names.push_back(seg.name);
                        } else {
                            LOG(ERROR) << "client_id=" << client_id
                                       << ", segment_name=" << seg.name
                                       << ", "
                                          "error=prepare_unmount_expired_"
                                          "segment_failed";
                        }
                    }
                }
            }  // Release the mutex before long-running ClearInvalidHandles and
               // avoid deadlocks

            if (!unmount_segments.empty()) {
                ClearInvalidHandles();

                ScopedSegmentAccess segment_access =
                    segment_manager_.getSegmentAccess();
                for (size_t i = 0; i < unmount_segments.size(); i++) {
                    segment_access.CommitUnmountSegment(
                        unmount_segments[i], client_ids[i], dec_capacities[i]);
                    LOG(INFO) << "client_id=" << client_ids[i]
                              << ", segment_name=" << segment_names[i]
                              << ", action=unmount_expired_segment";
                }
            }
        }

        std::this_thread::sleep_for(
            std::chrono::milliseconds(kClientMonitorSleepMs));
    }
}

tl::expected<std::vector<uint8_t>, SerializationError>
MasterService::MetadataSerializer::Serialize() {
    msgpack::sbuffer sbuf;
    msgpack::packer<msgpack::sbuffer> packer(&sbuf);

    // Create top-level map with 4 fields: "shards", "discarded_replicas",
    // "replica_next_id", "oplog_sequence_id"
    packer.pack_map(4);

    // 1. Serialize metadata shards
    packer.pack("shards");

    // First count non-empty shards
    size_t valid_shards = 0;
    for (size_t i = 0; i < kNumShards; ++i) {
        if (!service_->metadata_shards_[i].metadata.empty()) {
            valid_shards++;
        }
    }

    // Create shards map
    packer.pack_map(valid_shards);

    // Iterate through all shards, serialize each shard independently
    for (size_t shard_idx = 0; shard_idx < kNumShards; ++shard_idx) {
        const auto& shard = service_->metadata_shards_[shard_idx];

        // Skip if shard is empty
        if (shard.metadata.empty()) {
            continue;
        }

        // Use shard index as key
        packer.pack(shard_idx);

        // Create independent serialization buffer for current shard
        msgpack::sbuffer shard_buffer;
        msgpack::packer<msgpack::sbuffer> shard_packer(&shard_buffer);

        // Serialize shard using SerializeShard
        auto result = SerializeShard(shard, shard_packer);
        if (!result) {
            return tl::make_unexpected(SerializationError(
                result.error().code,
                fmt::format("Failed to serialize shard {}: {}", shard_idx,
                            result.error().message)));
        }

        // Compress data
        std::vector<uint8_t> compressed_data =
            zstd_compress(reinterpret_cast<const uint8_t*>(shard_buffer.data()),
                          shard_buffer.size(), 3);
        // Write entire shard serialized data as binary to main buffer
        packer.pack_bin(compressed_data.size());
        packer.pack_bin_body(
            reinterpret_cast<const char*>(compressed_data.data()),
            compressed_data.size());
    }

    // 2. Serialize discarded_replicas
    packer.pack("discarded_replicas");
    auto dr_result = SerializeDiscardedReplicas(packer);
    if (!dr_result) {
        return tl::make_unexpected(SerializationError(
            dr_result.error().code, "Failed to serialize discarded_replicas: " +
                                        dr_result.error().message));
    }

    // 3. Serialize replica_next_id (static variable for generating unique
    // replica IDs)
    packer.pack("replica_next_id");
    packer.pack(static_cast<uint64_t>(Replica::next_id_.load()));

    // 4. Serialize oplog_sequence_id
    // This allows Standby to resume OpLog replay from the correct point after
    // restore. Note: snapshot_mutex_ in PersistState protects against concurrent
    // metadata/OpLog updates, ensuring this ID is consistent with the
    // serialized metadata.
    packer.pack("oplog_sequence_id");
    packer.pack(service_->oplog_manager_.GetLastSequenceId());

    return std::vector<uint8_t>(
        reinterpret_cast<const uint8_t*>(sbuf.data()),
        reinterpret_cast<const uint8_t*>(sbuf.data()) + sbuf.size());
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::Deserialize(
    const std::vector<uint8_t>& data) {
    // Parse MessagePack data directly
    msgpack::object_handle oh;
    try {
        oh = msgpack::unpack(reinterpret_cast<const char*>(data.data()),
                             data.size());
    } catch (const std::exception& e) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "Failed to unpack MessagePack data: " + std::string(e.what())));
    }

    const msgpack::object& obj = oh.get();

    // Check if it's a map
    if (obj.type != msgpack::type::MAP) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "Invalid MessagePack format: expected map"));
    }

    // Expected format: top-level map with "shards", "discarded_replicas",
    // and "replica_next_id"
    const msgpack::object* shards_obj = nullptr;
    const msgpack::object* discarded_replicas_obj = nullptr;
    const msgpack::object* replica_next_id_obj = nullptr;
    const msgpack::object* oplog_sequence_id_obj = nullptr;

    // Extract fields from top-level map
    for (uint32_t i = 0; i < obj.via.map.size; ++i) {
        const auto& key_obj = obj.via.map.ptr[i].key;
        if (key_obj.type == msgpack::type::STR) {
            std::string key = key_obj.as<std::string>();
            if (key == "shards") {
                shards_obj = &obj.via.map.ptr[i].val;
            } else if (key == "discarded_replicas") {
                discarded_replicas_obj = &obj.via.map.ptr[i].val;
            } else if (key == "replica_next_id") {
                replica_next_id_obj = &obj.via.map.ptr[i].val;
            } else if (key == "oplog_sequence_id") {
                oplog_sequence_id_obj = &obj.via.map.ptr[i].val;
            }
        }
    }

    // Check required "shards" field
    if (shards_obj == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "Missing 'shards' field"));
    }

    // Iterate and deserialize each shard
    for (uint32_t i = 0; i < shards_obj->via.map.size; ++i) {
        // Get shard index
        uint32_t shard_idx = shards_obj->via.map.ptr[i].key.as<uint32_t>();

        // Check shard index validity
        if (shard_idx >= kNumShards) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("Invalid shard index: {}", shard_idx)));
        }

        // Get shard binary data
        const msgpack::object& shard_data_obj = shards_obj->via.map.ptr[i].val;
        if (shard_data_obj.type != msgpack::type::BIN) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Invalid MessagePack format: expected binary data for shard"));
        }

        // Parse shard binary data directly, avoiding copy
        msgpack::object_handle shard_oh;
        try {
            auto decompressed_data = zstd_decompress(
                reinterpret_cast<const uint8_t*>(shard_data_obj.via.bin.ptr),
                shard_data_obj.via.bin.size);
            shard_oh = msgpack::unpack(
                reinterpret_cast<const char*>(decompressed_data.data()),
                decompressed_data.size());
        } catch (const std::exception& e) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Failed to unpack shard data: " + std::string(e.what())));
        }

        const msgpack::object& shard_obj = shard_oh.get();

        // Get shard reference and deserialize
        auto& shard = service_->metadata_shards_[shard_idx];
        auto result = DeserializeShard(shard_obj, shard);
        if (!result) {
            return tl::make_unexpected(SerializationError(
                result.error().code,
                fmt::format("Failed to deserialize shard {}: {}", shard_idx,
                            result.error().message)));
        }
    }

    // Deserialize discarded_replicas
    if (discarded_replicas_obj == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "Missing required field 'discarded_replicas' in snapshot data"));
    }
    auto dr_result = DeserializeDiscardedReplicas(*discarded_replicas_obj);
    if (!dr_result) {
        return tl::make_unexpected(
            SerializationError(dr_result.error().code,
                               "Failed to deserialize discarded_replicas: " +
                                   dr_result.error().message));
    }

    // Restore replica_next_id
    if (replica_next_id_obj == nullptr) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "Missing required field 'replica_next_id' in snapshot data"));
    }
    auto next_id = replica_next_id_obj->as<uint64_t>();
    Replica::next_id_.store(next_id);
    LOG(INFO) << "Restored Replica::next_id_ to " << next_id;

    // Restore oplog_sequence_id (if present)
    if (oplog_sequence_id_obj != nullptr) {
        auto last_seq_id = oplog_sequence_id_obj->as<uint64_t>();
        service_->oplog_manager_.SetInitialSequenceId(last_seq_id);
        LOG(INFO) << "Restored OpLog sequence_id to " << last_seq_id;
    } else {
        LOG(INFO)
            << "No oplog_sequence_id in snapshot, keeping current (probably 0)";
    }

    return {};
}

void MasterService::MetadataSerializer::Reset() {
    for (auto& shard : service_->metadata_shards_) {
        shard.metadata.clear();
    }
    {
        std::lock_guard lock(service_->discarded_replicas_mutex_);
        service_->discarded_replicas_.clear();
    }
    Replica::next_id_.store(1);
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::SerializeShard(const MetadataShard& shard,
                                                  MsgpackPacker& packer) const {
    // MetadataShard format: map with "metadata" field
    packer.pack_map(1);

    // Serialize metadata
    packer.pack("metadata");
    packer.pack_array(shard.metadata.size());

    // Sort keys to ensure consistent serialization order.
    // NOTE: sort may be slow for large shards.
    std::vector<std::string> sorted_keys;
    sorted_keys.reserve(shard.metadata.size());
    for (const auto& [key, metadata] : shard.metadata) {
        sorted_keys.push_back(key);
    }
    std::sort(sorted_keys.begin(), sorted_keys.end());

    for (const auto& key : sorted_keys) {
        const auto& metadata = shard.metadata.at(key);
        // Each metadata item format: [key, metadata_object]
        packer.pack_array(2);
        packer.pack(key);

        auto result = SerializeMetadata(metadata, packer);
        if (!result) {
            return tl::make_unexpected(SerializationError(
                result.error().code,
                fmt::format("Failed to serialize metadata for key '{}': {}",
                            key, result.error().message)));
        }
    }

    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::DeserializeShard(const msgpack::object& obj,
                                                    MetadataShard& shard) {
    if (obj.type != msgpack::type::MAP) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "Invalid shard format: expected map"));
    }

    const msgpack::object* metadata_array = nullptr;

    // Extract fields from shard map
    for (uint32_t i = 0; i < obj.via.map.size; ++i) {
        const auto& key_obj = obj.via.map.ptr[i].key;
        if (key_obj.type == msgpack::type::STR) {
            std::string field_key(key_obj.via.str.ptr, key_obj.via.str.size);
            if (field_key == "metadata") {
                metadata_array = &obj.via.map.ptr[i].val;
            }
        }
    }

    // Clear existing data
    shard.metadata.clear();

    // Deserialize metadata
    if (metadata_array == nullptr ||
        metadata_array->type != msgpack::type::ARRAY) {
        return tl::make_unexpected(
            SerializationError(ErrorCode::DESERIALIZE_FAIL,
                               "Missing or invalid 'metadata' field in shard"));
    }

    shard.metadata.reserve(metadata_array->via.array.size);

    for (uint32_t j = 0; j < metadata_array->via.array.size; ++j) {
        const msgpack::object& item = metadata_array->via.array.ptr[j];

        if (item.type != msgpack::type::ARRAY || item.via.array.size != 2) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                "Invalid metadata item format: expected [key, metadata]"));
        }

        std::string key = item.via.array.ptr[0].as<std::string>();
        const msgpack::object& value_obj = item.via.array.ptr[1];

        auto metadata_result = DeserializeMetadata(value_obj);
        if (!metadata_result) {
            LOG(ERROR) << "Failed to deserialize metadata for key: " << key
                       << ": " << metadata_result.error().message;
            continue;
        }

        auto metadata_ptr = std::move(metadata_result.value());
        auto [it, inserted] = shard.metadata.emplace(
            std::piecewise_construct, std::forward_as_tuple(std::move(key)),
            std::forward_as_tuple(
                metadata_ptr->client_id, metadata_ptr->put_start_time,
                metadata_ptr->size, metadata_ptr->PopReplicas(),
                metadata_ptr->soft_pin_timeout.has_value()));

        it->second.lease_timeout = metadata_ptr->lease_timeout;
        it->second.soft_pin_timeout = metadata_ptr->soft_pin_timeout;
    }

    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::SerializeMetadata(
    const MasterService::ObjectMetadata& metadata,
    MsgpackPacker& packer) const {
    // Pack ObjectMetadata using array structure for efficiency
    // Format: [client_id, put_start_time, size, lease_timeout,
    // has_soft_pin_timeout, soft_pin_timeout, replicas_count, replicas...]

    size_t array_size = 7;  // size, lease_timeout, has_soft_pin_timeout,
                            // soft_pin_timeout, replicas_count
    array_size += metadata.CountReplicas();  // One element per replica
    packer.pack_array(array_size);

    // Serialize client_id
    std::string client_id = UuidToString(metadata.client_id);
    packer.pack(client_id);

    // Serialize put_start_time (convert to timestamp)
    auto put_start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                              metadata.put_start_time.time_since_epoch())
                              .count();
    packer.pack(put_start_time);

    // Serialize size
    packer.pack(static_cast<uint64_t>(metadata.size));

    // Serialize lease_timeout (convert to timestamp)
    auto lease_timestamp =
        std::chrono::duration_cast<std::chrono::milliseconds>(
            metadata.lease_timeout.time_since_epoch())
            .count();
    packer.pack(lease_timestamp);

    // Serialize soft_pin_timeout (if exists)
    if (metadata.soft_pin_timeout.has_value()) {
        packer.pack(true);  // Mark soft_pin_timeout exists
        auto soft_pin_timestamp =
            std::chrono::duration_cast<std::chrono::milliseconds>(
                metadata.soft_pin_timeout.value().time_since_epoch())
                .count();
        packer.pack(soft_pin_timestamp);
    } else {
        packer.pack(false);        // Mark soft_pin_timeout does not exist
        packer.pack(uint64_t(0));  // Placeholder
    }

    // Serialize replicas count
    packer.pack(static_cast<uint32_t>(metadata.CountReplicas()));

    // Serialize replicas
    for (const auto& replica : metadata.GetAllReplicas()) {
        auto result = Serializer<Replica>::serialize(
            replica, service_->segment_manager_.getView(), packer);
        if (!result) {
            return tl::unexpected(result.error());
        }
    }

    return {};
}

tl::expected<std::unique_ptr<MasterService::ObjectMetadata>, SerializationError>
MasterService::MetadataSerializer::DeserializeMetadata(
    const msgpack::object& obj) const {
    // Check if input is a valid array
    if (obj.type != msgpack::type::ARRAY) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize ObjectMetadata state is not an array"));
    }

    // Need at least 7 elements: client_id, put_start_time, size, lease_timeout,
    // has_soft_pin_timeout, soft_pin_timeout, replicas_count
    if (obj.via.array.size < 7) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize ObjectMetadata array size is too small"));
    }

    msgpack::object* array = obj.via.array.ptr;
    uint32_t index = 0;

    // Deserialize client_id string
    std::string client_id_str = array[index++].as<std::string>();
    UUID client_id;
    StringToUuid(client_id_str, client_id);

    // Deserialize put_start_time
    uint64_t put_start_time_timestamp = array[index++].as<uint64_t>();

    // Deserialize size
    auto size = static_cast<size_t>(array[index++].as<uint64_t>());

    // Deserialize lease_timeout
    uint64_t lease_timestamp = array[index++].as<uint64_t>();

    // Deserialize soft_pin_timeout flag
    bool has_soft_pin_timeout = array[index++].as<bool>();

    // Deserialize soft_pin_timeout value
    uint64_t soft_pin_timestamp = array[index++].as<uint64_t>();

    // Deserialize replicas count
    uint32_t replicas_count = array[index++].as<uint32_t>();

    // Check if array size matches replicas_count
    if (obj.via.array.size != 7 + replicas_count) {
        return tl::unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL,
            "deserialize ObjectMetadata array size mismatch"));
    }

    // Deserialize replicas
    std::vector<Replica> replicas;
    replicas.reserve(replicas_count);

    for (uint32_t i = 0; i < replicas_count; i++) {
        auto result = Serializer<Replica>::deserialize(
            array[index++], service_->segment_manager_.getView());
        if (!result) {
            return tl::unexpected(result.error());
        }
        replicas.emplace_back(std::move(*result.value()));
    }

    // Create ObjectMetadata instance
    bool enable_soft_pin = has_soft_pin_timeout;
    auto metadata = std::make_unique<ObjectMetadata>(
        client_id,
        std::chrono::system_clock::time_point(
            std::chrono::milliseconds(put_start_time_timestamp)),
        size, std::move(replicas), enable_soft_pin);
    metadata->lease_timeout = std::chrono::system_clock::time_point(
        std::chrono::milliseconds(lease_timestamp));

    // Set soft_pin_timeout (if exists)
    if (has_soft_pin_timeout) {
        metadata->soft_pin_timeout.emplace(
            std::chrono::system_clock::time_point(
                std::chrono::milliseconds(soft_pin_timestamp)));
    }

    return metadata;
}

std::string MasterService::FormatTimestamp(
    const std::chrono::system_clock::time_point& tp) {
    auto time_t = std::chrono::system_clock::to_time_t(tp);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&time_t), "%Y%m%d_%H%M%S");

    // Add milliseconds to ensure uniqueness
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                  tp.time_since_epoch()) %
              1000;

    ss << "_" << std::setfill('0') << std::setw(3) << ms.count();

    return ss.str();
}

tl::expected<UUID, ErrorCode> MasterService::CreateCopyTask(
    const std::string& key, const std::vector<std::string>& targets) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    if (targets.empty()) {
        LOG(ERROR) << "key=" << key << ", error=empty_targets";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }
    MetadataAccessorRO accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    ScopedSegmentAccess segment_accessor = segment_manager_.getSegmentAccess();
    for (const auto& target : targets) {
        if (!segment_accessor.ExistsSegmentName(target)) {
            LOG(ERROR) << "key=" << key << ", target_segment=" << target
                       << ", error=target_segment_not_mounted";
            return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
        }
    }

    const auto& metadata = accessor.Get();
    const auto& segment_names = metadata.GetReplicaSegmentNames();
    if (segment_names.empty()) {
        LOG(ERROR) << "key=" << key << ", error=no_valid_source_replicas";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    // Randomly pick a segment from the source replicas
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<size_t> dis(0, segment_names.size() - 1);
    std::string selected_source_segment = segment_names[dis(gen)];
    UUID select_client;
    ErrorCode error = segment_accessor.GetClientIdBySegmentName(
        selected_source_segment, select_client);
    if (error != ErrorCode::OK) {
        LOG(ERROR) << "key=" << key
                   << ", segment_name=" << selected_source_segment
                   << ", error=client_id_not_found";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }
    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_COPY>(
            select_client, {.key = key,
                            .source = selected_source_segment,
                            .targets = targets});
}

tl::expected<UUID, ErrorCode> MasterService::CreateMoveTask(
    const std::string& key, const std::string& source,
    const std::string& target) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    MetadataAccessorRO accessor(this, key);
    if (!accessor.Exists()) {
        VLOG(1) << "key=" << key << ", info=object_not_found";
        return tl::make_unexpected(ErrorCode::OBJECT_NOT_FOUND);
    }

    if (source == target) {
        LOG(ERROR) << "key=" << key << ", source_segment=" << source
                   << ", target_segment=" << target
                   << ", error=source_target_segments_are_same";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    ScopedSegmentAccess segment_accessor = segment_manager_.getSegmentAccess();
    if (!segment_accessor.ExistsSegmentName(target)) {
        LOG(ERROR) << "key=" << key << ", target_segment=" << target
                   << ", error=target_segment_not_mounted";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    const auto& metadata = accessor.Get();
    const auto& segment_names = metadata.GetReplicaSegmentNames();
    if (std::find(segment_names.begin(), segment_names.end(), source) ==
        segment_names.end()) {
        LOG(ERROR) << "key=" << key << ", source_segment=" << source
                   << ", error=source_segment_not_found";
        return tl::make_unexpected(ErrorCode::INVALID_PARAMS);
    }

    UUID select_client;
    ErrorCode error =
        segment_accessor.GetClientIdBySegmentName(source, select_client);

    if (error != ErrorCode::OK) {
        LOG(ERROR) << "key=" << key << ", segment_name=" << source
                   << ", error=client_id_not_found";
        return tl::make_unexpected(ErrorCode::INTERNAL_ERROR);
    }

    return task_manager_.get_write_access()
        .submit_task_typed<TaskType::REPLICA_MOVE>(
            select_client, {.key = key, .source = source, .target = target});
}

tl::expected<QueryTaskResponse, ErrorCode> MasterService::QueryTask(
    const UUID& task_id) {
    const auto& task_option =
        task_manager_.get_read_access().find_task_by_id(task_id);
    if (!task_option.has_value()) {
        LOG(ERROR) << "task_id=" << task_id << ", error=task_not_found";
        return tl::make_unexpected(ErrorCode::TASK_NOT_FOUND);
    }
    return QueryTaskResponse(task_option.value());
}

tl::expected<std::vector<TaskAssignment>, ErrorCode> MasterService::FetchTasks(
    const UUID& client_id, size_t batch_size) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    const auto& tasks =
        task_manager_.get_write_access().pop_tasks(client_id, batch_size);
    std::vector<TaskAssignment> assignments;
    for (const auto& task : tasks) {
        assignments.emplace_back(task);
    }
    return assignments;
}

tl::expected<void, ErrorCode> MasterService::MarkTaskToComplete(
    const UUID& client_id, const TaskCompleteRequest& request) {
    std::shared_lock<std::shared_mutex> shared_lock(snapshot_mutex_);
    auto write_access = task_manager_.get_write_access();
    ErrorCode err = write_access.complete_task(client_id, request.id,
                                               request.status, request.message);
    if (err != ErrorCode::OK) {
        LOG(ERROR) << "task_id=" << request.id
                   << ", error=complete_task_failed";
        return tl::make_unexpected(err);
    }
    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::SerializeDiscardedReplicas(
    MsgpackPacker& packer) const {
    std::lock_guard lock(service_->discarded_replicas_mutex_);

    // Serialize as array: [count, item1, item2, ...]
    packer.pack_array(service_->discarded_replicas_.size());

    for (const auto& item : service_->discarded_replicas_) {
        // Each item: [ttl_timestamp, mem_size, replica_count, replica1,
        // replica2, ...]
        auto ttl_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                          item.ttl_.time_since_epoch())
                          .count();

        packer.pack_array(3 + item.replicas_.size());
        packer.pack(ttl_ms);          // ttl timestamp
        packer.pack(item.mem_size_);  // mem_size
        packer.pack(
            static_cast<uint32_t>(item.replicas_.size()));  // replica count

        // Serialize each replica
        for (const auto& replica : item.replicas_) {
            auto result = Serializer<Replica>::serialize(
                replica, service_->segment_manager_.getView(), packer);
            if (!result) {
                return tl::unexpected(result.error());
            }
        }
    }

    return {};
}

tl::expected<void, SerializationError>
MasterService::MetadataSerializer::DeserializeDiscardedReplicas(
    const msgpack::object& obj) {
    if (obj.type != msgpack::type::ARRAY) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::DESERIALIZE_FAIL, "discarded_replicas: expected array"));
    }

    std::list<DiscardedReplicas> temp_list;

    for (uint32_t i = 0; i < obj.via.array.size; ++i) {
        const msgpack::object& item_obj = obj.via.array.ptr[i];

        if (item_obj.type != msgpack::type::ARRAY ||
            item_obj.via.array.size < 3) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format("Invalid discarded_replicas item at index {}: "
                            "expected array with at least 3 elements",
                            i)));
        }

        const msgpack::object* item_array = item_obj.via.array.ptr;

        // Deserialize ttl
        uint64_t ttl_ms = item_array[0].as<uint64_t>();
        auto ttl = std::chrono::system_clock::time_point(
            std::chrono::milliseconds(ttl_ms));

        // Deserialize mem_size
        uint64_t mem_size = item_array[1].as<uint64_t>();

        // Deserialize replica count
        uint32_t replica_count = item_array[2].as<uint32_t>();

        if (item_obj.via.array.size != 3 + replica_count) {
            return tl::make_unexpected(SerializationError(
                ErrorCode::DESERIALIZE_FAIL,
                fmt::format(
                    "Discarded replicas item size mismatch at index {}: "
                    "expected {} elements, got {}",
                    i, 3 + replica_count, item_obj.via.array.size)));
        }

        // Deserialize replicas
        std::vector<Replica> replicas;
        replicas.reserve(replica_count);

        for (uint32_t j = 0; j < replica_count; ++j) {
            auto replica_result = Serializer<Replica>::deserialize(
                item_array[3 + j], service_->segment_manager_.getView());
            if (!replica_result) {
                return tl::make_unexpected(SerializationError(
                    ErrorCode::DESERIALIZE_FAIL,
                    fmt::format("Failed to deserialize replica {} in "
                                "discarded_replicas item {}: {}",
                                j, i, replica_result.error().message)));
            }
            replicas.emplace_back(std::move(*replica_result.value()));
        }

        // Create DiscardedReplicas and manually set mem_size_
        temp_list.emplace_back(std::move(replicas), ttl);
        // Set the deserialized mem_size
        temp_list.back().mem_size_ = mem_size;
    }

    // Move deserialized items to service's discarded_replicas_
    if (!temp_list.empty()) {
        std::lock_guard lock(service_->discarded_replicas_mutex_);
        service_->discarded_replicas_ = std::move(temp_list);
    }

    return {};
}

// ============================================================================
// Snapshot Daemon Implementation (ETCD Storage)
// ============================================================================

bool MasterService::StartSnapshotDaemon() {
    std::lock_guard<std::mutex> lock(snapshot_daemon_mutex_);

    if (snapshot_daemon_pid_ != -1) {
        LOG(WARNING) << "[SnapshotDaemon] Daemon already running, pid="
                     << snapshot_daemon_pid_;
        return true;
    }

    auto start_time = std::chrono::steady_clock::now();
    LOG(INFO) << "[SnapshotDaemon] Starting snapshot daemon";

    // Create socket path
    snapshot_daemon_socket_path_ =
        snapshot_backup_dir_ + "/snapshot_daemon.sock";

    // Ensure snapshot directory exists
    fs::path snap_dir(snapshot_backup_dir_);
    std::error_code ec;
    fs::create_directories(snap_dir, ec);
    if (ec) {
        LOG(ERROR) << "[SnapshotDaemon] Failed to create snapshot directory: "
                   << ec.message();
        return false;
    }

    // Remove old socket file if exists
    unlink(snapshot_daemon_socket_path_.c_str());

    // Find snapshot_uploader_daemon executable
    char exe_path[PATH_MAX];
    ssize_t len = readlink("/proc/self/exe", exe_path, sizeof(exe_path) - 1);
    if (len == -1) {
        LOG(ERROR) << "[SnapshotDaemon] Failed to get executable path";
        return false;
    }
    exe_path[len] = '\0';

    std::string master_path(exe_path);
    size_t pos = master_path.rfind('/');
    std::string daemon_path;

    // Try same directory first
    std::string same_dir =
        master_path.substr(0, pos + 1) + "snapshot_uploader_daemon";
    if (access(same_dir.c_str(), X_OK) == 0) {
        daemon_path = same_dir;
    } else {
        // Try ../src/ path for test binaries
        std::string parent_dir = master_path.substr(0, pos);
        size_t pos2 = parent_dir.rfind('/');
        std::string test_relative =
            parent_dir.substr(0, pos2 + 1) + "src/snapshot_uploader_daemon";
        if (access(test_relative.c_str(), X_OK) == 0) {
            daemon_path = test_relative;
        } else {
            LOG(ERROR) << "[SnapshotDaemon] Cannot find "
                          "snapshot_uploader_daemon executable. "
                       << "Tried: " << same_dir << " and " << test_relative;
            return false;
        }
    }

    LOG(INFO) << "[SnapshotDaemon] Found daemon at: " << daemon_path;

    // Fork and exec daemon
    pid_t pid = fork();
    if (pid == -1) {
        LOG(ERROR) << "[SnapshotDaemon] Failed to fork: " << strerror(errno);
        return false;
    }

    if (pid == 0) {
        // Child process - exec daemon
        char* args[] = {const_cast<char*>(daemon_path.c_str()),
                        const_cast<char*>(etcd_endpoints_.c_str()),
                        const_cast<char*>(snapshot_daemon_socket_path_.c_str()),
                        nullptr};
        execv(daemon_path.c_str(), args);

        // If we reach here, exec failed
        LOG(ERROR) << "[SnapshotDaemon] execv failed: " << strerror(errno);
        _exit(127);
    }

    // Parent process - wait for daemon to be ready
    snapshot_daemon_pid_ = pid;
    LOG(INFO) << "[SnapshotDaemon] Daemon process started, pid=" << pid;

    // Wait for socket to be created (with timeout)
    int retry_count = 0;
    const int max_retries = 50;  // 5 seconds timeout
    while (retry_count < max_retries) {
        if (access(snapshot_daemon_socket_path_.c_str(), F_OK) == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        retry_count++;
    }

    if (retry_count >= max_retries) {
        LOG(ERROR) << "[SnapshotDaemon] Timeout waiting for daemon socket";
        kill(snapshot_daemon_pid_, SIGKILL);
        waitpid(snapshot_daemon_pid_, nullptr, 0);
        snapshot_daemon_pid_ = -1;
        return false;
    }

    // Socket exists, daemon is ready (no need to test connect)
    // First actual upload will verify connection works
    auto startup_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                            std::chrono::steady_clock::now() - start_time)
                            .count();
    LOG(INFO) << "[SnapshotDaemon] Daemon ready, startup_time=" << startup_time
              << "ms";

    return true;
}

void MasterService::StopSnapshotDaemon() {
    std::lock_guard<std::mutex> lock(snapshot_daemon_mutex_);

    if (snapshot_daemon_pid_ == -1) {
        return;
    }

    LOG(INFO) << "[SnapshotDaemon] Stopping daemon, pid="
              << snapshot_daemon_pid_;

    // Send SIGTERM to daemon
    if (kill(snapshot_daemon_pid_, SIGTERM) == 0) {
        // Wait up to 5 seconds for graceful shutdown
        int retry = 0;
        while (retry < 50) {
            int status;
            pid_t result = waitpid(snapshot_daemon_pid_, &status, WNOHANG);
            if (result == snapshot_daemon_pid_) {
                LOG(INFO) << "[SnapshotDaemon] Daemon terminated gracefully";
                snapshot_daemon_pid_ = -1;
                unlink(snapshot_daemon_socket_path_.c_str());
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            retry++;
        }

        // Force kill if not terminated
        LOG(WARNING) << "[SnapshotDaemon] Force killing daemon";
        kill(snapshot_daemon_pid_, SIGKILL);
        // Wait for process to actually exit after SIGKILL
        int kill_status;
        pid_t kill_result = waitpid(snapshot_daemon_pid_, &kill_status, 0);
        if (kill_result == -1) {
            LOG(ERROR) << "[SnapshotDaemon] waitpid failed after SIGKILL: "
                       << strerror(errno);
        } else {
            LOG(INFO) << "[SnapshotDaemon] Daemon force killed";
        }
    }

    snapshot_daemon_pid_ = -1;
    // Remove socket file (may fail if still in use, ignore error)
    if (unlink(snapshot_daemon_socket_path_.c_str()) != 0) {
        LOG(WARNING) << "[SnapshotDaemon] Failed to remove socket file: "
                     << strerror(errno);
    }
    LOG(INFO) << "[SnapshotDaemon] Daemon stopped";
}

tl::expected<void, SerializationError> MasterService::UploadViaDaemon(
    const std::vector<std::pair<std::string, std::string>>& files,
    const std::string& snapshot_id) {
    auto start_time = std::chrono::steady_clock::now();
    LOG(INFO) << "[SnapshotDaemon] Batch upload START, num_files="
              << files.size() << ", snapshot_id=" << snapshot_id;

    std::lock_guard<std::mutex> lock(snapshot_daemon_mutex_);

    if (snapshot_daemon_pid_ == -1) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL, "Daemon process not running"));
    }

    // Create new connection for each request (daemon uses short-lived
    // connections)
    int client_socket = socket(AF_UNIX, SOCK_STREAM, 0);
    if (client_socket == -1) {
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL,
            "Failed to create client socket: " + std::string(strerror(errno))));
    }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, snapshot_daemon_socket_path_.c_str(),
            sizeof(addr.sun_path) - 1);

    if (connect(client_socket, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
        close(client_socket);
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL,
            "Failed to connect to daemon: " + std::string(strerror(errno))));
    }

    LOG(INFO) << "[SnapshotDaemon] Connected to daemon";

    // Send request
    // Protocol:
    // [num_files:4]
    //   [key_len:4][key:N][payload_type:1][payload_len:4][payload:M]...
    // payload_type: 0 = inline data, 1 = local file path
    uint32_t num_files = files.size();
    // Helper lambda for writing with retry on partial writes
    auto write_all = [&](const void* buf, size_t count) -> bool {
        size_t written = 0;
        while (written < count) {
            ssize_t n =
                write(client_socket, static_cast<const char*>(buf) + written,
                      count - written);
            if (n <= 0) {
                return false;
            }
            written += n;
        }
        return true;
    };

    if (!write_all(&num_files, sizeof(num_files))) {
        close(client_socket);
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL,
            "Failed to write num_files: " + std::string(strerror(errno))));
    }

    for (const auto& [key, local_path] : files) {
        // Write key
        uint32_t key_len = key.size();
        if (!write_all(&key_len, sizeof(key_len))) {
            close(client_socket);
            return tl::make_unexpected(SerializationError(
                ErrorCode::PERSISTENT_FAIL,
                "Failed to write key_len: " + std::string(strerror(errno))));
        }

        if (!write_all(key.data(), key_len)) {
            close(client_socket);
            return tl::make_unexpected(SerializationError(
                ErrorCode::PERSISTENT_FAIL,
                "Failed to write key: " + std::string(strerror(errno))));
        }

        uint8_t payload_type = 1;
        if (!write_all(&payload_type, sizeof(payload_type))) {
            close(client_socket);
            return tl::make_unexpected(SerializationError(
                ErrorCode::PERSISTENT_FAIL, "Failed to write payload_type: " +
                                                std::string(strerror(errno))));
        }

        uint32_t payload_len = local_path.size();
        if (!write_all(&payload_len, sizeof(payload_len))) {
            close(client_socket);
            return tl::make_unexpected(SerializationError(
                ErrorCode::PERSISTENT_FAIL, "Failed to write payload_len: " +
                                                std::string(strerror(errno))));
        }

        if (payload_len > 0) {
            if (!write_all(local_path.data(), payload_len)) {
                close(client_socket);
                return tl::make_unexpected(
                    SerializationError(ErrorCode::PERSISTENT_FAIL,
                                       "Failed to write payload: " +
                                           std::string(strerror(errno))));
            }
        }

        LOG(INFO) << "[SnapshotDaemon] Sent request: key=" << key
                  << ", path=" << local_path;
    }

    auto send_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now() - start_time)
                         .count();
    LOG(INFO) << "[SnapshotDaemon] All requests sent, send_time=" << send_time
              << "ms";

    // Read response
    // Protocol: [status:4][error_len:4][error_msg:N]
    uint32_t status;
    if (read(client_socket, &status, sizeof(status)) != sizeof(status)) {
        close(client_socket);
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL,
            "Failed to read status: " + std::string(strerror(errno))));
    }

    uint32_t error_len;
    if (read(client_socket, &error_len, sizeof(error_len)) !=
        sizeof(error_len)) {
        close(client_socket);
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL,
            "Failed to read error_len: " + std::string(strerror(errno))));
    }

    // Sanity check: limit error message size to 10MB
    constexpr uint32_t MAX_ERROR_LEN = 10 * 1024 * 1024;
    if (error_len > MAX_ERROR_LEN) {
        close(client_socket);
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL,
            "Error message too large: " + std::to_string(error_len)));
    }

    std::string error_msg;
    if (error_len > 0) {
        error_msg.resize(error_len);
        if (read(client_socket, &error_msg[0], error_len) !=
            static_cast<ssize_t>(error_len)) {
            close(client_socket);
            return tl::make_unexpected(SerializationError(
                ErrorCode::PERSISTENT_FAIL,
                "Failed to read error_msg: " + std::string(strerror(errno))));
        }
    }

    // Close connection
    close(client_socket);

    auto total_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::steady_clock::now() - start_time)
                          .count();

    if (status != 0) {
        LOG(ERROR) << "[SnapshotDaemon] Batch upload FAILED, total_time="
                   << total_time << "ms, error=" << error_msg;
        return tl::make_unexpected(SerializationError(
            ErrorCode::PERSISTENT_FAIL, "Daemon upload failed: " + error_msg));
    }

    LOG(INFO) << "[SnapshotDaemon] Batch upload SUCCESS, total_time="
              << total_time << "ms";
    return {};
}

OpLogManager& MasterService::GetOpLogManager() { return oplog_manager_; }

// SetReplicationService removed - using etcd-based OpLog sync instead

}  // namespace mooncake
