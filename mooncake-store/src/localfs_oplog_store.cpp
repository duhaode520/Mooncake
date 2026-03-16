#include "localfs_oplog_store.h"

#include <glog/logging.h>

#include <algorithm>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>

#include <fcntl.h>
#include <unistd.h>

#include "oplog_serializer.h"
#include "polling_oplog_change_notifier.h"

namespace fs = std::filesystem;

namespace mooncake {

// ============================================================
// Constructor / Destructor
// ============================================================

LocalFsOpLogStore::LocalFsOpLogStore(const std::string& cluster_id,
                                     const std::string& root_dir,
                                     bool enable_batch_write,
                                     int poll_interval_ms)
    : cluster_id_(cluster_id),
      root_dir_(root_dir),
      cluster_dir_(root_dir + "/" + cluster_id),
      enable_batch_write_(enable_batch_write),
      poll_interval_ms_(poll_interval_ms) {}

LocalFsOpLogStore::~LocalFsOpLogStore() {
    if (batch_write_running_.load()) {
        batch_write_running_.store(false);
        cv_batch_updated_.notify_all();
        if (batch_write_thread_.joinable()) {
            batch_write_thread_.join();
        }
    }
    // Final flush of any remaining entries
    if (!pending_batch_.empty()) {
        FlushBatch();
    }
    // Notify any sync waiters so they don't block forever
    cv_sync_completed_.notify_all();
}

// ============================================================
// Init
// ============================================================

ErrorCode LocalFsOpLogStore::Init() {
    try {
        fs::create_directories(SegmentsDir());
        fs::create_directories(SnapshotsDir());
    } catch (const fs::filesystem_error& e) {
        LOG(ERROR) << "LocalFsOpLogStore::Init: failed to create directories: "
                   << e.what();
        return ErrorCode::INTERNAL_ERROR;
    }

    CleanupTempFiles();

    if (enable_batch_write_) {
        // Writer: initialize latest file if not exists
        std::string latest_path = LatestFilePath();
        if (!fs::exists(latest_path)) {
            auto err = AtomicWriteFile(latest_path, "0");
            if (err != ErrorCode::OK) {
                LOG(ERROR) << "LocalFsOpLogStore::Init: failed to create "
                              "latest file";
                return err;
            }
        }

        // Start batch write thread
        batch_write_running_.store(true);
        batch_write_thread_ =
            std::thread(&LocalFsOpLogStore::BatchWriteThread, this);
    }

    return ErrorCode::OK;
}

// ============================================================
// Path helpers
// ============================================================

std::string LocalFsOpLogStore::SegmentsDir() const {
    return cluster_dir_ + "/segments";
}

std::string LocalFsOpLogStore::SnapshotsDir() const {
    return cluster_dir_ + "/snapshots";
}

std::string LocalFsOpLogStore::LatestFilePath() const {
    return cluster_dir_ + "/latest";
}

std::string LocalFsOpLogStore::BuildSegmentFilename(uint64_t min_seq,
                                                    uint64_t max_seq) const {
    char buf[128];
    snprintf(buf, sizeof(buf), "seg_%020lu_%020lu", min_seq, max_seq);
    return SegmentsDir() + "/" + buf;
}

std::string LocalFsOpLogStore::BuildSnapshotPath(
    const std::string& snapshot_id) const {
    return SnapshotsDir() + "/" + snapshot_id;
}

// ============================================================
// Atomic file write
// ============================================================

ErrorCode LocalFsOpLogStore::AtomicWriteFile(const std::string& target_path,
                                             const std::string& content) {
    return AtomicWriteFile(target_path, content.data(), content.size());
}

ErrorCode LocalFsOpLogStore::AtomicWriteFile(const std::string& target_path,
                                             const void* data, size_t size) {
    std::string tmp_path = target_path + ".tmp";
    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        LOG(ERROR) << "AtomicWriteFile: open failed: " << tmp_path
                   << ", errno=" << errno;
        return ErrorCode::INTERNAL_ERROR;
    }

    const char* ptr = static_cast<const char*>(data);
    size_t remaining = size;
    while (remaining > 0) {
        ssize_t written = ::write(fd, ptr, remaining);
        if (written < 0) {
            if (errno == EINTR) continue;
            LOG(ERROR) << "AtomicWriteFile: write failed: " << tmp_path
                       << ", errno=" << errno;
            ::close(fd);
            ::unlink(tmp_path.c_str());
            return ErrorCode::INTERNAL_ERROR;
        }
        ptr += written;
        remaining -= static_cast<size_t>(written);
    }

    if (::fsync(fd) != 0) {
        LOG(ERROR) << "AtomicWriteFile: fsync failed: " << tmp_path
                   << ", errno=" << errno;
        ::close(fd);
        ::unlink(tmp_path.c_str());
        return ErrorCode::INTERNAL_ERROR;
    }
    ::close(fd);

    if (::rename(tmp_path.c_str(), target_path.c_str()) != 0) {
        LOG(ERROR) << "AtomicWriteFile: rename failed: " << tmp_path << " -> "
                   << target_path << ", errno=" << errno;
        ::unlink(tmp_path.c_str());
        return ErrorCode::INTERNAL_ERROR;
    }

    return ErrorCode::OK;
}

// ============================================================
// Cleanup temp files
// ============================================================

void LocalFsOpLogStore::CleanupTempFiles() {
    try {
        for (auto& dir : {SegmentsDir(), cluster_dir_}) {
            if (!fs::exists(dir)) continue;
            for (auto& entry : fs::directory_iterator(dir)) {
                if (entry.path().extension() == ".tmp") {
                    fs::remove(entry.path());
                    LOG(INFO) << "Cleaned up temp file: " << entry.path();
                }
            }
        }
    } catch (const fs::filesystem_error& e) {
        LOG(WARNING) << "CleanupTempFiles: " << e.what();
    }
}

// ============================================================
// Snapshot ID validation
// ============================================================

bool LocalFsOpLogStore::ValidateSnapshotId(const std::string& snapshot_id) {
    if (snapshot_id.empty()) return false;
    if (snapshot_id.find('/') != std::string::npos) return false;
    if (snapshot_id.find("..") != std::string::npos) return false;
    if (snapshot_id.find('\0') != std::string::npos) return false;
    return true;
}

// ============================================================
// Segment filename parsing
// ============================================================

bool LocalFsOpLogStore::ParseSegmentFilename(const std::string& filename,
                                             uint64_t& min_seq,
                                             uint64_t& max_seq) {
    // Expected format: seg_XXXXXXXXXXXXXXXXXXXX_XXXXXXXXXXXXXXXXXXXX
    if (filename.size() < 45) return false;
    if (filename.substr(0, 4) != "seg_") return false;
    try {
        min_seq = std::stoull(filename.substr(4, 20));
        max_seq = std::stoull(filename.substr(25, 20));
        return true;
    } catch (...) {
        return false;
    }
}

std::vector<LocalFsOpLogStore::SegmentInfo> LocalFsOpLogStore::ListSegments()
    const {
    std::vector<SegmentInfo> segments;
    std::string seg_dir = SegmentsDir();
    if (!fs::exists(seg_dir)) return segments;

    try {
        for (auto& entry : fs::directory_iterator(seg_dir)) {
            if (entry.path().extension() == ".tmp") continue;
            std::string filename = entry.path().filename().string();
            uint64_t min_seq = 0, max_seq = 0;
            if (ParseSegmentFilename(filename, min_seq, max_seq)) {
                segments.push_back({filename, min_seq, max_seq});
            }
        }
    } catch (const fs::filesystem_error& e) {
        LOG(WARNING) << "ListSegments: " << e.what();
    }

    // Sort by min_seq
    std::sort(segments.begin(), segments.end(),
              [](const SegmentInfo& a, const SegmentInfo& b) {
                  return a.min_seq < b.min_seq;
              });
    return segments;
}

// ============================================================
// Segment file I/O
// ============================================================

ErrorCode LocalFsOpLogStore::WriteSegmentFile(
    const std::vector<BatchEntry>& entries) {
    if (entries.empty()) return ErrorCode::OK;

    uint64_t min_seq = entries.front().sequence_id;
    uint64_t max_seq = entries.back().sequence_id;

    // Build file content: header + length-prefixed entries
    std::string content;

    // Reserve approximate size
    size_t estimated_size = kSegmentHeaderSize;
    for (const auto& e : entries) {
        estimated_size += sizeof(uint32_t) + e.serialized_value.size();
    }
    content.reserve(estimated_size);

    // Header
    SegmentHeader header;
    std::memcpy(header.magic, kSegmentMagic, 4);
    header.version = kSegmentVersion;
    header.min_seq = min_seq;
    header.max_seq = max_seq;
    header.entry_count = static_cast<uint32_t>(entries.size());
    header.reserved = 0;

    content.append(reinterpret_cast<const char*>(&header), sizeof(header));

    // Entries: [uint32_t length][serialized_value]
    for (const auto& e : entries) {
        uint32_t len = static_cast<uint32_t>(e.serialized_value.size());
        content.append(reinterpret_cast<const char*>(&len), sizeof(len));
        content.append(e.serialized_value);
    }

    std::string filepath = BuildSegmentFilename(min_seq, max_seq);
    return AtomicWriteFile(filepath, content.data(), content.size());
}

ErrorCode LocalFsOpLogStore::ReadSegmentHeader(const std::string& filepath,
                                               SegmentHeader& header) {
    std::ifstream f(filepath, std::ios::binary);
    if (!f) {
        LOG(ERROR) << "ReadSegmentHeader: cannot open " << filepath;
        return ErrorCode::INTERNAL_ERROR;
    }

    f.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (!f || static_cast<size_t>(f.gcount()) < sizeof(header)) {
        LOG(ERROR) << "ReadSegmentHeader: truncated header in " << filepath;
        return ErrorCode::INTERNAL_ERROR;
    }

    if (std::memcmp(header.magic, kSegmentMagic, 4) != 0) {
        LOG(ERROR) << "ReadSegmentHeader: bad magic in " << filepath;
        return ErrorCode::INTERNAL_ERROR;
    }

    if (header.version != kSegmentVersion) {
        LOG(ERROR) << "ReadSegmentHeader: unsupported version "
                   << header.version << " in " << filepath;
        return ErrorCode::INTERNAL_ERROR;
    }

    return ErrorCode::OK;
}

ErrorCode LocalFsOpLogStore::ReadSegmentEntries(
    const std::string& filepath, std::vector<OpLogEntry>& entries) {
    std::ifstream f(filepath, std::ios::binary);
    if (!f) {
        LOG(ERROR) << "ReadSegmentEntries: cannot open " << filepath;
        return ErrorCode::INTERNAL_ERROR;
    }

    SegmentHeader header;
    f.read(reinterpret_cast<char*>(&header), sizeof(header));
    if (!f || static_cast<size_t>(f.gcount()) < sizeof(header)) {
        LOG(ERROR) << "ReadSegmentEntries: truncated header in " << filepath;
        return ErrorCode::INTERNAL_ERROR;
    }

    if (std::memcmp(header.magic, kSegmentMagic, 4) != 0) {
        LOG(ERROR) << "ReadSegmentEntries: bad magic in " << filepath;
        return ErrorCode::INTERNAL_ERROR;
    }

    if (header.version != kSegmentVersion) {
        LOG(ERROR) << "ReadSegmentEntries: unsupported version "
                   << header.version << " in " << filepath;
        return ErrorCode::INTERNAL_ERROR;
    }

    for (uint32_t i = 0; i < header.entry_count; ++i) {
        uint32_t len = 0;
        f.read(reinterpret_cast<char*>(&len), sizeof(len));
        if (!f) {
            LOG(ERROR) << "ReadSegmentEntries: truncated entry length at "
                       << "entry " << i << " in " << filepath;
            return ErrorCode::INTERNAL_ERROR;
        }

        std::string buf(len, '\0');
        f.read(buf.data(), len);
        if (!f) {
            LOG(ERROR) << "ReadSegmentEntries: truncated entry data at "
                       << "entry " << i << " in " << filepath;
            return ErrorCode::INTERNAL_ERROR;
        }

        OpLogEntry entry;
        if (!DeserializeOpLogEntry(buf, entry)) {
            LOG(ERROR) << "ReadSegmentEntries: failed to deserialize entry "
                       << i << " in " << filepath;
            return ErrorCode::INTERNAL_ERROR;
        }
        entries.push_back(std::move(entry));
    }

    return ErrorCode::OK;
}

// ============================================================
// Write path (Group Commit)
// ============================================================

ErrorCode LocalFsOpLogStore::WriteOpLog(const OpLogEntry& entry, bool sync) {
    if (!enable_batch_write_) {
        return ErrorCode::INVALID_PARAMS;
    }

    // Idempotent: if already persisted, return OK
    if (last_persisted_seq_id_.load() >= entry.sequence_id) {
        return ErrorCode::OK;
    }

    // Serialize entry
    std::string serialized = SerializeOpLogEntry(entry);

    {
        std::lock_guard<std::mutex> lock(batch_mutex_);
        pending_batch_.push_back(
            {std::move(serialized), entry.sequence_id, sync});
    }

    // Notify batch thread
    if (sync || pending_batch_.size() >= kBatchCountLimit) {
        cv_batch_updated_.notify_one();
    }

    if (sync) {
        // Wait for flush to complete
        std::unique_lock<std::mutex> lock(batch_mutex_);
        bool flushed = cv_sync_completed_.wait_for(
            lock, std::chrono::milliseconds(kSyncWaitTimeoutMs),
            [&] { return last_persisted_seq_id_.load() >= entry.sequence_id; });
        if (!flushed) {
            LOG(ERROR) << "WriteOpLog sync wait timed out for seq="
                       << entry.sequence_id;
            return ErrorCode::INTERNAL_ERROR;
        }
    }

    return ErrorCode::OK;
}

void LocalFsOpLogStore::BatchWriteThread() {
    while (batch_write_running_.load()) {
        {
            std::unique_lock<std::mutex> lock(batch_mutex_);
            if (pending_batch_.empty()) {
                cv_batch_updated_.wait_for(
                    lock, std::chrono::milliseconds(kBatchTimeoutMs));
            }
            if (!batch_write_running_.load() && pending_batch_.empty()) {
                break;
            }
        }
        FlushBatch();
    }
}

void LocalFsOpLogStore::FlushBatch() {
    std::deque<BatchEntry> batch_to_write;
    {
        std::lock_guard<std::mutex> lock(batch_mutex_);
        if (pending_batch_.empty()) return;
        batch_to_write.swap(pending_batch_);
    }

    std::vector<BatchEntry> entries(
        std::make_move_iterator(batch_to_write.begin()),
        std::make_move_iterator(batch_to_write.end()));

    // Retry on failure
    ErrorCode err = ErrorCode::INTERNAL_ERROR;
    for (int retry = 0; retry < kFlushRetryCount; ++retry) {
        err = WriteSegmentFile(entries);
        if (err == ErrorCode::OK) break;
        LOG(WARNING) << "FlushBatch: WriteSegmentFile failed, retry "
                     << (retry + 1) << "/" << kFlushRetryCount;
        if (retry + 1 < kFlushRetryCount) {
            std::this_thread::sleep_for(
                std::chrono::milliseconds(kFlushRetryIntervalMs));
        }
    }

    if (err != ErrorCode::OK) {
        LOG(ERROR) << "FlushBatch: all retries failed, data may be lost";
        // Put entries back
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            for (auto it = entries.rbegin(); it != entries.rend(); ++it) {
                pending_batch_.push_front(std::move(*it));
            }
        }
        return;
    }

    // Update last_persisted_seq_id_
    uint64_t max_seq = entries.back().sequence_id;
    last_persisted_seq_id_.store(max_seq);

    // Update latest file
    AtomicWriteFile(LatestFilePath(), std::to_string(max_seq));

    // Notify sync waiters
    cv_sync_completed_.notify_all();
}

// ============================================================
// Read path
// ============================================================

ErrorCode LocalFsOpLogStore::ReadOpLog(uint64_t sequence_id,
                                       OpLogEntry& entry) {
    auto segments = ListSegments();

    // Binary search for the segment containing sequence_id
    auto it = std::lower_bound(
        segments.begin(), segments.end(), sequence_id,
        [](const SegmentInfo& seg, uint64_t seq) { return seg.max_seq < seq; });

    if (it == segments.end()) {
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }

    // Check if sequence_id is within this segment's range
    if (sequence_id < it->min_seq || sequence_id > it->max_seq) {
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }

    std::string filepath = SegmentsDir() + "/" + it->filename;
    std::vector<OpLogEntry> entries;
    auto err = ReadSegmentEntries(filepath, entries);
    if (err != ErrorCode::OK) return err;

    for (auto& e : entries) {
        if (e.sequence_id == sequence_id) {
            entry = std::move(e);
            return ErrorCode::OK;
        }
    }

    return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
}

ErrorCode LocalFsOpLogStore::ReadOpLogSince(uint64_t start_sequence_id,
                                            size_t limit,
                                            std::vector<OpLogEntry>& entries) {
    entries.clear();
    auto segments = ListSegments();

    // Find first segment that could contain entries after start_sequence_id
    auto it =
        std::lower_bound(segments.begin(), segments.end(), start_sequence_id,
                         [](const SegmentInfo& seg, uint64_t seq) {
                             return seg.max_seq <= seq;
                         });

    for (; it != segments.end() && entries.size() < limit; ++it) {
        std::string filepath = SegmentsDir() + "/" + it->filename;
        std::vector<OpLogEntry> seg_entries;
        auto err = ReadSegmentEntries(filepath, seg_entries);
        if (err != ErrorCode::OK) {
            LOG(WARNING) << "ReadOpLogSince: skipping corrupted segment "
                         << it->filename;
            continue;
        }

        for (auto& e : seg_entries) {
            if (e.sequence_id > start_sequence_id) {
                entries.push_back(std::move(e));
                if (entries.size() >= limit) break;
            }
        }
    }

    return ErrorCode::OK;
}

// ============================================================
// Sequence ID management
// ============================================================

ErrorCode LocalFsOpLogStore::GetLatestSequenceId(uint64_t& sequence_id) {
    std::string latest_path = LatestFilePath();
    if (!fs::exists(latest_path)) {
        sequence_id = 0;
        return ErrorCode::OK;
    }

    std::ifstream f(latest_path);
    if (!f) {
        LOG(ERROR) << "GetLatestSequenceId: cannot read " << latest_path;
        return ErrorCode::INTERNAL_ERROR;
    }

    std::string content;
    f >> content;
    try {
        sequence_id = std::stoull(content);
    } catch (...) {
        LOG(ERROR) << "GetLatestSequenceId: invalid content in " << latest_path
                   << ": " << content;
        return ErrorCode::INTERNAL_ERROR;
    }

    return ErrorCode::OK;
}

ErrorCode LocalFsOpLogStore::GetMaxSequenceId(uint64_t& sequence_id) {
    auto segments = ListSegments();
    if (segments.empty()) {
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }

    sequence_id = segments.back().max_seq;
    return ErrorCode::OK;
}

ErrorCode LocalFsOpLogStore::UpdateLatestSequenceId(uint64_t sequence_id) {
    return AtomicWriteFile(LatestFilePath(), std::to_string(sequence_id));
}

// ============================================================
// Snapshot operations
// ============================================================

ErrorCode LocalFsOpLogStore::RecordSnapshotSequenceId(
    const std::string& snapshot_id, uint64_t sequence_id) {
    if (!ValidateSnapshotId(snapshot_id)) {
        return ErrorCode::INVALID_PARAMS;
    }
    return AtomicWriteFile(BuildSnapshotPath(snapshot_id),
                           std::to_string(sequence_id));
}

ErrorCode LocalFsOpLogStore::GetSnapshotSequenceId(
    const std::string& snapshot_id, uint64_t& sequence_id) {
    if (!ValidateSnapshotId(snapshot_id)) {
        return ErrorCode::INVALID_PARAMS;
    }

    std::string path = BuildSnapshotPath(snapshot_id);
    if (!fs::exists(path)) {
        return ErrorCode::OPLOG_ENTRY_NOT_FOUND;
    }

    std::ifstream f(path);
    if (!f) {
        return ErrorCode::INTERNAL_ERROR;
    }

    std::string content;
    f >> content;
    try {
        sequence_id = std::stoull(content);
    } catch (...) {
        return ErrorCode::INTERNAL_ERROR;
    }

    return ErrorCode::OK;
}

// ============================================================
// Cleanup
// ============================================================

ErrorCode LocalFsOpLogStore::CleanupOpLogBefore(uint64_t before_sequence_id) {
    auto segments = ListSegments();
    for (const auto& seg : segments) {
        if (seg.max_seq < before_sequence_id) {
            std::string filepath = SegmentsDir() + "/" + seg.filename;
            try {
                fs::remove(filepath);
            } catch (const fs::filesystem_error& e) {
                LOG(WARNING) << "CleanupOpLogBefore: failed to remove "
                             << filepath << ": " << e.what();
            }
        }
    }
    return ErrorCode::OK;
}

// ============================================================
// Change notifier
// ============================================================

std::unique_ptr<OpLogChangeNotifier> LocalFsOpLogStore::CreateChangeNotifier(
    const std::string& /*cluster_id*/) {
    return std::make_unique<PollingOpLogChangeNotifier>(this,
                                                        poll_interval_ms_);
}

}  // namespace mooncake
