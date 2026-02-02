#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <iterator>
#include <random>
#include <thread>
#include <unordered_set>

#include "master_client.h"
#include "replica.h"
#include "segment.h"
#include "types.h"

DEFINE_string(master_address, "127.0.0.1:50051",
              "Master service address (IP:Port)");
DEFINE_int32(write_interval_ms, 1000, "Interval between writes in milliseconds");
DEFINE_int32(max_keys, 100, "Maximum number of different keys to use");
DEFINE_int32(delete_interval, 10,
             "Delete one key every N writes (0 to disable deletion)");
DEFINE_bool(enable_new_keys, true,
            "Allow new keys every 100 writes (beyond the max_keys limit)");
DEFINE_int32(value_size, 1024, "Size of value (kvcache) in bytes");
DEFINE_bool(mount_segment, true,
            "Mount a segment to master_service before starting operations");
DEFINE_string(segment_name, "mock_client_segment",
              "Name of the segment to mount");
DEFINE_int64(segment_size, 1024 * 1024 * 64,
             "Size of the segment to mount in bytes (default: 64MB)");
DEFINE_int64(segment_base, 0x300000000,
             "Base address of the segment (virtual address, default: 0x300000000)");
DEFINE_int32(ping_interval_sec, 20,
             "Interval between ping operations in seconds (should be less than "
             "master_service client_live_ttl_sec / 2, default: 20)");

namespace mooncake {

// Generate a key name using timestamp
std::string GenerateKey(uint64_t timestamp) {
    return "mock_key_" + std::to_string(timestamp);
}

// Mock client simulator
class MockClientSimulator {
   public:
    MockClientSimulator(const std::string& master_address)
        : master_address_(master_address),
          client_id_(generate_uuid()),
          client_(client_id_),
          write_count_(0),
          key_index_(std::chrono::duration_cast<std::chrono::milliseconds>(
              std::chrono::system_clock::now().time_since_epoch()).count()),
          running_(true) {
        // Connect to master service
        ErrorCode err = client_.Connect(master_address);
        if (err != ErrorCode::OK) {
            LOG(FATAL) << "Failed to connect to master service: "
                       << master_address << ", error=" << static_cast<int>(err);
        }

        // Default replication config
        config_.replica_num = 1;
        config_.with_soft_pin = false;

        LOG(INFO) << "Mock client simulator initialized";
        LOG(INFO) << "  master_address: " << master_address;
        LOG(INFO) << "  client_id: " << client_id_;
        LOG(INFO) << "  max_keys: " << FLAGS_max_keys << " (ignored, keys will grow unlimited)";
        LOG(INFO) << "  write_interval_ms: " << FLAGS_write_interval_ms;
        LOG(INFO) << "  delete_interval: " << FLAGS_delete_interval;
        LOG(INFO) << "  value_size: " << FLAGS_value_size;
        LOG(INFO) << "  mount_segment: " << FLAGS_mount_segment;
        LOG(INFO) << "  ping_interval_sec: " << FLAGS_ping_interval_sec;

        // Mount segment if enabled
        if (FLAGS_mount_segment) {
            Segment segment;
            segment.id = generate_uuid();
            segment.name = FLAGS_segment_name;
            segment.base = static_cast<uintptr_t>(FLAGS_segment_base);
            segment.size = static_cast<size_t>(FLAGS_segment_size);
            segment.te_endpoint = FLAGS_segment_name;

            LOG(INFO) << "[Mount] Attempting to mount segment:";
            LOG(INFO) << "  name: " << segment.name;
            LOG(INFO) << "  id: " << segment.id;
            LOG(INFO) << "  base: 0x" << std::hex << segment.base << std::dec;
            LOG(INFO) << "  size: " << segment.size << " bytes ("
                      << (segment.size / (1024 * 1024)) << " MB)";
            LOG(INFO) << "  te_endpoint: " << segment.te_endpoint;

            auto mount_result = client_.MountSegment(segment);
            if (mount_result.has_value()) {
                LOG(INFO) << "[Mount] Segment mounted successfully";
                mounted_segment_ = segment;

                // Immediately send a ping after mounting to register the client
                LOG(INFO) << "[Mount] Sending initial ping to register client...";
                auto ping_result = client_.Ping();
                if (ping_result.has_value()) {
                    LOG(INFO) << "[Mount] Initial ping SUCCESS - client registered";
                } else {
                    LOG(WARNING) << "[Mount] Initial ping FAILED: error="
                                 << static_cast<int>(ping_result.error())
                                 << " (this may cause segment to be unmounted)";
                }
            } else {
                LOG(WARNING) << "[Mount] Failed to mount segment: error="
                             << static_cast<int>(mount_result.error());
                LOG(WARNING) << "[Mount] PutStart operations may fail with "
                                "NO_AVAILABLE_HANDLE error";
                LOG(WARNING) << "[Mount] You may need to start mooncake_client "
                                "to provide real storage";
            }
        } else {
            LOG(INFO) << "[Mount] Segment mounting disabled (--nomount_segment)";
            LOG(INFO) << "[Mount] PutStart operations may fail if no segment is "
                         "available";
        }
    }

    void Run() {
        LOG(INFO) << "=== Starting mock client simulation ===";
        LOG(INFO) << "Press Ctrl+C to stop";

        // Start ping thread IMMEDIATELY to keep connection alive
        // This is critical: if Run() is called with delay after constructor,
        // the client may expire before ping thread starts
        std::thread ping_thread([this]() {
            LOG(INFO) << "[Ping] Starting ping thread (interval="
                      << FLAGS_ping_interval_sec << "s)";
            
            // Send first ping IMMEDIATELY (no sleep) to ensure client is registered
            // This compensates for any delay between constructor and Run() call
            LOG(INFO) << "[Ping] Sending immediate ping to register client...";
            auto first_ping_result = client_.Ping();
            if (first_ping_result.has_value()) {
                LOG(INFO) << "[Ping] Immediate ping SUCCESS - client registered";
            } else {
                LOG(WARNING) << "[Ping] Immediate ping FAILED: error="
                             << static_cast<int>(first_ping_result.error())
                             << " (client may expire if this persists)";
            }
            
            // Now start periodic pings
            while (running_.load()) {
                std::this_thread::sleep_for(
                    std::chrono::seconds(FLAGS_ping_interval_sec));
                if (!running_.load()) {
                    break;
                }
                LOG(INFO) << "[Ping] Sending periodic ping to master_service...";
                auto ping_result = client_.Ping();
                if (ping_result.has_value()) {
                    LOG(INFO) << "[Ping] Ping SUCCESS - connection kept alive";
                } else {
                    LOG(WARNING) << "[Ping] Ping FAILED: error="
                                 << static_cast<int>(ping_result.error())
                                 << " (segment may be unmounted if this persists)";
                }
            }
            LOG(INFO) << "[Ping] Ping thread stopped";
        });

        int success_count = 0;
        int failure_count = 0;
        int new_key_count = 0;      // Count of successfully created new keys
        int update_count = 0;        // Count of successfully updated existing keys
        int delete_count = 0;
        int delete_success_count = 0;
        int delete_failure_count = 0;
        auto start_time = std::chrono::steady_clock::now();
        const int stats_interval = 50;  // Print stats every N operations

        // Track unique keys that have been successfully written
        std::unordered_set<std::string> written_keys;

        while (running_.load()) {
            int current_write_count = write_count_.load();
            LOG(INFO) << "--- Operation #" << (current_write_count + 1)
                      << " (unique keys written: " << written_keys.size() << ") ---";

            // Determine which key to use
            std::string key;

            // Always use a new key, allowing unlimited key growth
            // Use timestamp-based key index (incremented from initial timestamp)
            key_index_++;
            key = GenerateKey(key_index_.load());
            LOG(INFO) << "[Key Selection] Using key: " << key
                      << " (timestamp=" << key_index_.load() << ")";

            // Simulate GET operation: check if key exists
            LOG(INFO) << "[GET] Checking if key exists: " << key;
            auto exist_result = client_.ExistKey(key);
            bool exists = false;
            if (exist_result.has_value()) {
                exists = exist_result.value();
                LOG(INFO) << "[GET] Key " << key
                          << (exists ? " EXISTS" : " does NOT exist");
            } else {
                LOG(WARNING) << "[GET] ExistKey failed for key=" << key
                             << ", error="
                             << static_cast<int>(exist_result.error());
            }

            if (!exists) {
                // Key doesn't exist, simulate PUT operation
                LOG(INFO) << "[PUT] Starting PUT operation for new key: " << key
                          << ", value_size=" << FLAGS_value_size;

                // Step 1: PutStart
                std::vector<size_t> slice_lengths = {
                    static_cast<size_t>(FLAGS_value_size)};
                LOG(INFO) << "[PUT] Step 1/2: Calling PutStart...";
                auto put_start_result =
                    client_.PutStart(key, slice_lengths, config_);
                if (!put_start_result.has_value()) {
                    ErrorCode err = put_start_result.error();
                    failure_count++;
                    if (err == ErrorCode::NO_AVAILABLE_HANDLE) {
                        LOG(WARNING) << "[PUT] PutStart FAILED: key=" << key
                                     << ", error=NO_AVAILABLE_HANDLE (-200) "
                                     << "(master_service may not have segments configured or is out of space)";
                    } else {
                        LOG(ERROR) << "[PUT] PutStart FAILED: key=" << key
                                   << ", error=" << static_cast<int>(err);
                    }
                    write_count_++;
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(FLAGS_write_interval_ms));
                    continue;
                }

                LOG(INFO) << "[PUT] PutStart SUCCESS: key=" << key
                          << ", replicas=" << put_start_result.value().size();

                // Step 2: PutEnd (complete the put operation)
                LOG(INFO) << "[PUT] Step 2/2: Calling PutEnd...";
                auto put_end_result =
                    client_.PutEnd(key, ReplicaType::MEMORY);
                if (put_end_result.has_value()) {
                    success_count++;
                    new_key_count++;
                    write_count_++;
                    written_keys.insert(key);
                    LOG(INFO) << "[PUT] PutEnd SUCCESS: key=" << key
                              << " (new key created)";
                    LOG(INFO) << "[PUT] Operation COMPLETE: key=" << key
                              << " (total unique keys: " << written_keys.size()
                              << ", new keys: " << new_key_count << ")";
                } else {
                    failure_count++;
                    write_count_++;
                    LOG(ERROR) << "[PUT] PutEnd FAILED: key=" << key
                               << ", error="
                               << static_cast<int>(put_end_result.error());
                    LOG(ERROR) << "[PUT] Operation INCOMPLETE: key=" << key
                               << " (PutStart succeeded but PutEnd failed)";
                    LOG(ERROR) << "[PUT] This may indicate segment is full or "
                                  "other resource issue";
                }
            } else {
                // Key exists, simulate PUT (update) operation
                LOG(INFO) << "[PUT] Starting PUT operation for existing key: "
                          << key << ", value_size=" << FLAGS_value_size;

                // For update, we also use PutStart + PutEnd
                std::vector<size_t> slice_lengths = {
                    static_cast<size_t>(FLAGS_value_size)};
                LOG(INFO) << "[PUT] Step 1/2: Calling PutStart (update)...";
                auto put_start_result =
                    client_.PutStart(key, slice_lengths, config_);
                if (!put_start_result.has_value()) {
                    ErrorCode err = put_start_result.error();
                    failure_count++;
                    if (err == ErrorCode::NO_AVAILABLE_HANDLE) {
                        LOG(WARNING) << "[PUT] PutStart (update) FAILED: key="
                                     << key
                                     << ", error=NO_AVAILABLE_HANDLE (-200) "
                                     << "(master_service may not have segments configured or is out of space)";
                        LOG(WARNING) << "[PUT] Current unique keys written: "
                                     << written_keys.size()
                                     << ", this may indicate segment is full";
                    } else {
                        LOG(ERROR) << "[PUT] PutStart (update) FAILED: key="
                                   << key << ", error="
                                   << static_cast<int>(err);
                    }
                    write_count_++;
                    std::this_thread::sleep_for(
                        std::chrono::milliseconds(FLAGS_write_interval_ms));
                    continue;
                }

                LOG(INFO) << "[PUT] PutStart (update) SUCCESS: key=" << key
                          << ", replicas=" << put_start_result.value().size();

                LOG(INFO) << "[PUT] Step 2/2: Calling PutEnd (update)...";
                auto put_end_result =
                    client_.PutEnd(key, ReplicaType::MEMORY);
                if (put_end_result.has_value()) {
                    success_count++;
                    update_count++;
                    write_count_++;
                    // Note: We don't add to written_keys for updates, only for new keys
                    LOG(INFO) << "[PUT] PutEnd (update) SUCCESS: key=" << key;
                    LOG(INFO) << "[PUT] Operation COMPLETE: key=" << key
                              << " (key updated, total unique keys: "
                              << written_keys.size() << ", updates: " << update_count
                              << ")";
                } else {
                    failure_count++;
                    write_count_++;
                    LOG(ERROR) << "[PUT] PutEnd (update) FAILED: key=" << key
                               << ", error="
                               << static_cast<int>(put_end_result.error());
                    LOG(ERROR) << "[PUT] Operation INCOMPLETE: key=" << key
                               << " (PutStart succeeded but PutEnd failed)";
                    LOG(ERROR) << "[PUT] This may indicate segment is full or "
                                  "other resource issue";
                }
            }

            // Simulate DELETE operation periodically
            if (FLAGS_delete_interval > 0 &&
                write_count_ % FLAGS_delete_interval == 0 &&
                write_count_ > 0 && !written_keys.empty()) {
                delete_count++;
                // Delete a random key from the written keys set
                // Use a simple hash-based selection for deterministic behavior
                size_t key_to_delete_index = (write_count_ / FLAGS_delete_interval) % written_keys.size();
                auto it = written_keys.begin();
                std::advance(it, key_to_delete_index);
                std::string delete_key = *it;
                LOG(INFO) << "[DELETE] Attempting to delete key: " << delete_key
                          << " (delete operation #" << delete_count << ")";

                // First check if the key exists
                auto exist_result = client_.ExistKey(delete_key);
                bool key_exists = false;
                if (exist_result.has_value()) {
                    key_exists = exist_result.value();
                    LOG(INFO) << "[DELETE] Key " << delete_key
                              << (key_exists ? " EXISTS" : " does NOT exist");
                } else {
                    LOG(WARNING) << "[DELETE] Failed to check key existence: "
                                 << delete_key
                                 << ", error="
                                 << static_cast<int>(exist_result.error());
                }

                if (!key_exists) {
                    LOG(INFO) << "[DELETE] Skipping delete: key=" << delete_key
                              << " does not exist (this is normal if the key "
                                 "hasn't been written yet or was already deleted)";
                    continue;
                }

                // Key exists, proceed with deletion
                auto remove_result = client_.Remove(delete_key);
                if (remove_result.has_value()) {
                    delete_success_count++;
                    written_keys.erase(delete_key);  // Remove from tracked keys
                    LOG(INFO) << "[DELETE] SUCCESS: key=" << delete_key
                              << " deleted";
                } else {
                    ErrorCode err = remove_result.error();
                    if (err == ErrorCode::OBJECT_NOT_FOUND) {
                        // Key was deleted between ExistKey and Remove (race condition)
                        written_keys.erase(delete_key);  // Remove from tracked keys anyway
                        delete_success_count++;  // Treat as success (key is already gone)
                        LOG(INFO) << "[DELETE] Key " << delete_key
                                  << " was already deleted (race condition)";
                    } else if (err == ErrorCode::OBJECT_HAS_LEASE) {
                        // Object has active lease, cannot delete yet (this is normal)
                        // Note: ExistKey may have granted a lease, causing this error
                        LOG(INFO) << "[DELETE] Key " << delete_key
                                  << " has active lease, skipping deletion "
                                  << "(this is normal, the key will be deleted after lease expires)";
                        // Don't count as failure, and don't remove from written_keys
                        // so it can be retried later
                    } else {
                        delete_failure_count++;
                        LOG(ERROR) << "[DELETE] FAILED: key=" << delete_key
                                   << ", error=" << static_cast<int>(err);
                    }
                }
            }

            // Print statistics periodically
            int total_ops = success_count + failure_count;
            if (total_ops > 0 && total_ops % stats_interval == 0) {
                auto current_time = std::chrono::steady_clock::now();
                auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                    current_time - start_time).count();
                double success_rate =
                    (total_ops > 0) ? (100.0 * success_count / total_ops) : 0.0;
                double ops_per_sec = (elapsed > 0) ? (total_ops / elapsed) : 0.0;

                LOG(INFO) << "=== Statistics (last " << stats_interval
                          << " operations) ===";
                LOG(INFO) << "  Total operations: " << total_ops;
                LOG(INFO) << "  Successful: " << success_count
                          << " (" << success_rate << "%)";
                LOG(INFO) << "    - New keys created: " << new_key_count;
                LOG(INFO) << "    - Keys updated: " << update_count;
                LOG(INFO) << "  Failed: " << failure_count;
                LOG(INFO) << "  Unique keys written: " << written_keys.size();
                LOG(INFO) << "  Delete operations: " << delete_count
                          << " (success: " << delete_success_count
                          << ", failed: " << delete_failure_count << ")";
                LOG(INFO) << "  Elapsed time: " << elapsed << " seconds";
                LOG(INFO) << "  Operations/sec: " << ops_per_sec;
                LOG(INFO) << "========================================";
            }

            // Sleep for the specified interval
            LOG(INFO) << "[Sleep] Waiting " << FLAGS_write_interval_ms
                      << " ms before next operation...";
            std::this_thread::sleep_for(
                std::chrono::milliseconds(FLAGS_write_interval_ms));
        }

        // Print final statistics
        auto end_time = std::chrono::steady_clock::now();
        auto total_elapsed = std::chrono::duration_cast<std::chrono::seconds>(
            end_time - start_time).count();
        int total_ops = success_count + failure_count;
        double final_success_rate =
            (total_ops > 0) ? (100.0 * success_count / total_ops) : 0.0;
        double final_ops_per_sec =
            (total_elapsed > 0) ? (total_ops / total_elapsed) : 0.0;

        LOG(INFO) << "=== Final Statistics ===";
        LOG(INFO) << "  Total operations: " << total_ops;
        LOG(INFO) << "  Successful: " << success_count << " ("
                  << final_success_rate << "%)";
        LOG(INFO) << "    - New keys created: " << new_key_count;
        LOG(INFO) << "    - Keys updated: " << update_count;
        LOG(INFO) << "  Failed: " << failure_count;
        LOG(INFO) << "  Unique keys written: " << written_keys.size();
        LOG(INFO) << "  Delete operations: " << delete_count
                  << " (success: " << delete_success_count
                  << ", failed: " << delete_failure_count << ")";
        LOG(INFO) << "  Total elapsed time: " << total_elapsed << " seconds";
        LOG(INFO) << "  Average operations/sec: " << final_ops_per_sec;
        LOG(INFO) << "=========================";

        // Stop ping thread
        running_.store(false);
        if (ping_thread.joinable()) {
            ping_thread.join();
        }
    }

    void Stop() { running_.store(false); }

   private:
    std::string master_address_;
    UUID client_id_;
    MasterClient client_;
    ReplicateConfig config_;
    Segment mounted_segment_;
    std::atomic<int> write_count_;
    std::atomic<uint64_t> key_index_;
    std::atomic<bool> running_;
};

}  // namespace mooncake

int main(int argc, char* argv[]) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);

    if (FLAGS_master_address.empty()) {
        LOG(FATAL) << "master_address must be specified";
    }

    mooncake::MockClientSimulator simulator(FLAGS_master_address);

    // Handle Ctrl+C gracefully
    std::signal(SIGINT, [](int) {
        LOG(INFO) << "Received SIGINT, stopping...";
        exit(0);
    });

    simulator.Run();

    return 0;
}
