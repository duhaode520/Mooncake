#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "etcd_helper.h"
#include "etcd_oplog_store.h"
#include "ha_helper.h"
#include "ha_metric_manager.h"
#include "hot_standby_service.h"
#include "master_service.h"
#include "metadata_store.h"
#include "oplog_manager.h"
#include "standby_state_machine.h"

namespace mooncake {
namespace testing {

DEFINE_string(hs_etcd_endpoints, "",
              "Etcd endpoints for hot-standby integration tests (required for integration tests)");
DEFINE_string(hs_cluster_id, "hs_integration_cluster",
              "Cluster ID prefix for hot-standby integration tests");

// RAII helper to ensure HotStandbyService is always stopped
class StandbyServiceGuard {
   public:
    explicit StandbyServiceGuard(HotStandbyService* service) : service_(service) {}
    ~StandbyServiceGuard() {
        if (service_) {
            service_->Stop();
            // Give etcd watch goroutines time to clean up and exit.
            // Increased delay to ensure all pending callbacks are processed
            // and Go goroutines have time to exit completely.
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }
    // Disable copy and move
    StandbyServiceGuard(const StandbyServiceGuard&) = delete;
    StandbyServiceGuard& operator=(const StandbyServiceGuard&) = delete;
    StandbyServiceGuard(StandbyServiceGuard&&) = delete;
    StandbyServiceGuard& operator=(StandbyServiceGuard&&) = delete;

   private:
    HotStandbyService* service_;
};

class HotStandbyIntegrationTest : public ::testing::Test {
   protected:
    // Static mutex to ensure tests run serially and prevent etcd resource conflicts
    static std::mutex test_mutex_;
    // Per-test lock guard to ensure lock is released even if test fails
    std::unique_ptr<std::lock_guard<std::mutex>> test_lock_;

    static void SetUpTestSuite() {
#ifdef STORE_USE_ETCD
        // Initialize glog
        google::InitGoogleLogging("HotStandbyIntegrationTest");
        google::SetVLOGLevel("*", 1);
        FLAGS_logtostderr = 1;

        // Check if etcd endpoints are provided
        if (FLAGS_hs_etcd_endpoints.empty()) {
            GTEST_SKIP() << "hs_etcd_endpoints not provided. "
                            "Set --hs_etcd_endpoints=<endpoint> to run integration tests.";
        }

        // Initialize etcd client
        ASSERT_EQ(ErrorCode::OK,
                  EtcdHelper::ConnectToEtcdStoreClient(FLAGS_hs_etcd_endpoints))
            << "Failed to connect to etcd at " << FLAGS_hs_etcd_endpoints;
#else
        GTEST_SKIP() << "STORE_USE_ETCD is not enabled; "
                        "hot-standby integration tests require etcd.";
#endif
    }

    static void TearDownTestSuite() {
#ifdef STORE_USE_ETCD
        google::ShutdownGoogleLogging();
#endif
    }

    void SetUp() override {
#ifdef STORE_USE_ETCD
        // Acquire lock to ensure tests run serially
        // Use lock_guard to ensure lock is released even if test fails
        test_lock_ = std::make_unique<std::lock_guard<std::mutex>>(test_mutex_);
        // Clean up test data before each test
        CleanupTestData();
#endif
    }

    void TearDown() override {
#ifdef STORE_USE_ETCD
        // Clean up test data after each test
        CleanupTestData();
        // Give etcd watch goroutines time to clean up before next test
        // Increased delay to allow watch streams and gRPC connections to fully close
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        // Release lock (lock_guard will automatically unlock in destructor)
        test_lock_.reset();
#endif
    }

   private:
    // Helper function to construct end_key for DeleteRange to delete a single key
    static std::string MakeEndKeyForSingleKey(const std::string& key) {
        std::string end_key = key;
        for (int i = static_cast<int>(end_key.size()) - 1; i >= 0; --i) {
            unsigned char c = static_cast<unsigned char>(end_key[i]);
            if (c < 0xFF) {
                end_key[i] = static_cast<char>(c + 1);
                end_key.resize(i + 1);
                return end_key;
            }
        }
        // If all characters are 0xFF, append '\0'
        return key + std::string(1, '\0');
    }

    void CleanupTestData() {
#ifdef STORE_USE_ETCD
        // Delete all keys under /oplog/{cluster_id}/ prefix
        std::string prefix = std::string("/oplog/") + FLAGS_hs_cluster_id + "/";

        auto prefix_end = [](std::string p) -> std::string {
            for (int i = static_cast<int>(p.size()) - 1; i >= 0; --i) {
                unsigned char c = static_cast<unsigned char>(p[i]);
                if (c < 0xFF) {
                    p[i] = static_cast<char>(c + 1);
                    p.resize(i + 1);
                    return p;
                }
            }
            return std::string(1, '\0');
        };
        std::string end_key = prefix_end(prefix);

        (void)EtcdHelper::DeleteRange(prefix.c_str(), prefix.size(), end_key.c_str(),
                                      end_key.size());

        // Also delete /latest key using DeleteRange
        std::string latest_key = std::string("/oplog/") + FLAGS_hs_cluster_id + "/latest";
        std::string latest_end_key = MakeEndKeyForSingleKey(latest_key);
        (void)EtcdHelper::DeleteRange(latest_key.c_str(), latest_key.size(),
                                      latest_end_key.c_str(), latest_end_key.size());
#endif
    }
};

// ========== 8.1.1 End-to-end tests ==========

TEST_F(HotStandbyIntegrationTest, TestPrimaryStandbySync) {
#ifndef STORE_USE_ETCD
    GTEST_SKIP() << "STORE_USE_ETCD is not enabled.";
#else
    if (FLAGS_hs_etcd_endpoints.empty()) {
        GTEST_SKIP() << "hs_etcd_endpoints not provided.";
    }

    // 1. Simulate primary writing OpLog entries to etcd
    OpLogManager primary_oplog;
    primary_oplog.SetEtcdOpLogStore(
        std::make_shared<EtcdOpLogStore>(FLAGS_hs_cluster_id, true));

    // Write several test entries
    std::vector<std::string> test_keys;
    std::vector<uint64_t> test_seq_ids;
    for (int i = 0; i < 10; ++i) {
        std::string key = "test_key_" + std::to_string(i);
        std::string payload =
            R"({"client_id_first":1,"client_id_second":2,"size":1024,"replicas":[]})";

        uint64_t seq_id = primary_oplog.Append(OpType::PUT_END, key, payload);
        test_keys.push_back(key);
        test_seq_ids.push_back(seq_id);

        LOG(INFO) << "Primary wrote OpLog entry: seq_id=" << seq_id
                  << ", key=" << key;
    }

    uint64_t last_seq_id = primary_oplog.GetLastSequenceId();
    LOG(INFO) << "Primary wrote " << last_seq_id << " OpLog entries";

    // 2. Start the standby HotStandbyService
    HotStandbyConfig hs_config;
    hs_config.enable_verification = false;
    hs_config.max_replication_lag_entries = 1000;

    HotStandbyService standby(hs_config);
    StandbyServiceGuard guard(&standby);  // RAII: ensure cleanup even on failure

    ErrorCode start_err = standby.Start(
        /*primary_address_unused=*/"", FLAGS_hs_etcd_endpoints,
        FLAGS_hs_cluster_id);

    ASSERT_EQ(ErrorCode::OK, start_err)
        << "Failed to start HotStandbyService";

    // 3. Wait for the standby to finish initial sync
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    bool synced = false;

    while (std::chrono::steady_clock::now() < deadline) {
        auto status = standby.GetSyncStatus();

        LOG(INFO) << "Standby status: state="
                  << StandbyStateToString(status.state)
                  << ", applied_seq_id=" << status.applied_seq_id
                  << ", primary_seq_id=" << status.primary_seq_id
                  << ", lag_entries=" << status.lag_entries
                  << ", is_connected=" << status.is_connected;

        if (status.state == StandbyState::WATCHING &&
            status.lag_entries == 0 &&
            status.applied_seq_id >= last_seq_id) {
            synced = true;
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    ASSERT_TRUE(synced) << "Standby failed to sync within timeout";

    // 4. Verify standby metadata snapshot
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
    ASSERT_TRUE(standby.ExportMetadataSnapshot(snapshot));

    LOG(INFO) << "Standby metadata snapshot size: " << snapshot.size();

    // Verify all written keys exist in the snapshot
    std::set<std::string> snapshot_keys;
    for (const auto& kv : snapshot) {
        snapshot_keys.insert(kv.first);
    }

    for (const auto& key : test_keys) {
        EXPECT_NE(snapshot_keys.end(), snapshot_keys.find(key))
            << "Key " << key << " not found in Standby snapshot";
    }

    EXPECT_GE(snapshot.size(), test_keys.size())
        << "Standby snapshot should contain at least the test keys";

    // 5. Verify sequence IDs
    uint64_t latest_applied = standby.GetLatestAppliedSequenceId();
    EXPECT_GE(latest_applied, last_seq_id)
        << "Standby should have applied at least the last test sequence ID";

    // guard will automatically stop the service
#endif
}

TEST_F(HotStandbyIntegrationTest, TestStandbyPromotion) {
#ifndef STORE_USE_ETCD
    GTEST_SKIP() << "STORE_USE_ETCD is not enabled.";
#else
    if (FLAGS_hs_etcd_endpoints.empty()) {
        GTEST_SKIP() << "hs_etcd_endpoints not provided.";
    }

    // 1. Write several OpLog entries
    OpLogManager primary_oplog;
    primary_oplog.SetEtcdOpLogStore(
        std::make_shared<EtcdOpLogStore>(FLAGS_hs_cluster_id, true));

    std::vector<std::string> test_keys;
    for (int i = 0; i < 5; ++i) {
        std::string key = "promote_test_key_" + std::to_string(i);
        std::string payload =
            R"({"client_id_first":1,"client_id_second":2,"size":1024,"replicas":[]})";
        primary_oplog.Append(OpType::PUT_END, key, payload);
        test_keys.push_back(key);
    }

    uint64_t last_seq_id = primary_oplog.GetLastSequenceId();

    // 2. Start the standby and wait for sync
    HotStandbyConfig hs_config;
    hs_config.enable_verification = false;
    HotStandbyService standby(hs_config);
    StandbyServiceGuard guard(&standby);  // RAII: ensure cleanup

    ASSERT_EQ(ErrorCode::OK, standby.Start("", FLAGS_hs_etcd_endpoints,
                                            FLAGS_hs_cluster_id));

    // Wait until sync finishes
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline) {
        auto status = standby.GetSyncStatus();
        if (status.state == StandbyState::WATCHING &&
            status.applied_seq_id >= last_seq_id &&
            status.lag_entries == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // 3. Verify standby is ready for promotion
    ASSERT_TRUE(standby.IsReadyForPromotion())
        << "Standby should be ready for promotion";

    // 4. Perform promotion
    // Note: `Promote()` returns an `ErrorCode` (MasterService creation is
    // handled externally), so verify the promotion succeeded.
    ErrorCode err = standby.Promote();
    EXPECT_EQ(ErrorCode::OK, err);

    // 5. Verify sequence ID after promotion
    uint64_t promoted_seq_id = standby.GetLatestAppliedSequenceId();
    EXPECT_GE(promoted_seq_id, last_seq_id)
        << "Promoted sequence ID should be at least the last written sequence ID";

    // 6. Verify metadata snapshot is still valid after promotion
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
    ASSERT_TRUE(standby.ExportMetadataSnapshot(snapshot));

    std::set<std::string> snapshot_keys;
    for (const auto& kv : snapshot) {
        snapshot_keys.insert(kv.first);
    }

    for (const auto& key : test_keys) {
        EXPECT_NE(snapshot_keys.end(), snapshot_keys.find(key))
            << "Key " << key << " should be in snapshot after promotion";
    }

    // guard will automatically stop the service
#endif
}

TEST_F(HotStandbyIntegrationTest, TestFailoverScenario) {
#ifndef STORE_USE_ETCD
    GTEST_SKIP() << "STORE_USE_ETCD is not enabled.";
#else
    if (FLAGS_hs_etcd_endpoints.empty()) {
        GTEST_SKIP() << "hs_etcd_endpoints not provided.";
    }

    // 1. Simulate primary writing data
    OpLogManager primary_oplog;
    primary_oplog.SetEtcdOpLogStore(
        std::make_shared<EtcdOpLogStore>(FLAGS_hs_cluster_id, true));

    std::vector<std::string> test_keys;
    for (int i = 0; i < 10; ++i) {
        std::string key = "failover_key_" + std::to_string(i);
        std::string payload =
            R"({"client_id_first":1,"client_id_second":2,"size":1024,"replicas":[]})";
        primary_oplog.Append(OpType::PUT_END, key, payload);
        test_keys.push_back(key);
    }

    uint64_t last_seq_id = primary_oplog.GetLastSequenceId();

    // 2. Start the standby
    HotStandbyConfig hs_config;
    hs_config.enable_verification = false;
    HotStandbyService standby(hs_config);
    StandbyServiceGuard guard(&standby);  // RAII: ensure cleanup

    ASSERT_EQ(ErrorCode::OK, standby.Start("", FLAGS_hs_etcd_endpoints,
                                            FLAGS_hs_cluster_id));

    // Wait for sync
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline) {
        auto status = standby.GetSyncStatus();
        if (status.state == StandbyState::WATCHING &&
            status.applied_seq_id >= last_seq_id) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // 3. Simulate primary failure (stop writing new OpLogs)
    // In real scenarios this could be a primary crash or a network partition

    // 4. Verify standby data integrity
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
    ASSERT_TRUE(standby.ExportMetadataSnapshot(snapshot));

    std::set<std::string> snapshot_keys;
    for (const auto& kv : snapshot) {
        snapshot_keys.insert(kv.first);
    }

    for (const auto& key : test_keys) {
        EXPECT_NE(snapshot_keys.end(), snapshot_keys.find(key))
            << "Key " << key << " should be in Standby after Primary failure";
    }

    // 5. Perform promotion (simulate failover)
    ASSERT_TRUE(standby.IsReadyForPromotion());
    ErrorCode promote_err = standby.Promote();
    EXPECT_EQ(ErrorCode::OK, promote_err);

    // 6. Verify state after promotion
    uint64_t promoted_seq_id = standby.GetLatestAppliedSequenceId();
    EXPECT_GE(promoted_seq_id, last_seq_id);

    // guard will automatically stop the service
#endif
}

TEST_F(HotStandbyIntegrationTest, TestDataConsistency) {
#ifndef STORE_USE_ETCD
    GTEST_SKIP() << "STORE_USE_ETCD is not enabled.";
#else
    if (FLAGS_hs_etcd_endpoints.empty()) {
        GTEST_SKIP() << "hs_etcd_endpoints not provided.";
    }

    // 1. Simulate primary writing mixed operations
    OpLogManager primary_oplog;
    primary_oplog.SetEtcdOpLogStore(
        std::make_shared<EtcdOpLogStore>(FLAGS_hs_cluster_id, true));

    std::map<std::string, bool> expected_keys;  // key -> should_exist

    // PUT operations
    for (int i = 0; i < 5; ++i) {
        std::string key = "put_key_" + std::to_string(i);
        std::string payload =
            R"({"client_id_first":1,"client_id_second":2,"size":1024,"replicas":[]})";
        primary_oplog.Append(OpType::PUT_END, key, payload);
        expected_keys[key] = true;
    }

    // REMOVE operations
    for (int i = 0; i < 2; ++i) {
        std::string key = "put_key_" + std::to_string(i);
        primary_oplog.Append(OpType::REMOVE, key, "");
        expected_keys[key] = false;  // Deleted
    }

    // Then PUT some more keys
    for (int i = 5; i < 8; ++i) {
        std::string key = "put_key_" + std::to_string(i);
        std::string payload =
            R"({"client_id_first":1,"client_id_second":2,"size":2048,"replicas":[]})";
        primary_oplog.Append(OpType::PUT_END, key, payload);
        expected_keys[key] = true;
    }

    uint64_t last_seq_id = primary_oplog.GetLastSequenceId();
    LOG(INFO) << "Primary wrote " << last_seq_id << " OpLog entries";

    // 2. Start the standby
    HotStandbyConfig hs_config;
    hs_config.enable_verification = false;
    HotStandbyService standby(hs_config);
    StandbyServiceGuard guard(&standby);  // RAII: ensure cleanup

    ASSERT_EQ(ErrorCode::OK, standby.Start("", FLAGS_hs_etcd_endpoints,
                                            FLAGS_hs_cluster_id));

    // 3. Wait for sync
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    while (std::chrono::steady_clock::now() < deadline) {
        auto status = standby.GetSyncStatus();
        if (status.state == StandbyState::WATCHING &&
            status.applied_seq_id >= last_seq_id &&
            status.lag_entries == 0) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // 4. Verify consistency
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
    ASSERT_TRUE(standby.ExportMetadataSnapshot(snapshot));

    std::set<std::string> actual_keys;
    for (const auto& kv : snapshot) {
        actual_keys.insert(kv.first);
    }

    // Verify all keys expected to exist are present
    for (const auto& kv : expected_keys) {
        if (kv.second) {
            EXPECT_NE(actual_keys.end(), actual_keys.find(kv.first))
                << "Key " << kv.first << " should exist but not found";
        } else {
            EXPECT_EQ(actual_keys.end(), actual_keys.find(kv.first))
                << "Key " << kv.first << " should be removed but still exists";
        }
    }

    // Verify metadata entry count
    size_t expected_count = 0;
    for (const auto& kv : expected_keys) {
        if (kv.second) expected_count++;
    }
    EXPECT_EQ(expected_count, actual_keys.size())
        << "Standby metadata count mismatch. Expected: " << expected_count
        << ", Actual: " << actual_keys.size();

    // guard will automatically stop the service
#endif
}

// ========== 8.1.2 Multi-node tests ==========

TEST_F(HotStandbyIntegrationTest, TestMultipleStandbys) {
#ifndef STORE_USE_ETCD
    GTEST_SKIP() << "STORE_USE_ETCD is not enabled.";
#else
    if (FLAGS_hs_etcd_endpoints.empty()) {
        GTEST_SKIP() << "hs_etcd_endpoints not provided.";
    }

    // 1. Simulate primary writing data
    OpLogManager primary_oplog;
    primary_oplog.SetEtcdOpLogStore(
        std::make_shared<EtcdOpLogStore>(FLAGS_hs_cluster_id, true));

    std::vector<std::string> test_keys;
    for (int i = 0; i < 10; ++i) {
        std::string key = "multi_standby_key_" + std::to_string(i);
        std::string payload =
            R"({"client_id_first":1,"client_id_second":2,"size":1024,"replicas":[]})";
        primary_oplog.Append(OpType::PUT_END, key, payload);
        test_keys.push_back(key);
    }

    uint64_t last_seq_id = primary_oplog.GetLastSequenceId();

    // 2. Start two standby instances
    HotStandbyConfig hs_config;
    hs_config.enable_verification = false;

    HotStandbyService standby1(hs_config);
    HotStandbyService standby2(hs_config);
    StandbyServiceGuard guard1(&standby1);  // RAII: ensure cleanup
    StandbyServiceGuard guard2(&standby2);  // RAII: ensure cleanup

    ASSERT_EQ(ErrorCode::OK, standby1.Start("", FLAGS_hs_etcd_endpoints,
                                            FLAGS_hs_cluster_id));
    ASSERT_EQ(ErrorCode::OK, standby2.Start("", FLAGS_hs_etcd_endpoints,
                                            FLAGS_hs_cluster_id));

    // 3. Wait for both standbys to finish syncing
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    bool both_synced = false;

    while (std::chrono::steady_clock::now() < deadline) {
        auto status1 = standby1.GetSyncStatus();
        auto status2 = standby2.GetSyncStatus();

        if (status1.state == StandbyState::WATCHING &&
            status2.state == StandbyState::WATCHING &&
            status1.applied_seq_id >= last_seq_id &&
            status2.applied_seq_id >= last_seq_id &&
            status1.lag_entries == 0 && status2.lag_entries == 0) {
            both_synced = true;
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    ASSERT_TRUE(both_synced) << "Both standbys failed to sync within timeout";

    // 4. Verify metadata snapshots of both standbys are identical
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot1;
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot2;

    ASSERT_TRUE(standby1.ExportMetadataSnapshot(snapshot1));
    ASSERT_TRUE(standby2.ExportMetadataSnapshot(snapshot2));

    EXPECT_EQ(snapshot1.size(), snapshot2.size())
        << "Both standbys should have the same number of metadata entries";

    std::set<std::string> keys1, keys2;
    for (const auto& kv : snapshot1) {
        keys1.insert(kv.first);
    }
    for (const auto& kv : snapshot2) {
        keys2.insert(kv.first);
    }

    EXPECT_EQ(keys1, keys2) << "Both standbys should have identical key sets";

    // 5. Verify all test keys are present in both standbys
    for (const auto& key : test_keys) {
        EXPECT_NE(keys1.end(), keys1.find(key))
            << "Key " << key << " not found in standby1";
        EXPECT_NE(keys2.end(), keys2.find(key))
            << "Key " << key << " not found in standby2";
    }

    // guards will automatically stop the services
#endif
}

TEST_F(HotStandbyIntegrationTest, TestLeaderElection) {
#ifndef STORE_USE_ETCD
    GTEST_SKIP() << "STORE_USE_ETCD is not enabled.";
#else
    if (FLAGS_hs_etcd_endpoints.empty()) {
        GTEST_SKIP() << "hs_etcd_endpoints not provided.";
    }

    // 1. Clean up master view (delete any existing key)
    MasterViewHelper mv_helper(FLAGS_hs_cluster_id);
    mv_helper.ConnectToEtcd(FLAGS_hs_etcd_endpoints);
    std::string master_view_key = mv_helper.GetMasterViewKey();
    // Use DeleteRange to delete a single key
    std::string master_view_end_key = master_view_key;
    for (int i = static_cast<int>(master_view_end_key.size()) - 1; i >= 0; --i) {
        unsigned char c = static_cast<unsigned char>(master_view_end_key[i]);
        if (c < 0xFF) {
            master_view_end_key[i] = static_cast<char>(c + 1);
            master_view_end_key.resize(i + 1);
            break;
        }
    }
    if (master_view_end_key == master_view_key) {
        master_view_end_key = master_view_key + std::string(1, '\0');
    }
    (void)EtcdHelper::DeleteRange(master_view_key.c_str(), master_view_key.size(),
                                   master_view_end_key.c_str(), master_view_end_key.size());
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // 2. First node election (run in background thread because ElectLeader blocks)
    std::string master1_address = "10.0.0.1:8888";
    ViewVersionId version1 = 0;
    EtcdLeaseId lease1 = 0;
    bool election1_done = false;

    std::thread election1_thread([&]() {
        mv_helper.ElectLeader(master1_address, version1, lease1);
        election1_done = true;
    });

    // Wait for the election to complete
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!election1_done && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ASSERT_TRUE(election1_done) << "Master1 election should complete";
    ASSERT_NE(0, lease1) << "Master1 should successfully elect as leader";

    // Verify master view is set
    std::string current_master;
    ViewVersionId current_version = 0;
    ASSERT_EQ(ErrorCode::OK,
              mv_helper.GetMasterView(current_master, current_version));
    EXPECT_EQ(master1_address, current_master);

    // 3. Cancel the first node's lease (simulate failure)
    EtcdHelper::CancelKeepAlive(lease1);
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 4. Second node should now be able to win the election
    MasterViewHelper mv_helper2(FLAGS_hs_cluster_id);
    mv_helper2.ConnectToEtcd(FLAGS_hs_etcd_endpoints);
    std::string master2_address = "10.0.0.2:8888";
    ViewVersionId version2 = 0;
    EtcdLeaseId lease2 = 0;
    bool election2_done = false;

    std::thread election2_thread([&]() {
        mv_helper2.ElectLeader(master2_address, version2, lease2);
        election2_done = true;
    });

    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (!election2_done && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    ASSERT_TRUE(election2_done) << "Master2 election should complete";
    ASSERT_NE(0, lease2) << "Master2 should successfully elect after master1 fails";

    // Verify master view switches to the second node
    ASSERT_EQ(ErrorCode::OK,
              mv_helper2.GetMasterView(current_master, current_version));
    EXPECT_EQ(master2_address, current_master)
        << "Master view should switch to master2";

    // Cleanup
    if (election1_thread.joinable()) {
        election1_thread.join();
    }
    if (election2_thread.joinable()) {
        election2_thread.join();
    }
    if (lease2 != 0) {
        EtcdHelper::CancelKeepAlive(lease2);
    }
#endif
}

// ========== 8.1.3 Stress tests ==========

TEST_F(HotStandbyIntegrationTest, TestHighThroughputSync) {
#ifndef STORE_USE_ETCD
    GTEST_SKIP() << "STORE_USE_ETCD is not enabled.";
#else
    if (FLAGS_hs_etcd_endpoints.empty()) {
        GTEST_SKIP() << "hs_etcd_endpoints not provided.";
    }

    // 1. Start the standby
    HotStandbyConfig hs_config;
    hs_config.enable_verification = false;
    HotStandbyService standby(hs_config);
    StandbyServiceGuard guard(&standby);  // RAII: ensure cleanup

    ASSERT_EQ(ErrorCode::OK, standby.Start("", FLAGS_hs_etcd_endpoints,
                                            FLAGS_hs_cluster_id));

    // Wait for standby to enter WATCHING state
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        auto status = standby.GetSyncStatus();
        if (status.state == StandbyState::WATCHING) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // 2. Simulate high-throughput writes on the primary
    OpLogManager primary_oplog;
    primary_oplog.SetEtcdOpLogStore(
        std::make_shared<EtcdOpLogStore>(FLAGS_hs_cluster_id, true));

    const int num_writes = 100;
    std::string payload =
        R"({"client_id_first":1,"client_id_second":2,"size":1024,"replicas":[]})";

    auto write_start = std::chrono::steady_clock::now();
    for (int i = 0; i < num_writes; ++i) {
        std::string key = "throughput_key_" + std::to_string(i);
        primary_oplog.Append(OpType::PUT_END, key, payload);
    }
    auto write_end = std::chrono::steady_clock::now();
    auto write_duration = std::chrono::duration_cast<std::chrono::milliseconds>(
        write_end - write_start);

    uint64_t last_seq_id = primary_oplog.GetLastSequenceId();
    LOG(INFO) << "Wrote " << num_writes << " entries in "
              << write_duration.count() << "ms, last_seq_id=" << last_seq_id;

    // 3. Monitor standby lag
    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    uint64_t max_lag = 0;

    while (std::chrono::steady_clock::now() < deadline) {
        auto status = standby.GetSyncStatus();
        if (status.lag_entries > max_lag) {
            max_lag = status.lag_entries;
        }

        LOG(INFO) << "Standby lag: " << status.lag_entries
                  << " entries, applied_seq_id=" << status.applied_seq_id
                  << ", primary_seq_id=" << status.primary_seq_id;

        if (status.applied_seq_id >= last_seq_id && status.lag_entries == 0) {
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // 4. Verify final sync result
    auto final_status = standby.GetSyncStatus();
    EXPECT_GE(final_status.applied_seq_id, last_seq_id)
        << "Standby should have applied all entries";
    EXPECT_EQ(0u, final_status.lag_entries)
        << "Standby lag should be zero after sync";

    LOG(INFO) << "Max lag observed: " << max_lag << " entries";

    // guard will automatically stop the service
#endif
}

TEST_F(HotStandbyIntegrationTest, TestLargePayloadSync) {
#ifndef STORE_USE_ETCD
    GTEST_SKIP() << "STORE_USE_ETCD is not enabled.";
#else
    if (FLAGS_hs_etcd_endpoints.empty()) {
        GTEST_SKIP() << "hs_etcd_endpoints not provided.";
    }

    // 1. Start the standby
    HotStandbyConfig hs_config;
    hs_config.enable_verification = false;
    HotStandbyService standby(hs_config);
    StandbyServiceGuard guard(&standby);  // RAII: ensure cleanup

    ASSERT_EQ(ErrorCode::OK, standby.Start("", FLAGS_hs_etcd_endpoints,
                                            FLAGS_hs_cluster_id));

    // Wait for standby to enter WATCHING state
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    while (std::chrono::steady_clock::now() < deadline) {
        auto status = standby.GetSyncStatus();
        if (status.state == StandbyState::WATCHING) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    // 2. Write an entry with payload close to the maximum size
    OpLogManager primary_oplog;
    primary_oplog.SetEtcdOpLogStore(
        std::make_shared<EtcdOpLogStore>(FLAGS_hs_cluster_id, true));

    // Create a JSON payload close to but not exceeding the logical max payload size.
    // Note: etcd has a default max request size limit for the entire request
    // (including key, value, and metadata). The actual limit may be smaller than 2MB.
    // We use 1MB to stay safely under typical etcd limits without requiring etcd config changes.
    // kMaxPayloadSize = 10MB, but for integration tests we use 1MB to work with default etcd settings.
    const size_t large_payload_size = 1024 * 1024;  // 1MB (safe for typical etcd limits)
    std::string large_payload = R"({"client_id_first":1,"client_id_second":2,"size":1024,"replicas":[])";
    size_t padding_size = large_payload_size - large_payload.size() - 1;
    if (padding_size > 0) {
        large_payload += std::string(padding_size, 'X');
    }
    large_payload += "}";

    std::string key = "large_payload_key";
    // Use AppendAndPersist to check if write succeeds (may fail if etcd limit is too small)
    auto result = primary_oplog.AppendAndPersist(OpType::PUT_END, key, large_payload);
    if (!result.has_value()) {
        // Write failed (likely due to etcd size limit), skip this test
        GTEST_SKIP() << "Failed to write large payload to etcd (error="
                     << static_cast<int>(result.error())
                     << "). This may indicate etcd --max-request-bytes is too small. "
                     << "Payload size was " << large_payload.size() << " bytes.";
    }
    uint64_t seq_id = result.value();

    LOG(INFO) << "Wrote large payload entry: seq_id=" << seq_id
              << ", payload_size=" << large_payload.size();

    // 3. Wait for standby to sync
    deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
    bool synced = false;

    while (std::chrono::steady_clock::now() < deadline) {
        auto status = standby.GetSyncStatus();
        if (status.applied_seq_id >= seq_id && status.lag_entries == 0) {
            synced = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }

    ASSERT_TRUE(synced) << "Standby failed to sync large payload entry";

    // 4. Verify standby metadata snapshot contains the key
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
    ASSERT_TRUE(standby.ExportMetadataSnapshot(snapshot));

    bool found = false;
    for (const auto& kv : snapshot) {
        if (kv.first == key) {
            found = true;
            break;
        }
    }

    EXPECT_TRUE(found) << "Large payload key should be in Standby snapshot";

    // 5. Verify that entries exceeding the maximum payload size are rejected
    std::string oversized_payload(OpLogManager::kMaxPayloadSize + 1, 'X');
    // Note: OpLogManager::Append does not directly reject oversize payloads, but
    // EtcdOpLogStore or OpLogApplier will validate them when applying.
    // Here we verify that ValidateEntrySize rejects such an entry.
    OpLogEntry test_entry;
    test_entry.object_key = "oversized_key";
    test_entry.payload = oversized_payload;

    std::string reason;
    bool is_valid = OpLogManager::ValidateEntrySize(test_entry, &reason);
    EXPECT_FALSE(is_valid) << "Oversized payload should be rejected: " << reason;

    // guard will automatically stop the service
#endif
}

}  // namespace testing
}  // namespace mooncake

// Define static member
namespace mooncake {
namespace testing {
std::mutex HotStandbyIntegrationTest::test_mutex_;
}  // namespace testing
}  // namespace mooncake

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


