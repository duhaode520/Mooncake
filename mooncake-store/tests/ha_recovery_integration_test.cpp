// mooncake-store/tests/ha_recovery_integration_test.cpp
//
// Integration-layer tests for HA recovery scenarios.
// Uses real backends via TEST_P parameterization (OpLogStoreType +
// snapshot_backend). Tests through HotStandbyService for end-to-end
// validation.

#include <gtest/gtest.h>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <mutex>
#include <random>
#include <thread>

#include <xxhash.h>
#include <ylt/struct_pack.hpp>

#include "hot_standby_service.h"
#include "oplog_manager.h"
#include "oplog_store_factory.h"
#include "serialize/serializer_backend.h"
#include "serializer_snapshot_provider.h"
#include "standby_state_machine.h"
#include "types.h"

#include "hot_standby_ut/mock_snapshot_provider.h"

#ifdef STORE_USE_ETCD
#include "etcd_helper.h"
#endif

DEFINE_string(etcd_endpoints, "",
              "etcd endpoints for HA recovery integration tests");

using namespace mooncake;
using namespace mooncake::test;

// ============================================================
// Helper: struct_pack-serialized payload (same as MasterService)
// ============================================================

static std::string MakeValidPayload(uint64_t client_first = 1,
                                    uint64_t client_second = 2,
                                    uint64_t size = 1024) {
    MetadataPayload payload;
    payload.client_id = {client_first, client_second};
    payload.size = size;
    auto result = struct_pack::serialize(payload);
    return std::string(result.begin(), result.end());
}

// ============================================================
// Test Configuration
// ============================================================

struct HaRecoveryTestConfig {
    OpLogStoreType oplog_store_type;
    std::string snapshot_backend;  // "mock", "etcd", "local"
};

std::string TestConfigName(
    const ::testing::TestParamInfo<HaRecoveryTestConfig>& info) {
    return OpLogStoreTypeToString(info.param.oplog_store_type) + "_snap_" +
           info.param.snapshot_backend;
}

// ============================================================
// RAII Guard
// ============================================================

class StandbyServiceGuard {
   public:
    explicit StandbyServiceGuard(HotStandbyService* svc) : svc_(svc) {}
    ~StandbyServiceGuard() {
        if (svc_) {
            svc_->Stop();
            std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        }
    }
    StandbyServiceGuard(const StandbyServiceGuard&) = delete;
    StandbyServiceGuard& operator=(const StandbyServiceGuard&) = delete;

   private:
    HotStandbyService* svc_;
};

// ============================================================
// Fixture
// ============================================================

class HaRecoveryIntegrationTest
    : public ::testing::TestWithParam<HaRecoveryTestConfig> {
   protected:
    static std::mutex test_mutex_;
    std::unique_ptr<std::lock_guard<std::mutex>> test_lock_;

    void SetUp() override {
        auto config = GetParam();
#ifdef STORE_USE_ETCD
        if (config.oplog_store_type == OpLogStoreType::ETCD) {
            if (FLAGS_etcd_endpoints.empty()) {
                GTEST_SKIP() << "etcd_endpoints not provided";
            }
            EtcdHelper::ConnectToEtcdStoreClient(FLAGS_etcd_endpoints);
        }
#else
        GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif

        test_lock_ = std::make_unique<std::lock_guard<std::mutex>>(test_mutex_);

        // Unique cluster ID for test isolation
        std::mt19937 rng(std::random_device{}());
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        cluster_id_ =
            "ha_recovery_" + std::to_string(ts) + "_" + std::to_string(rng());
        snapshot_root_ = "ha_test_snap_" + cluster_id_;

        // Writer-side OpLogStore via OpLogManager
        auto writer_store = OpLogStoreFactory::Create(
            config.oplog_store_type, cluster_id_, OpLogStoreRole::WRITER);
        ASSERT_NE(writer_store, nullptr);
        ASSERT_EQ(writer_store->Init(), ErrorCode::OK);

        primary_oplog_ = std::make_unique<OpLogManager>();
        primary_oplog_->SetOpLogStore(
            std::shared_ptr<OpLogStore>(std::move(writer_store)));

        // Snapshot backend
        if (config.snapshot_backend == "mock") {
            mock_snapshot_ = std::make_shared<MockSnapshotProvider>();
        } else {
#ifdef STORE_USE_ETCD
            auto backend_type =
                ParseSnapshotBackendType(config.snapshot_backend);
            snapshot_write_backend_ =
                SerializerBackend::Create(backend_type, FLAGS_etcd_endpoints);
            ASSERT_NE(snapshot_write_backend_, nullptr);
#else
            GTEST_SKIP() << "Real snapshot backend requires STORE_USE_ETCD";
#endif
        }
    }

    void TearDown() override {
#ifdef STORE_USE_ETCD
        CleanupTestData();
#endif
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        test_lock_.reset();
    }

    // --- Write helpers ---

    std::vector<uint64_t> WriteEntries(int count) {
        std::vector<uint64_t> seq_ids;
        for (int i = 0; i < count; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string payload = MakeValidPayload();
            uint64_t seq =
                primary_oplog_->Append(OpType::PUT_END, key, payload);
            seq_ids.push_back(seq);
        }
        // Wait for async writes to flush
        std::this_thread::sleep_for(std::chrono::milliseconds(3000));
        return seq_ids;
    }

    // --- Snapshot helpers ---

    std::vector<std::pair<std::string, StandbyObjectMetadata>>
    BuildSnapshotData(int count, uint64_t base_seq = 1) {
        std::vector<std::pair<std::string, StandbyObjectMetadata>> data;
        for (int i = 0; i < count; ++i) {
            StandbyObjectMetadata meta;
            meta.client_id = {1, 2};
            meta.size = 1024;
            meta.last_sequence_id = base_seq + i;
            data.emplace_back("key_" + std::to_string(i), meta);
        }
        return data;
    }

    void InjectSnapshot(
        const std::string& snap_id, uint64_t seq_id,
        const std::vector<std::pair<std::string, StandbyObjectMetadata>>&
            data) {
        if (mock_snapshot_) {
            mock_snapshot_->SetSnapshot(snap_id, seq_id, data);
        } else {
            WriteSnapshotViaBackend(snap_id, seq_id, data);
        }
    }

    std::unique_ptr<SnapshotProvider> MakeSnapshotProvider() {
        if (mock_snapshot_) {
            return std::make_unique<MockSnapshotProvider>(*mock_snapshot_);
        }
        auto config = GetParam();
        auto backend_type = ParseSnapshotBackendType(config.snapshot_backend);
        return std::make_unique<SerializerBackendSnapshotProvider>(
            backend_type, FLAGS_etcd_endpoints, BufferAllocatorType::OFFSET,
            snapshot_root_);
    }

    // --- Standby helpers ---

    std::unique_ptr<HotStandbyService> StartStandby(
        bool enable_snapshot = true) {
        auto config = GetParam();
        HotStandbyConfig hs_config;
        hs_config.enable_verification = false;
        hs_config.max_replication_lag_entries = 1000;
        hs_config.enable_snapshot_bootstrap = enable_snapshot;
        hs_config.oplog_store_type = config.oplog_store_type;

        auto standby = std::make_unique<HotStandbyService>(hs_config);
        if (enable_snapshot) {
            standby->SetSnapshotProvider(MakeSnapshotProvider());
        }

        ErrorCode err = standby->Start(
            /*primary_address=*/"", FLAGS_etcd_endpoints, cluster_id_);
        EXPECT_EQ(ErrorCode::OK, err);
        if (err != ErrorCode::OK) return nullptr;
        return standby;
    }

    bool WaitForStandbySync(HotStandbyService* standby, uint64_t target_seq,
                            int timeout_sec = 30) {
        auto deadline = std::chrono::steady_clock::now() +
                        std::chrono::seconds(timeout_sec);
        while (std::chrono::steady_clock::now() < deadline) {
            auto status = standby->GetSyncStatus();
            LOG(INFO) << "Standby: state=" << StandbyStateToString(status.state)
                      << " applied=" << status.applied_seq_id
                      << " target=" << target_seq;
            if (status.state == StandbyState::WATCHING &&
                status.lag_entries == 0 &&
                status.applied_seq_id >= target_seq) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        return false;
    }

    void VerifyStandbyMetadata(HotStandbyService* standby,
                               size_t expected_count) {
        std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
        ASSERT_TRUE(standby->ExportMetadataSnapshot(snapshot));
        EXPECT_EQ(snapshot.size(), expected_count);
    }

   private:
    void WriteSnapshotViaBackend(
        const std::string& snap_id, uint64_t seq_id,
        const std::vector<std::pair<std::string, StandbyObjectMetadata>>&
        /*data*/) {
        // Write manifest in MasterService::PersistSnapshot format
        std::string prefix = snapshot_root_ + "/" + snap_id + "/";

        auto ts = std::chrono::duration_cast<std::chrono::seconds>(
                      std::chrono::system_clock::now().time_since_epoch())
                      .count();
        std::string manifest = "struct_pack|1|" + snap_id + "|0|0|0|0|" +
                               std::to_string(ts) + "|complete|" +
                               std::to_string(seq_id);
        auto r1 = snapshot_write_backend_->UploadString(prefix + "manifest.txt",
                                                        manifest);
        ASSERT_TRUE(r1.has_value()) << r1.error();

        auto r2 = snapshot_write_backend_->UploadString(
            snapshot_root_ + "/latest.txt", snap_id);
        ASSERT_TRUE(r2.has_value()) << r2.error();

        LOG(INFO) << "Wrote snapshot via backend: " << snap_id
                  << " @ seq=" << seq_id;
    }

    void CleanupTestData() {
#ifdef STORE_USE_ETCD
        std::string oplog_prefix = "/oplog/" + cluster_id_ + "/";
        auto end = oplog_prefix;
        end.back()++;
        EtcdHelper::DeleteRange(oplog_prefix.c_str(), oplog_prefix.size(),
                                end.c_str(), end.size());
        if (snapshot_write_backend_) {
            snapshot_write_backend_->DeleteObjectsWithPrefix(snapshot_root_);
        }
#endif
    }

   protected:
    std::string cluster_id_;
    std::string snapshot_root_;
    std::unique_ptr<OpLogManager> primary_oplog_;

    std::shared_ptr<MockSnapshotProvider> mock_snapshot_;
    std::unique_ptr<SerializerBackend> snapshot_write_backend_;
};

std::mutex HaRecoveryIntegrationTest::test_mutex_;

// ============================================================
// Parameterization
// ============================================================

#ifdef STORE_USE_ETCD
INSTANTIATE_TEST_SUITE_P(
    Etcd, HaRecoveryIntegrationTest,
    ::testing::Values(HaRecoveryTestConfig{OpLogStoreType::ETCD, "mock"},
                      HaRecoveryTestConfig{OpLogStoreType::ETCD, "etcd"}),
    TestConfigName);
#endif

// ============================================================
// Scenario 1: OpLog + Snapshot Joint Recovery (E2E)
// ============================================================

TEST_P(HaRecoveryIntegrationTest, E2E_SnapshotThenOpLogReplay) {
    if (GetParam().snapshot_backend != "mock") {
        GTEST_SKIP() << "Snapshot injection requires mock backend; "
                     << "real backends need MasterService to persist snapshots";
    }
    auto seq_ids = WriteEntries(20);
    ASSERT_EQ(seq_ids.size(), 20u);
    uint64_t last_seq = seq_ids.back();

    auto snap_data = BuildSnapshotData(10);
    InjectSnapshot("snap_" + cluster_id_, 10, snap_data);

    auto standby = StartStandby(/*enable_snapshot=*/true);
    ASSERT_NE(standby, nullptr);
    StandbyServiceGuard guard(standby.get());

    ASSERT_TRUE(WaitForStandbySync(standby.get(), last_seq))
        << "Standby did not sync to seq " << last_seq;
    VerifyStandbyMetadata(standby.get(), 20);
}

TEST_P(HaRecoveryIntegrationTest, E2E_SnapshotRecoveryThenWatch) {
    if (GetParam().snapshot_backend != "mock") {
        GTEST_SKIP() << "Snapshot injection requires mock backend; "
                     << "real backends need MasterService to persist snapshots";
    }
    auto seq_ids = WriteEntries(20);
    uint64_t first_batch_last = seq_ids.back();

    auto snap_data = BuildSnapshotData(10);
    InjectSnapshot("snap_" + cluster_id_, 10, snap_data);

    auto standby = StartStandby(/*enable_snapshot=*/true);
    ASSERT_NE(standby, nullptr);
    StandbyServiceGuard guard(standby.get());

    ASSERT_TRUE(WaitForStandbySync(standby.get(), first_batch_last));

    // Primary writes 10 more (keys key_20..key_29)
    for (int i = 20; i < 30; ++i) {
        primary_oplog_->Append(OpType::PUT_END, "key_" + std::to_string(i),
                               MakeValidPayload());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    uint64_t final_seq = primary_oplog_->GetLastSequenceId();
    ASSERT_TRUE(WaitForStandbySync(standby.get(), final_seq, 30));
    VerifyStandbyMetadata(standby.get(), 30);
}

// ============================================================
// Scenario 2: OpLog GC + Snapshot Safety (E2E)
// ============================================================

TEST_P(HaRecoveryIntegrationTest, E2E_GC_StandbyJoinsAfterCleanup) {
    if (GetParam().snapshot_backend != "mock") {
        GTEST_SKIP() << "Snapshot injection requires mock backend; "
                     << "real backends need MasterService to persist snapshots";
    }
    auto seq_ids = WriteEntries(20);

    auto snap_data = BuildSnapshotData(10);
    InjectSnapshot("snap_" + cluster_id_, 10, snap_data);

    // GC seq 1~9
    auto writer = OpLogStoreFactory::Create(
        GetParam().oplog_store_type, cluster_id_, OpLogStoreRole::WRITER);
    ASSERT_NE(writer, nullptr);
    ASSERT_EQ(writer->Init(), ErrorCode::OK);
    ASSERT_EQ(writer->CleanupOpLogBefore(10), ErrorCode::OK);

    auto standby = StartStandby(/*enable_snapshot=*/true);
    ASSERT_NE(standby, nullptr);
    StandbyServiceGuard guard(standby.get());

    ASSERT_TRUE(WaitForStandbySync(standby.get(), seq_ids.back()));
    VerifyStandbyMetadata(standby.get(), 20);
}

TEST_P(HaRecoveryIntegrationTest, E2E_GC_WithoutSnapshot_PartialData) {
    auto seq_ids = WriteEntries(20);

    // GC seq 1~9, no snapshot
    auto writer = OpLogStoreFactory::Create(
        GetParam().oplog_store_type, cluster_id_, OpLogStoreRole::WRITER);
    ASSERT_NE(writer, nullptr);
    ASSERT_EQ(writer->Init(), ErrorCode::OK);
    ASSERT_EQ(writer->CleanupOpLogBefore(10), ErrorCode::OK);

    auto standby = StartStandby(/*enable_snapshot=*/false);
    ASSERT_NE(standby, nullptr);
    StandbyServiceGuard guard(standby.get());

    // Negative test: without snapshot + GC, standby cannot recover GC'd
    // entries. The OpLogApplier expects seq=1 but receives seq>=10, creating an
    // unresolvable gap. The gap resolution (ProcessPendingEntries) is only
    // triggered after a successful apply, which never happens here.
    // Verify the standby does NOT sync within a short timeout.
    EXPECT_FALSE(WaitForStandbySync(standby.get(), seq_ids.back(), 10));

    // Verify standby state is still WATCHING (not crashed)
    auto status = standby->GetSyncStatus();
    EXPECT_EQ(status.state, StandbyState::WATCHING);
    // applied_seq_id should be 0 (no entries applied)
    EXPECT_EQ(status.applied_seq_id, 0u);
}

// ============================================================
// Scenario 3: Promotion Consistency (E2E)
// ============================================================

TEST_P(HaRecoveryIntegrationTest, E2E_PromotionFinalCatchUp) {
    auto seq_ids = WriteEntries(10);
    uint64_t first_batch_last = seq_ids.back();

    auto standby = StartStandby(/*enable_snapshot=*/false);
    ASSERT_NE(standby, nullptr);
    StandbyServiceGuard guard(standby.get());

    ASSERT_TRUE(WaitForStandbySync(standby.get(), first_batch_last));

    // Primary writes 10 more
    for (int i = 10; i < 20; ++i) {
        primary_oplog_->Append(OpType::PUT_END, "key_" + std::to_string(i),
                               MakeValidPayload());
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(3000));

    ErrorCode err = standby->Promote();
    ASSERT_EQ(ErrorCode::OK, err);
    VerifyStandbyMetadata(standby.get(), 20);
}

TEST_P(HaRecoveryIntegrationTest, E2E_PromotionWithConcurrentWrites) {
    auto seq_ids = WriteEntries(10);
    uint64_t first_batch_last = seq_ids.back();

    auto standby = StartStandby(/*enable_snapshot=*/false);
    ASSERT_NE(standby, nullptr);
    StandbyServiceGuard guard(standby.get());

    ASSERT_TRUE(WaitForStandbySync(standby.get(), first_batch_last));

    // Concurrent writer thread
    std::atomic<bool> stop_writing{false};
    std::atomic<uint64_t> write_count{0};
    std::thread writer_thread([&]() {
        int idx = 10;
        while (!stop_writing.load()) {
            primary_oplog_->Append(OpType::PUT_END,
                                   "key_" + std::to_string(idx),
                                   MakeValidPayload());
            ++idx;
            write_count.fetch_add(1);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    ErrorCode err = standby->Promote();
    stop_writing.store(true);
    writer_thread.join();

    ASSERT_EQ(ErrorCode::OK, err);

    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;
    ASSERT_TRUE(standby->ExportMetadataSnapshot(snapshot));
    EXPECT_GE(snapshot.size(), 10u);

    LOG(INFO) << "Promotion with concurrent writes: writer wrote "
              << write_count.load() << " extra, standby has "
              << snapshot.size();
}

// ============================================================
// main() — parse gflags for --etcd_endpoints
// ============================================================

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = 1;
    return RUN_ALL_TESTS();
}
