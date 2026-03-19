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
#include <filesystem>
#include <mutex>
#include <random>
#include <thread>

#include <xxhash.h>
#include <ylt/struct_pack.hpp>

#include "hot_standby_service.h"
#include "master_service.h"
#include "oplog_manager.h"
#include "oplog_store_factory.h"
#include "segment.h"
#include "serialize/serializer_backend.h"
#include "serializer_snapshot_provider.h"
#include "standby_state_machine.h"
#include "types.h"

#include "hot_standby_ut/mock_snapshot_provider.h"
#include "master_service_test_for_snapshot_base.h"

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
// Helper: persist snapshot via MasterService
// ============================================================

// Expose MasterServiceSnapshotTestBase::CallPersistState (protected)
struct SnapshotPersistHelper
    : public mooncake::test::MasterServiceSnapshotTestBase {
    void TestBody() override {}  // satisfy pure virtual
    using MasterServiceSnapshotTestBase::CallPersistState;
    using MasterServiceSnapshotTestBase::GenerateSnapshotId;
};

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

        // ETCD backend requires etcd connection
        if (config.oplog_store_type == OpLogStoreType::ETCD) {
#ifdef STORE_USE_ETCD
            if (FLAGS_etcd_endpoints.empty()) {
                GTEST_SKIP() << "etcd_endpoints not provided";
            }
            EtcdHelper::ConnectToEtcdStoreClient(FLAGS_etcd_endpoints);
#else
            GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
        }

        test_lock_ = std::make_unique<std::lock_guard<std::mutex>>(test_mutex_);

        // Unique cluster ID for test isolation
        std::mt19937 rng(std::random_device{}());
        auto ts = std::chrono::steady_clock::now().time_since_epoch().count();
        cluster_id_ =
            "ha_recovery_" + std::to_string(ts) + "_" + std::to_string(rng());

        // LocalFS backend: create a unique temp directory
        if (config.oplog_store_type == OpLogStoreType::LOCAL_FS) {
            static std::atomic<int> dir_counter{0};
            localfs_test_dir_ = "/tmp/ha_recovery_localfs_" +
                                std::to_string(getpid()) + "_" +
                                std::to_string(dir_counter.fetch_add(1));
            std::filesystem::create_directories(localfs_test_dir_);
        }

        // Writer-side OpLogStore via OpLogManager
        auto writer_store = OpLogStoreFactory::Create(
            config.oplog_store_type, cluster_id_, OpLogStoreRole::WRITER,
            localfs_test_dir_, kLocalFsPollIntervalMs);
        ASSERT_NE(writer_store, nullptr);

        primary_oplog_ = std::make_unique<OpLogManager>();
        primary_oplog_->SetOpLogStore(
            std::shared_ptr<OpLogStore>(std::move(writer_store)));

        // Snapshot backend
        if (config.snapshot_backend == "mock") {
            mock_snapshot_ = std::make_shared<MockSnapshotProvider>();
        } else if (config.snapshot_backend == "local") {
            // LOCAL_FILE snapshot backend — no etcd required
            snapshot_root_ = "mooncake_master_snapshot";
            static std::atomic<int> snap_counter{0};
            localfs_snapshot_dir_ = "/tmp/ha_recovery_snap_" +
                                    std::to_string(getpid()) + "_" +
                                    std::to_string(snap_counter.fetch_add(1));
            std::filesystem::create_directories(localfs_snapshot_dir_);
            setenv("MOONCAKE_SNAPSHOT_LOCAL_PATH", localfs_snapshot_dir_.c_str(), 1);
            auto backend_type =
                ParseSnapshotBackendType(config.snapshot_backend);
            snapshot_write_backend_ =
                SerializerBackend::Create(backend_type, "");
            ASSERT_NE(snapshot_write_backend_, nullptr);
        } else {
#ifdef STORE_USE_ETCD
            // Real backends use "mooncake_master_snapshot" root (matches
            // MasterService::SNAPSHOT_ROOT used by RestoreState)
            snapshot_root_ = "mooncake_master_snapshot";
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
        // Clean up LocalFS temp directory
        if (!localfs_test_dir_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(localfs_test_dir_, ec);
            if (ec) {
                LOG(WARNING) << "Failed to remove localfs test dir "
                             << localfs_test_dir_ << ": " << ec.message();
            }
        }
        // Clean up local snapshot directory
        if (!localfs_snapshot_dir_.empty()) {
            std::error_code ec;
            std::filesystem::remove_all(localfs_snapshot_dir_, ec);
            if (ec) {
                LOG(WARNING) << "Failed to remove snapshot dir "
                             << localfs_snapshot_dir_ << ": " << ec.message();
            }
            unsetenv("MOONCAKE_SNAPSHOT_LOCAL_PATH");
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        test_lock_.reset();
    }

    // --- Write helpers ---

    std::vector<uint64_t> WriteEntries(int count) {
        std::vector<uint64_t> seq_ids;
        for (int i = 0; i < count; ++i) {
            std::string key = "key_" + std::to_string(i);
            std::string payload = MakeValidPayload();
            if (i < count - 1) {
                uint64_t seq =
                    primary_oplog_->Append(OpType::PUT_END, key, payload);
                seq_ids.push_back(seq);
            } else {
                // Use AppendAndPersist for the last entry to force flush
                // (LocalFS batch writer is async)
                auto result = primary_oplog_->AppendAndPersist(OpType::PUT_END,
                                                               key, payload);
                EXPECT_TRUE(result.has_value())
                    << "AppendAndPersist failed for last entry";
                seq_ids.push_back(result.has_value()
                                      ? result.value()
                                      : primary_oplog_->GetLastSequenceId());
            }
        }
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
        std::string etcd_ep = (backend_type == SnapshotBackendType::LOCAL_FILE)
                                  ? ""
                                  : FLAGS_etcd_endpoints;
        return std::make_unique<SerializerBackendSnapshotProvider>(
            backend_type, etcd_ep, BufferAllocatorType::OFFSET, snapshot_root_);
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
        if (config.oplog_store_type == OpLogStoreType::LOCAL_FS) {
            hs_config.oplog_store_root_dir = localfs_test_dir_;
            hs_config.oplog_poll_interval_ms = kLocalFsPollIntervalMs;
        }

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
        const std::string& /*snap_id*/, uint64_t /*seq_id*/,
        const std::vector<std::pair<std::string, StandbyObjectMetadata>>&
            data) {
        // Create a temporary MasterService, populate it with test data,
        // and persist a real snapshot. This ensures the snapshot uses the
        // correct binary format (MessagePack + zstd) that RestoreState
        // expects.
        auto backend_type =
            ParseSnapshotBackendType(GetParam().snapshot_backend);
        std::string etcd_ep = (backend_type == SnapshotBackendType::LOCAL_FILE)
                                  ? ""
                                  : FLAGS_etcd_endpoints;
        std::string snap_backup =
            (backend_type == SnapshotBackendType::LOCAL_FILE)
                ? localfs_snapshot_dir_ + "/backup"
                : "/tmp/ha_recovery_snap_test";
        auto cfg = MasterServiceConfig::builder()
                       .set_enable_ha(false)
                       .set_memory_allocator(BufferAllocatorType::OFFSET)
                       .set_enable_snapshot(true)
                       .set_enable_snapshot_restore(false)
                       .set_snapshot_backend_type(backend_type)
                       .set_etcd_endpoints(etcd_ep)
                       .set_snapshot_backup_dir(snap_backup)
                       .build();

        MasterService service(cfg);

        // Mount a segment to satisfy PutStart allocation
        Segment segment;
        segment.id = generate_uuid();
        segment.name = "test_snapshot_segment";
        segment.base = 0x300000000;       // non-zero required by SegmentManager
        segment.size = 16 * 1024 * 1024;  // 16MB
        segment.te_endpoint = "test_snapshot_segment";
        UUID client_id = generate_uuid();
        auto mount_result = service.MountSegment(segment, client_id);
        ASSERT_TRUE(mount_result.has_value())
            << "MountSegment failed: "
            << static_cast<int>(mount_result.error());

        // Put data through public API. Each PutEnd increments the internal
        // oplog sequence_id, so after N puts the snapshot will have
        // oplog_seq = N (matching InjectSnapshot(snap_id, N, data)).
        ReplicateConfig replicate_config;
        replicate_config.replica_num = 1;

        for (const auto& [key, meta] : data) {
            auto put_start_result =
                service.PutStart(client_id, key, meta.size, replicate_config);
            ASSERT_TRUE(put_start_result.has_value())
                << "PutStart failed for key=" << key;
            auto put_end_result =
                service.PutEnd(client_id, key, ReplicaType::MEMORY);
            ASSERT_TRUE(put_end_result.has_value())
                << "PutEnd failed for key=" << key;
        }

        // Persist snapshot (writes to "master_snapshot/" via
        // MasterService::SNAPSHOT_ROOT)
        auto persist_snap_id = SnapshotPersistHelper().GenerateSnapshotId();
        auto result =
            SnapshotPersistHelper::CallPersistState(&service, persist_snap_id);
        ASSERT_TRUE(result.has_value())
            << "PersistState failed: " << result.error().message;

        LOG(INFO) << "Wrote real snapshot via MasterService: "
                  << persist_snap_id
                  << " @ seq=" << service.GetOpLogLastSequenceId()
                  << ", keys=" << data.size();
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
    std::string localfs_test_dir_;
    std::string localfs_snapshot_dir_;
    std::unique_ptr<OpLogManager> primary_oplog_;

    std::shared_ptr<MockSnapshotProvider> mock_snapshot_;
    std::unique_ptr<SerializerBackend> snapshot_write_backend_;

    static constexpr int kLocalFsPollIntervalMs = 100;
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

INSTANTIATE_TEST_SUITE_P(
    LocalFs, HaRecoveryIntegrationTest,
    ::testing::Values(HaRecoveryTestConfig{OpLogStoreType::LOCAL_FS, "mock"},
                      HaRecoveryTestConfig{OpLogStoreType::LOCAL_FS, "local"}),
    TestConfigName);

// ============================================================
// Scenario 1: OpLog + Snapshot Joint Recovery (E2E)
// ============================================================

TEST_P(HaRecoveryIntegrationTest, E2E_SnapshotThenOpLogReplay) {
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
        std::string key = "key_" + std::to_string(i);
        if (i < 29) {
            primary_oplog_->Append(OpType::PUT_END, key, MakeValidPayload());
        } else {
            auto result = primary_oplog_->AppendAndPersist(OpType::PUT_END, key,
                                                           MakeValidPayload());
            ASSERT_TRUE(result.has_value());
        }
    }

    uint64_t final_seq = primary_oplog_->GetLastSequenceId();
    ASSERT_TRUE(WaitForStandbySync(standby.get(), final_seq, 30));
    VerifyStandbyMetadata(standby.get(), 30);
}

// ============================================================
// Scenario 2: OpLog GC + Snapshot Safety (E2E)
// ============================================================

TEST_P(HaRecoveryIntegrationTest, E2E_GC_StandbyJoinsAfterCleanup) {
    auto seq_ids = WriteEntries(20);

    auto snap_data = BuildSnapshotData(10);
    InjectSnapshot("snap_" + cluster_id_, 10, snap_data);

    // GC seq 1~9
    auto writer = OpLogStoreFactory::Create(
        GetParam().oplog_store_type, cluster_id_, OpLogStoreRole::WRITER,
        localfs_test_dir_, kLocalFsPollIntervalMs);
    ASSERT_NE(writer, nullptr);
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
        GetParam().oplog_store_type, cluster_id_, OpLogStoreRole::WRITER,
        localfs_test_dir_, kLocalFsPollIntervalMs);
    ASSERT_NE(writer, nullptr);
    ASSERT_EQ(writer->CleanupOpLogBefore(10), ErrorCode::OK);

    auto standby = StartStandby(/*enable_snapshot=*/false);
    ASSERT_NE(standby, nullptr);
    StandbyServiceGuard guard(standby.get());

    if (GetParam().oplog_store_type == OpLogStoreType::LOCAL_FS) {
        // LocalFS uses segment files that may contain entries spanning the
        // GC boundary.  CleanupOpLogBefore only removes segments whose
        // max_seq < threshold, so entries may survive GC.  In that case
        // the standby successfully syncs all 20 entries.
        EXPECT_TRUE(WaitForStandbySync(standby.get(), seq_ids.back(), 15));
    } else {
        // ETCD GC truly deletes individual keys, so the standby cannot
        // recover GC'd entries without a snapshot.
        EXPECT_FALSE(WaitForStandbySync(standby.get(), seq_ids.back(), 10));

        // Verify standby state is still WATCHING (not crashed)
        auto status = standby->GetSyncStatus();
        EXPECT_EQ(status.state, StandbyState::WATCHING);
        // applied_seq_id should be 0 (no entries applied)
        EXPECT_EQ(status.applied_seq_id, 0u);
    }
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
        std::string key = "key_" + std::to_string(i);
        if (i < 19) {
            primary_oplog_->Append(OpType::PUT_END, key, MakeValidPayload());
        } else {
            auto result = primary_oplog_->AppendAndPersist(OpType::PUT_END, key,
                                                           MakeValidPayload());
            ASSERT_TRUE(result.has_value());
        }
    }
    // Brief wait to let async batch writes fully persist and give the
    // standby's polling notifier time to pick up new entries before
    // Promote() performs its final catch-up read.
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

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
