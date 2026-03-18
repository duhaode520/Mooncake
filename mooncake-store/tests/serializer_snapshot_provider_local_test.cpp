#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <string>
#include <utility>
#include <vector>

#include <unistd.h>

#include "master_service_test_for_snapshot_base.h"
#include "serializer_snapshot_provider.h"

namespace mooncake::test {

class SerializerSnapshotProviderLocalTest : public MasterServiceSnapshotTestBase {
   protected:
    static bool glog_initialized_;

    void SetUp() override {
        MasterServiceSnapshotTestBase::SetUp();

        if (!glog_initialized_) {
            google::InitGoogleLogging("SerializerSnapshotProviderLocalTest");
            FLAGS_logtostderr = true;
            glog_initialized_ = true;
        }

        snapshot_local_path_ = std::string("/tmp/mooncake_snapshots_provider_test_") +
                               std::to_string(static_cast<long>(::getpid()));
        std::error_code ec;
        std::filesystem::remove_all(snapshot_local_path_, ec);
        std::filesystem::create_directories(snapshot_local_path_, ec);

        ::setenv("SNAPSHOT_LOCAL_PATH", snapshot_local_path_.c_str(), 1);
    }

    void TearDown() override {
        service_.reset();

        ::unsetenv("SNAPSHOT_LOCAL_PATH");

        std::error_code ec;
        std::filesystem::remove_all(snapshot_local_path_, ec);
    }

    std::string snapshot_local_path_;
};

bool SerializerSnapshotProviderLocalTest::glog_initialized_ = false;

TEST_F(SerializerSnapshotProviderLocalTest,
       LoadLatestSnapshot_NonEmpty_ReturnsTrueAndContainsKey) {
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_snapshot_backend_type(SnapshotBackendType::LOCAL_FILE)
                              .set_enable_snapshot(false)
                              .build();

    service_.reset(new MasterService(service_config));

    const auto ctx = PrepareSimpleSegment(*service_, "seg_for_provider");
    const std::string key = "provider_key_1";

    ReplicateConfig repl;
    repl.replica_num = 1;
    auto put_start = service_->PutStart(ctx.client_id, key, 1024, repl);
    ASSERT_TRUE(put_start.has_value());
    ASSERT_TRUE(service_->PutEnd(ctx.client_id, key, ReplicaType::MEMORY).has_value());

    const std::string snapshot_id = GenerateSnapshotId();
    auto persist = CallPersistState(service_.get(), snapshot_id);
    ASSERT_TRUE(persist.has_value()) << persist.error().message;

    SerializerBackendSnapshotProvider provider(SnapshotBackendType::LOCAL_FILE, "",
                                              BufferAllocatorType::OFFSET);

    std::string loaded_snapshot_id;
    uint64_t snapshot_seq_id = 0;
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;

    ASSERT_TRUE(provider.LoadLatestSnapshot("ignored_cluster", loaded_snapshot_id,
                                           snapshot_seq_id, snapshot));
    EXPECT_EQ(loaded_snapshot_id, snapshot_id);
    EXPECT_GT(snapshot_seq_id, 0u);

    bool found = false;
    for (const auto& kv : snapshot) {
        if (kv.first == key) {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found);
}

TEST_F(SerializerSnapshotProviderLocalTest,
       LoadLatestSnapshot_EmptySnapshot_ReturnsTrueWithEmptyData) {
    auto service_config = MasterServiceConfig::builder()
                              .set_memory_allocator(BufferAllocatorType::OFFSET)
                              .set_snapshot_backend_type(SnapshotBackendType::LOCAL_FILE)
                              .set_enable_snapshot(false)
                              .build();

    service_.reset(new MasterService(service_config));

    const std::string snapshot_id = GenerateSnapshotId();
    auto persist = CallPersistState(service_.get(), snapshot_id);
    ASSERT_TRUE(persist.has_value()) << persist.error().message;

    SerializerBackendSnapshotProvider provider(SnapshotBackendType::LOCAL_FILE, "",
                                              BufferAllocatorType::OFFSET);

    std::string loaded_snapshot_id;
    uint64_t snapshot_seq_id = 0;
    std::vector<std::pair<std::string, StandbyObjectMetadata>> snapshot;

    ASSERT_TRUE(provider.LoadLatestSnapshot("ignored_cluster", loaded_snapshot_id,
                                           snapshot_seq_id, snapshot));
    EXPECT_EQ(loaded_snapshot_id, snapshot_id);
    EXPECT_TRUE(snapshot.empty());
}

}  // namespace mooncake::test
