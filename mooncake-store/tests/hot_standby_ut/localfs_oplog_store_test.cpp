#include <gtest/gtest.h>
#include <glog/logging.h>
#include <filesystem>
#include <fstream>

#include "localfs_oplog_store.h"
#include "oplog_serializer.h"

namespace fs = std::filesystem;

namespace mooncake {
namespace {

class LocalFsOpLogStoreTest : public ::testing::Test {
   protected:
    void SetUp() override {
        test_dir_ = "/tmp/localfs_oplog_test_" +
                     std::to_string(::getpid()) + "_" +
                     std::to_string(test_counter_++);
        fs::create_directories(test_dir_);
        cluster_id_ = "test_cluster";
    }

    void TearDown() override {
        fs::remove_all(test_dir_);
    }

    std::unique_ptr<LocalFsOpLogStore> CreateWriter() {
        auto store = std::make_unique<LocalFsOpLogStore>(
            cluster_id_, test_dir_, /*enable_batch_write=*/true);
        EXPECT_EQ(ErrorCode::OK, store->Init());
        return store;
    }

    std::unique_ptr<LocalFsOpLogStore> CreateReader() {
        auto store = std::make_unique<LocalFsOpLogStore>(
            cluster_id_, test_dir_, /*enable_batch_write=*/false);
        EXPECT_EQ(ErrorCode::OK, store->Init());
        return store;
    }

    OpLogEntry MakeEntry(uint64_t seq_id,
                         const std::string& key = "test_key") {
        OpLogEntry entry;
        entry.sequence_id = seq_id;
        entry.op_type = OpType::PUT_END;
        entry.object_key = key;
        entry.payload = "payload_" + std::to_string(seq_id);
        return entry;
    }

    std::string test_dir_;
    std::string cluster_id_;
    static inline int test_counter_ = 0;
};

// --- Init tests ---

TEST_F(LocalFsOpLogStoreTest, InitCreatesDirectoryStructure) {
    auto store = CreateWriter();
    EXPECT_TRUE(fs::exists(test_dir_ + "/" + cluster_id_ + "/segments"));
    EXPECT_TRUE(fs::exists(test_dir_ + "/" + cluster_id_ + "/snapshots"));
    EXPECT_TRUE(fs::exists(test_dir_ + "/" + cluster_id_ + "/latest"));
}

TEST_F(LocalFsOpLogStoreTest, InitWriterCreatesLatestFile) {
    auto store = CreateWriter();
    std::string latest_path = test_dir_ + "/" + cluster_id_ + "/latest";
    std::ifstream f(latest_path);
    std::string content;
    f >> content;
    EXPECT_EQ("0", content);
}

TEST_F(LocalFsOpLogStoreTest, InitReaderDoesNotCreateLatestFile) {
    // Remove the test dir so Init starts fresh
    fs::remove_all(test_dir_);
    fs::create_directories(test_dir_);
    auto store = std::make_unique<LocalFsOpLogStore>(
        cluster_id_, test_dir_, /*enable_batch_write=*/false);
    EXPECT_EQ(ErrorCode::OK, store->Init());
    // Reader should create dirs but NOT create latest file
    EXPECT_TRUE(fs::exists(test_dir_ + "/" + cluster_id_ + "/segments"));
    EXPECT_FALSE(fs::exists(test_dir_ + "/" + cluster_id_ + "/latest"));
}

TEST_F(LocalFsOpLogStoreTest, InitCleansTmpFiles) {
    // Create directory structure manually
    std::string seg_dir = test_dir_ + "/" + cluster_id_ + "/segments";
    fs::create_directories(seg_dir);
    // Create a stale .tmp file
    std::ofstream(seg_dir +
                  "/seg_00000000000000000001_00000000000000000010.tmp")
        << "stale data";
    EXPECT_TRUE(fs::exists(
        seg_dir + "/seg_00000000000000000001_00000000000000000010.tmp"));

    auto store = CreateWriter();
    EXPECT_FALSE(fs::exists(
        seg_dir + "/seg_00000000000000000001_00000000000000000010.tmp"));
}

}  // namespace
}  // namespace mooncake
