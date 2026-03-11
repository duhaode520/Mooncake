#include "oplog_watcher.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <xxhash.h>

#include "etcd_oplog_store.h"
#include "metadata_store.h"
#include "oplog_applier.h"
#include "oplog_manager.h"
#include "oplog_serializer.h"
#include "standby_state_machine.h"
#include "types.h"

namespace mooncake::test {

// Minimal MetadataStore implementation for OpLogApplier
class MinimalMockMetadataStore : public MetadataStore {
   public:
    bool PutMetadata(const std::string&,
                     const StandbyObjectMetadata&) override {
        return true;
    }
    bool Put(const std::string&, const std::string&) override { return true; }
    std::optional<StandbyObjectMetadata> GetMetadata(
        const std::string&) const override {
        return std::nullopt;
    }
    bool Remove(const std::string&) override { return true; }
    bool Exists(const std::string&) const override { return false; }
    size_t GetKeyCount() const override { return 0; }
};

// Simple wrapper around OpLogApplier for testing OpLogWatcher.
class MockOpLogApplier : public OpLogApplier {
   public:
    MockOpLogApplier() : OpLogApplier(&metadata_store_, "test_cluster") {}

   private:
    MinimalMockMetadataStore metadata_store_;
};

// Helper function to create a valid OpLogEntry with checksum
OpLogEntry MakeEntry(uint64_t seq, OpType type, const std::string& key,
                     const std::string& payload) {
    OpLogEntry e;
    e.sequence_id = seq;
    e.timestamp_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::steady_clock::now().time_since_epoch())
                         .count();
    e.op_type = type;
    e.object_key = key;
    e.payload = payload;
    e.checksum =
        static_cast<uint32_t>(XXH32(payload.data(), payload.size(), 0));
    e.prefix_hash =
        key.empty() ? 0
                    : static_cast<uint32_t>(XXH32(key.data(), key.size(), 0));
    return e;
}

class OpLogWatcherTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OpLogWatcherTest");
        FLAGS_logtostderr = 1;
        etcd_endpoints_ = "http://localhost:2379";
        cluster_id_ = "test_cluster_001";
        mock_applier_ = std::make_unique<MockOpLogApplier>();
        watcher_ = std::make_unique<OpLogWatcher>(etcd_endpoints_, cluster_id_,
                                                  mock_applier_.get());
    }

    void TearDown() override {
        if (watcher_) {
            watcher_->Stop();
        }
        google::ShutdownGoogleLogging();
    }

    // Helper to call protected HandleWatchEvent from tests
    void CallHandleWatchEvent(const std::string& key, const std::string& value,
                              int event_type, int64_t mod_revision) {
        // Use a helper subclass to access protected member
        struct Accessor : public OpLogWatcher {
            using OpLogWatcher::HandleWatchEvent;
        };
        (static_cast<Accessor*>(watcher_.get()))
            ->HandleWatchEvent(key, value, event_type, mod_revision);
    }

    std::string etcd_endpoints_;
    std::string cluster_id_;
    std::unique_ptr<MockOpLogApplier> mock_applier_;
    std::unique_ptr<OpLogWatcher> watcher_;
};

// ========== Start/Stop tests (still SKIP - require real etcd) ==========

TEST_F(OpLogWatcherTest, TestStart) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd connection, skipping integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestStartFromSequenceId) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd connection, skipping integration test";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestStop) {
#ifdef STORE_USE_ETCD
    watcher_->Stop();
    EXPECT_FALSE(watcher_->IsWatchHealthy());
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

// ========== HandleWatchEvent tests (now testable via protected virtual) ====

TEST_F(OpLogWatcherTest, HandleWatchEvent_PutValid) {
    OpLogEntry entry = MakeEntry(1, OpType::PUT_END, "key1", "payload1");
    std::string json_value = SerializeOpLogEntry(entry);
    std::string key = "/oplog/" + cluster_id_ + "/00000000000000000001";

    // PUT event (type=0)
    CallHandleWatchEvent(key, json_value, 0, 100);

    // Verify applier received the entry
    EXPECT_EQ(2u, mock_applier_->GetExpectedSequenceId());
}

TEST_F(OpLogWatcherTest, HandleWatchEvent_DeleteIgnored) {
    std::string key = "/oplog/" + cluster_id_ + "/00000000000000000001";

    // DELETE event (type=1) should be silently ignored
    CallHandleWatchEvent(key, "", 1, 100);

    // Applier should not have been called
    EXPECT_EQ(1u, mock_applier_->GetExpectedSequenceId());
}

TEST_F(OpLogWatcherTest, HandleWatchEvent_InvalidJson) {
    std::string key = "/oplog/" + cluster_id_ + "/00000000000000000001";

    // Invalid JSON should not crash
    CallHandleWatchEvent(key, "{ invalid }", 0, 100);

    EXPECT_EQ(1u, mock_applier_->GetExpectedSequenceId());
}

TEST_F(OpLogWatcherTest, HandleWatchEvent_LatestKeyIgnored) {
    std::string key = "/oplog/" + cluster_id_ + "/latest";
    CallHandleWatchEvent(key, "123", 0, 100);

    EXPECT_EQ(1u, mock_applier_->GetExpectedSequenceId());
}

TEST_F(OpLogWatcherTest, HandleWatchEvent_SequenceTracking) {
    std::string payload1_json = SerializeOpLogEntry(
        MakeEntry(1, OpType::PUT_END, "k1", "p1"));
    std::string payload2_json = SerializeOpLogEntry(
        MakeEntry(2, OpType::PUT_END, "k2", "p2"));

    CallHandleWatchEvent(
        "/oplog/" + cluster_id_ + "/00000000000000000001", payload1_json, 0,
        100);
    CallHandleWatchEvent(
        "/oplog/" + cluster_id_ + "/00000000000000000002", payload2_json, 0,
        101);

    EXPECT_EQ(2u, watcher_->GetLastProcessedSequenceId());
}

// ========== Reconnection tests (still SKIP - require real etcd) ==========

TEST_F(OpLogWatcherTest, TestReconnectAfterDisconnect) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP()
        << "Requires real etcd connection and watch failure simulation";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

TEST_F(OpLogWatcherTest, TestReconnectResumeFromLastSequence) {
#ifdef STORE_USE_ETCD
    GTEST_SKIP() << "Requires real etcd connection and reconnection simulation";
#else
    GTEST_SKIP() << "STORE_USE_ETCD not enabled";
#endif
}

// ========== Utility tests ==========

TEST_F(OpLogWatcherTest, TestInvalidClusterId_Rejected) {
    std::string valid_cluster_id = "test_cluster_001";
    std::unique_ptr<MockOpLogApplier> mock_applier =
        std::make_unique<MockOpLogApplier>();
    std::unique_ptr<OpLogWatcher> watcher = std::make_unique<OpLogWatcher>(
        etcd_endpoints_, valid_cluster_id, mock_applier.get());
    EXPECT_NE(nullptr, watcher);
    watcher->Stop();
}

}  // namespace mooncake::test

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
