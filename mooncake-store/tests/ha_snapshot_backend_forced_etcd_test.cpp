#include <gtest/gtest.h>

#include "master_config.h"

namespace mooncake::test {

#ifdef STORE_USE_ETCD

static mooncake::MasterConfig MakeMinimalMasterConfig() {
    mooncake::MasterConfig cfg{};

    // Required-ish fields for MasterServiceSupervisorConfig::validate()
    cfg.enable_metric_reporting = true;
    cfg.metrics_port = 9003;
    cfg.rpc_port = 50053;
    cfg.rpc_thread_num = 4;
    cfg.rpc_address = "0.0.0.0";
    cfg.rpc_conn_timeout_seconds = 0;
    cfg.rpc_enable_tcp_no_delay = true;

    cfg.default_kv_lease_ttl = 5000;
    cfg.default_kv_soft_pin_ttl = 1800000;
    cfg.allow_evict_soft_pinned_objects = true;
    cfg.eviction_ratio = 0.05;
    cfg.eviction_high_watermark_ratio = 0.95;
    cfg.client_live_ttl_sec = 10;

    cfg.enable_ha = true;
    cfg.enable_offload = false;
    cfg.etcd_endpoints = "http://127.0.0.1:2379";

    cfg.cluster_id = "test_cluster";
    cfg.root_fs_dir = "";
    cfg.global_file_segment_size = 0;
    cfg.memory_allocator = "offset";

    cfg.enable_http_metadata_server = false;
    cfg.http_metadata_server_port = 0;
    cfg.http_metadata_server_host = "0.0.0.0";

    cfg.put_start_discard_timeout_sec = 30;
    cfg.put_start_release_timeout_sec = 600;

    cfg.enable_disk_eviction = true;
    cfg.quota_bytes = 0;

    cfg.enable_snapshot_restore = false;
    cfg.enable_snapshot_restore_clean_metadata = true;
    cfg.enable_snapshot = true;
    cfg.snapshot_backup_dir = "snapshots";
    cfg.snapshot_interval_seconds = 60;
    cfg.snapshot_child_timeout_seconds = 60;

    cfg.snapshot_backend_type = "local";

    cfg.max_total_finished_tasks = 10000;
    cfg.max_total_pending_tasks = 10000;
    cfg.max_total_processing_tasks = 10000;
    cfg.pending_task_timeout_sec = 300;
    cfg.processing_task_timeout_sec = 300;

    return cfg;
}

TEST(HAConfigPolicyTest, SupervisorConfigForcesEtcdSnapshotBackend) {
    auto cfg = MakeMinimalMasterConfig();

    cfg.snapshot_backend_type = "local";
    mooncake::MasterServiceSupervisorConfig sup(cfg);
    EXPECT_EQ(sup.snapshot_backend_type, mooncake::SnapshotBackendType::ETCD);

    cfg.snapshot_backend_type = "s3";
    mooncake::MasterServiceSupervisorConfig sup2(cfg);
    EXPECT_EQ(sup2.snapshot_backend_type, mooncake::SnapshotBackendType::ETCD);

    cfg.snapshot_backend_type = "etcd";
    mooncake::MasterServiceSupervisorConfig sup3(cfg);
    EXPECT_EQ(sup3.snapshot_backend_type, mooncake::SnapshotBackendType::ETCD);
}

TEST(HAConfigPolicyTest, WrappedConfigForcesEtcdSnapshotBackend) {
    auto cfg = MakeMinimalMasterConfig();
    cfg.snapshot_backend_type = "local";

    mooncake::MasterServiceSupervisorConfig sup(cfg);
    mooncake::WrappedMasterServiceConfig wrapped(sup, /*view_version=*/0);

    EXPECT_TRUE(wrapped.enable_ha);
    EXPECT_EQ(wrapped.snapshot_backend_type,
              mooncake::SnapshotBackendType::ETCD);
}

#else

TEST(HAConfigPolicyTest, SkippedWhenEtcdDisabled) {
    GTEST_SKIP() << "STORE_USE_ETCD is not enabled";
}

#endif

}  // namespace mooncake::test
