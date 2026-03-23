#include <gtest/gtest.h>

#include "master_config.h"

namespace mooncake::test {

// These tests validated the old HA policy that forced snapshot_backend_type
// to ETCD regardless of user configuration. That policy was removed in
// commit 500bc768 ("refactor(master): disable HA mode snapshot overrides")
// to allow LocalFS and S3 backends in HA mode.

TEST(HAConfigPolicyTest, SkippedHANoLongerForcesEtcd) {
    GTEST_SKIP() << "HA mode no longer forces ETCD snapshot backend "
                    "(removed by design)";
}

}  // namespace mooncake::test
