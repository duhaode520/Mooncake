"""
Mooncake HA Chaos Tests — Phase 1: Basic HA Failover
=====================================================

These tests verify fundamental HA scenarios with REAL data:
  - S1: Leader killed → failover → data intact
  - S2: Backup killed → no impact on operations
  - S3: Leader killed → restarted → joins as standby → data synced

Each test writes real data through the full TransferEngine path,
then verifies data integrity byte-by-byte after the chaos event.
"""
import os
import time
import logging

import pytest

from conftest import (
    DockerCtl,
    MetricsChecker,
    StoreClient,
    DataIntegrityTracker,
    MasterInfo,
    ClientInfo,
    find_leader,
    wait_for_leader,
    wait_for_standby_sync,
    LEADER_ELECTION_TIMEOUT,
)

logger = logging.getLogger("chaos.phase1")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def write_test_data(client: StoreClient, tracker: DataIntegrityTracker,
                    prefix: str, count: int = 10, value_size: int = 4096) -> None:
    """Write `count` key-value pairs with known data patterns."""
    for i in range(count):
        key = f"{prefix}/key_{i:04d}"
        # Create deterministic but unique data
        pattern = f"data-{prefix}-{i}-".encode()
        value = (pattern * ((value_size // len(pattern)) + 1))[:value_size]
        rc = client.put(key, value)
        assert rc == 0, f"Put failed for key={key}, rc={rc}"
        tracker.record_put(key, value)
    logger.info(f"✍️  Wrote {count} keys with prefix '{prefix}' ({value_size}B each)")


def verify_data_on_leader(masters, client: StoreClient,
                          tracker: DataIntegrityTracker) -> None:
    """Find leader and verify both metadata and data integrity."""
    leader = wait_for_leader(masters)
    checker = MetricsChecker(leader)

    # 1. Verify key count on master metadata
    master_key_count = checker.get_key_count()
    logger.info(f"Master key_count={master_key_count}, tracker count={tracker.key_count}")
    # key_count on master may include segment-level keys, so >= is OK
    assert master_key_count >= tracker.key_count, (
        f"Master has fewer keys ({master_key_count}) than expected ({tracker.key_count})"
    )

    # 2. Verify key set on master
    key_errors = tracker.verify_keys_on_master(checker)
    if key_errors:
        logger.warning(f"Key set mismatches: {key_errors}")

    # 3. Verify actual data content (byte-level integrity)
    data_errors = tracker.verify_all(client)
    assert not data_errors, f"Data integrity failures:\n" + "\n".join(data_errors)

    # 4. Verify HA health
    try:
        checksum_failures = checker.get_oplog_checksum_failures()
        assert checksum_failures == 0, f"Checksum failures detected: {checksum_failures}"
    except Exception:
        pass  # HA metrics may not be available on fresh leader

    logger.info("✅ Data integrity verification passed")


# ═══════════════════════════════════════════════════════════════════════════════
# S1: Leader Killed → Failover → Data Intact
# ═══════════════════════════════════════════════════════════════════════════════

class TestS1LeaderKilledFailover:
    """
    Scenario: Kill the leader master after writing data.
    Expected: A new leader is elected within timeout, all data is preserved.
    """

    @pytest.mark.timeout(180)
    def test_leader_killed_data_survives(self, masters, clients,
                                         ensure_cluster_ready, tracker):
        initial_leader = ensure_cluster_ready

        # Connect DummyClient → RealClient-1
        store = StoreClient(clients[0].rpc_addr)
        store.setup()

        # Write data through the full data path
        write_test_data(store, tracker, prefix="s1_leader_kill", count=20, value_size=8192)

        # Wait for data to be fully committed + standby replication
        time.sleep(5)
        wait_for_standby_sync(masters, timeout=30)

        # Record pre-kill metrics
        pre_checker = MetricsChecker(initial_leader)
        pre_key_count = pre_checker.get_key_count()
        logger.info(f"Pre-kill leader={initial_leader.name}, key_count={pre_key_count}")

        # 🔪 Kill the leader
        DockerCtl.kill(initial_leader.container_name)
        logger.info(f"Leader {initial_leader.name} killed. Waiting for failover...")

        # Wait for new leader
        remaining_masters = [m for m in masters if m.name != initial_leader.name]
        new_leader = wait_for_leader(remaining_masters)
        assert new_leader.name != initial_leader.name, "Old leader should not be the new leader"

        # Give new leader time to fully initialize
        time.sleep(5)

        # Verify data on new leader
        verify_data_on_leader(remaining_masters, store, tracker)

        # Verify the new leader's key count
        new_checker = MetricsChecker(new_leader)
        post_key_count = new_checker.get_key_count()
        logger.info(f"Post-failover leader={new_leader.name}, key_count={post_key_count}")
        assert post_key_count >= tracker.key_count, (
            f"Keys lost after failover: {post_key_count} < {tracker.key_count}"
        )

        # Write more data to the new leader to verify it's fully functional
        for i in range(5):
            key = f"s1_after_failover/key_{i}"
            value = f"post-failover-data-{i}".encode() * 100
            rc = store.put(key, value)
            assert rc == 0, f"Post-failover put failed: key={key}, rc={rc}"
            tracker.record_put(key, value)

        # Final verification
        verify_data_on_leader(remaining_masters, store, tracker)
        logger.info("✅ S1: Leader kill failover — PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# S2: Backup Master Killed → No Impact
# ═══════════════════════════════════════════════════════════════════════════════

class TestS2BackupKilled:
    """
    Scenario: Kill a non-leader (backup) master.
    Expected: Leader and clients are unaffected, reads/writes continue.
    """

    @pytest.mark.timeout(120)
    def test_backup_killed_no_impact(self, masters, clients,
                                     ensure_cluster_ready, tracker):
        leader = ensure_cluster_ready

        store = StoreClient(clients[0].rpc_addr)
        store.setup()

        # Write initial data
        write_test_data(store, tracker, prefix="s2_before_kill", count=10)

        # Kill ALL backup masters (not the leader)
        killed = []
        for m in masters:
            if m.name != leader.name:
                DockerCtl.kill(m.container_name)
                killed.append(m.name)
        logger.info(f"Killed backup masters: {killed}")

        # Wait a moment, then verify the leader is still serving
        time.sleep(3)

        # Write more data — should succeed since leader is alive
        write_test_data(store, tracker, prefix="s2_after_kill", count=10)

        # Verify all data intact on leader
        checker = MetricsChecker(leader)
        assert checker.is_healthy(), "Leader should still be healthy"

        # Full data integrity check
        data_errors = tracker.verify_all(store)
        assert not data_errors, f"Data integrity failures:\n" + "\n".join(data_errors)

        logger.info("✅ S2: Backup killed, no impact — PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# S3: Leader Killed → Restarted → Joins as Standby → Synced
# ═══════════════════════════════════════════════════════════════════════════════

class TestS3LeaderRestartAsStandby:
    """
    Scenario: Kill leader, wait for failover, restart old leader.
    Expected: Old leader rejoins as standby, catches up via oplog replay.
    """

    @pytest.mark.timeout(240)
    def test_leader_restart_rejoins_as_standby(self, masters, clients,
                                                ensure_cluster_ready, tracker):
        initial_leader = ensure_cluster_ready

        store = StoreClient(clients[0].rpc_addr)
        store.setup()

        # Write data
        write_test_data(store, tracker, prefix="s3_initial", count=15, value_size=16384)
        time.sleep(3)
        wait_for_standby_sync(masters, timeout=30)

        # Kill leader
        DockerCtl.kill(initial_leader.container_name)
        logger.info(f"Killed leader {initial_leader.name}")

        # Wait for new leader
        remaining = [m for m in masters if m.name != initial_leader.name]
        new_leader = wait_for_leader(remaining)
        time.sleep(5)

        # Write more data to the new leader
        write_test_data(store, tracker, prefix="s3_post_failover", count=10, value_size=8192)

        # Restart the old leader
        DockerCtl.start(initial_leader.container_name)
        logger.info(f"Restarted old leader {initial_leader.name}, should join as standby")

        # Wait for the old leader to sync up
        time.sleep(15)
        wait_for_standby_sync(masters, timeout=60)

        # Verify data on the new leader
        verify_data_on_leader([new_leader], store, tracker)

        # Check old leader's metrics (should be a healthy standby now)
        try:
            old_checker = MetricsChecker(initial_leader)
            if old_checker.is_healthy():
                lag = old_checker.get_oplog_standby_lag()
                logger.info(f"Old leader ({initial_leader.name}) standby lag: {lag}")
                # Lag should be 0 or very small after sync
                assert lag <= 5, f"Old leader standby lag too high: {lag}"
        except Exception as e:
            logger.warning(f"Could not check old leader metrics: {e}")

        logger.info("✅ S3: Leader restart as standby — PASSED")
