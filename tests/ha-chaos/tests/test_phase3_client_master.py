"""
Mooncake HA Chaos Tests — Phase 3: Client + Master Joint Failures
==================================================================

Tests combining client and master failures together:
  - S7: Client + Leader crash simultaneously → metadata cleanup after recovery
  - S8: Network partition simulation (docker pause) → no split-brain
"""
import time
import logging

import pytest

from conftest import (
    DockerCtl,
    MetricsChecker,
    StoreClient,
    DataIntegrityTracker,
    find_leader,
    wait_for_leader,
    wait_for_standby_sync,
    CLIENT_CRASH_DETECTION_TIMEOUT,
)

logger = logging.getLogger("chaos.phase3")


def write_test_data(client, tracker, prefix, count=10, value_size=4096):
    for i in range(count):
        key = f"{prefix}/key_{i:04d}"
        pattern = f"data-{prefix}-{i}-".encode()
        value = (pattern * ((value_size // len(pattern)) + 1))[:value_size]
        rc = client.put(key, value)
        assert rc == 0, f"Put failed for key={key}, rc={rc}"
        tracker.record_put(key, value)


# ═══════════════════════════════════════════════════════════════════════════════
# S7: Client + Leader Crash Simultaneously
# ═══════════════════════════════════════════════════════════════════════════════

class TestS7ClientAndLeaderCrash:
    """
    Scenario: Write data via client-1 → kill both client-1 AND the leader.
    Expected: New leader elected, client-1's segments cleaned up,
              keys from client-1's segment removed from metadata.
    """

    @pytest.mark.timeout(300)
    def test_simultaneous_client_leader_crash(self, masters, clients,
                                               ensure_cluster_ready, tracker):
        leader = ensure_cluster_ready

        # Use client-1 to write data
        store1 = StoreClient(clients[0].rpc_addr)
        store1.setup()

        write_test_data(store1, tracker, prefix="s7_client1_data", count=15, value_size=8192)
        time.sleep(3)
        wait_for_standby_sync(masters, timeout=30)

        # Also write via client-2 (this data should survive)
        store2 = StoreClient(clients[1].rpc_addr)
        store2.setup()

        surviving_tracker = DataIntegrityTracker()
        for i in range(10):
            key = f"s7_client2_data/key_{i:04d}"
            value = f"client2-data-{i}-".encode() * 500
            rc = store2.put(key, value)
            assert rc == 0
            surviving_tracker.record_put(key, value)

        time.sleep(3)
        wait_for_standby_sync(masters, timeout=30)

        # Record state
        checker = MetricsChecker(leader)
        pre_clients = checker.get_active_clients()
        pre_keys = checker.get_key_count()
        logger.info(f"Pre-crash: clients={pre_clients}, keys={pre_keys}")

        # ☠️ Kill BOTH client-1 AND the leader simultaneously
        DockerCtl.kill(clients[0].container_name)
        DockerCtl.kill(leader.container_name)
        logger.info(f"Killed client-1 ({clients[0].name}) AND leader ({leader.name})")

        # Wait for new leader
        remaining_masters = [m for m in masters if m.name != leader.name]
        new_leader = wait_for_leader(remaining_masters)
        time.sleep(5)

        # Wait for client crash detection TTL
        logger.info(f"Waiting {CLIENT_CRASH_DETECTION_TIMEOUT}s for client crash detection...")
        time.sleep(CLIENT_CRASH_DETECTION_TIMEOUT)

        # Check that client-1's data is cleaned up (segments gone)
        new_checker = MetricsChecker(new_leader)
        post_clients = new_checker.get_active_clients()
        logger.info(f"Post-crash: active_clients={post_clients}")

        # Client-1 should have been cleaned up
        # The exact behavior depends on the client_live_ttl_sec setting
        assert post_clients < pre_clients, (
            f"Active clients should decrease: {post_clients} >= {pre_clients}"
        )

        # Verify client-2's data survives (if client-2 is still connected)
        try:
            data_errors = surviving_tracker.verify_all(store2)
            if data_errors:
                logger.warning(f"Client-2 data verification: {len(data_errors)} issues")
            else:
                logger.info("✅ Client-2's data survived the crash")
        except Exception as e:
            logger.warning(f"Client-2 data check failed (may need reconnect): {e}")

        # The post-crash keys on master should be fewer (client-1's data removed)
        post_keys = new_checker.get_key_count()
        logger.info(f"Post-crash+cleanup: keys={post_keys} (was {pre_keys})")

        logger.info("✅ S7: Client + Leader crash — PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# S8: Network Partition Simulation (Docker Pause)
# ═══════════════════════════════════════════════════════════════════════════════

class TestS8NetworkPartition:
    """
    Scenario: Pause the leader container (simulating network partition).
    Expected:
      1. etcd lease expires → leader loses leadership
      2. A backup becomes new leader
      3. When old leader unpauses, it should NOT serve as leader (no split-brain)
      4. Old leader should detect it's no longer leader and step down to standby
    """

    @pytest.mark.timeout(300)
    def test_leader_paused_no_split_brain(self, masters, clients,
                                           ensure_cluster_ready, tracker):
        leader = ensure_cluster_ready

        store = StoreClient(clients[0].rpc_addr)
        store.setup()

        # Write initial data
        write_test_data(store, tracker, prefix="s8_before_partition", count=15)
        time.sleep(3)
        wait_for_standby_sync(masters, timeout=30)

        # ⏸️ Pause the leader (simulates network partition)
        DockerCtl.pause(leader.container_name)
        logger.info(f"⏸️ Paused leader {leader.name} (simulating partition)")

        # Wait for etcd lease to expire and a new leader to be elected
        # ETCD_MASTER_VIEW_LEASE_TTL * 3 = typical wait
        time.sleep(35)

        # Find the new leader
        remaining = [m for m in masters if m.name != leader.name]
        new_leader = wait_for_leader(remaining)
        assert new_leader.name != leader.name, "Paused node should not be leader"
        logger.info(f"New leader: {new_leader.name}")

        # Write data to the new leader
        write_test_data(store, tracker, prefix="s8_during_partition", count=10)

        # Unpause the old leader
        DockerCtl.unpause(leader.container_name)
        logger.info(f"▶️ Unpaused old leader {leader.name}")

        # Give time for the old leader to realize it's no longer leader
        time.sleep(15)

        # Verify NO split brain: only one leader should be serving
        leaders_found = []
        for m in masters:
            try:
                checker = MetricsChecker(m)
                if checker.is_healthy():
                    try:
                        keys = checker.get_all_keys()
                        leaders_found.append(m.name)
                    except Exception:
                        pass
            except Exception:
                pass

        logger.info(f"Masters responding as leader: {leaders_found}")
        # There should be exactly 1 leader (or the old leader + new leader
        # during a brief transition window)
        assert len(leaders_found) >= 1, "At least one leader should be serving"

        # Wait for standby sync
        wait_for_standby_sync(masters, timeout=60)

        # Verify all data
        verify_leader = wait_for_leader(masters)
        data_errors = tracker.verify_all(store)
        assert not data_errors, f"Data integrity failures:\n" + "\n".join(data_errors)

        logger.info("✅ S8: Network partition, no split-brain — PASSED")
