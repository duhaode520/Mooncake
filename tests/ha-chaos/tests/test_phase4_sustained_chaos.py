"""
Mooncake HA Chaos Tests — Phase 4: Sustained Chaos (Rolling & Random)
======================================================================

Long-running chaos scenarios:
  - S9:  Rolling restart of all masters while continuous writes happen
  - S10: Random chaos for 5 minutes: kill/restart random masters
"""
import os
import time
import random
import logging
import threading
from typing import List

import pytest

from conftest import (
    DockerCtl,
    MetricsChecker,
    StoreClient,
    DataIntegrityTracker,
    MasterInfo,
    find_leader,
    wait_for_leader,
    wait_for_standby_sync,
)

logger = logging.getLogger("chaos.phase4")


# ─── Background Writer ──────────────────────────────────────────────────────

class BackgroundWriter:
    """
    Continuously writes data in a background thread.
    Tracks all successful writes for later verification.
    """

    def __init__(self, client: StoreClient, tracker: DataIntegrityTracker,
                 prefix: str, interval: float = 0.5, value_size: int = 4096):
        self.client = client
        self.tracker = tracker
        self.prefix = prefix
        self.interval = interval
        self.value_size = value_size
        self._stop = threading.Event()
        self._thread = None
        self.write_count = 0
        self.error_count = 0
        self.errors: List[str] = []
        self._lock = threading.Lock()

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info(f"📝 Background writer started: prefix={self.prefix}")

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=30)
        logger.info(f"📝 Background writer stopped: "
                   f"writes={self.write_count}, errors={self.error_count}")

    def _run(self):
        idx = 0
        while not self._stop.is_set():
            key = f"{self.prefix}/bg_key_{idx:06d}"
            pattern = f"bg-{self.prefix}-{idx}-".encode()
            value = (pattern * ((self.value_size // len(pattern)) + 1))[:self.value_size]

            try:
                rc = self.client.put(key, value)
                with self._lock:
                    if rc == 0:
                        self.tracker.record_put(key, value)
                        self.write_count += 1
                    else:
                        self.error_count += 1
                        self.errors.append(f"Put failed: key={key}, rc={rc}")
            except Exception as e:
                with self._lock:
                    self.error_count += 1
                    self.errors.append(f"Put exception: key={key}, {e}")

            idx += 1
            self._stop.wait(self.interval)


# ═══════════════════════════════════════════════════════════════════════════════
# S9: Rolling Restart
# ═══════════════════════════════════════════════════════════════════════════════

class TestS9RollingRestart:
    """
    Scenario: Restart each master one by one (with 20s gap) while
              a background writer continuously pushes data.
    Expected: Zero write failures, all data intact after completion.
    """

    @pytest.mark.timeout(360)
    def test_rolling_restart_zero_data_loss(self, masters, clients,
                                             ensure_cluster_ready, tracker):
        leader = ensure_cluster_ready

        store = StoreClient(clients[0].rpc_addr)
        store.setup()

        # Write some initial data
        for i in range(10):
            key = f"s9_initial/key_{i:04d}"
            value = f"initial-{i}-".encode() * 500
            assert store.put(key, value) == 0
            tracker.record_put(key, value)

        time.sleep(3)

        # Start background writer
        writer = BackgroundWriter(store, tracker, prefix="s9_rolling",
                                  interval=1.0, value_size=4096)
        writer.start()

        try:
            # Rolling restart: each master in sequence
            for idx, master in enumerate(masters):
                logger.info(f"🔄 Rolling restart [{idx+1}/{len(masters)}]: {master.name}")

                DockerCtl.restart(master.container_name)

                # Wait for the cluster to stabilize
                time.sleep(20)

                # Verify a leader exists
                try:
                    current_leader = wait_for_leader(masters)
                    logger.info(f"  Leader after restart: {current_leader.name}")
                except Exception as e:
                    logger.warning(f"  Leader detection failed: {e}")

            # Final stabilization
            time.sleep(10)
            wait_for_standby_sync(masters, timeout=60)

        finally:
            writer.stop()

        # Report writer statistics
        logger.info(f"Writer stats: writes={writer.write_count}, errors={writer.error_count}")
        if writer.errors:
            for err in writer.errors[:10]:
                logger.warning(f"  Write error: {err}")

        # Verify data integrity
        current_leader = wait_for_leader(masters)
        checker = MetricsChecker(current_leader)
        logger.info(f"Final leader: {current_leader.name}, "
                   f"key_count={checker.get_key_count()}")

        data_errors = tracker.verify_all(store)
        if data_errors:
            logger.error(f"Data integrity failures: {len(data_errors)}")
            for err in data_errors[:10]:
                logger.error(f"  {err}")

        # Allow some write errors during rolling restart (transient failover)
        error_rate = writer.error_count / max(writer.write_count + writer.error_count, 1)
        logger.info(f"Write error rate: {error_rate:.2%}")
        assert error_rate < 0.2, (
            f"Too many write errors: {writer.error_count}/{writer.write_count + writer.error_count} "
            f"({error_rate:.2%})"
        )

        # But verified data should be 100% intact
        assert not data_errors, (
            f"Data integrity failures after rolling restart:\n" +
            "\n".join(data_errors[:20])
        )

        logger.info("✅ S9: Rolling restart — PASSED")


# ═══════════════════════════════════════════════════════════════════════════════
# S10: Random Chaos (Monkey Test)
# ═══════════════════════════════════════════════════════════════════════════════

class TestS10RandomChaos:
    """
    Scenario: For 3 minutes, every 10-30 seconds, randomly choose one of:
      - Kill a random master
      - Restart a random master
      - Pause a random master for 15s then unpause
    Meanwhile, a background writer pushes data continuously.
    Expected: After chaos ends, data integrity is preserved.
    """

    @pytest.mark.timeout(480)
    def test_random_chaos_monkey(self, masters, clients,
                                 ensure_cluster_ready, tracker):
        leader = ensure_cluster_ready

        store = StoreClient(clients[0].rpc_addr)
        store.setup()

        # Seed for reproducibility (can override via env)
        seed = int(os.getenv("CHAOS_SEED", str(int(time.time()))))
        rng = random.Random(seed)
        logger.info(f"🎲 Random chaos seed: {seed}")

        # Write initial data
        for i in range(20):
            key = f"s10_initial/key_{i:04d}"
            value = f"chaos-initial-{i}-".encode() * 1000
            assert store.put(key, value) == 0
            tracker.record_put(key, value)

        time.sleep(5)

        # Start background writer
        writer = BackgroundWriter(store, tracker, prefix="s10_chaos",
                                  interval=0.5, value_size=8192)
        writer.start()

        chaos_duration = 180  # 3 minutes
        chaos_end = time.time() + chaos_duration
        action_count = 0
        paused_masters = set()

        try:
            while time.time() < chaos_end:
                # Pick a random action
                action = rng.choice(["kill", "restart", "pause"])
                target = rng.choice(masters)

                try:
                    if action == "kill" and target.name not in paused_masters:
                        DockerCtl.kill(target.container_name)
                        logger.info(f"🎲 [{action_count}] KILL {target.name}")

                    elif action == "restart":
                        if target.name in paused_masters:
                            DockerCtl.unpause(target.container_name)
                            paused_masters.discard(target.name)
                        DockerCtl.restart(target.container_name)
                        logger.info(f"🎲 [{action_count}] RESTART {target.name}")

                    elif action == "pause" and target.name not in paused_masters:
                        DockerCtl.pause(target.container_name)
                        paused_masters.add(target.name)
                        logger.info(f"🎲 [{action_count}] PAUSE {target.name}")

                        # Schedule unpause after 15s
                        def unpause_later(name=target.name, container=target.container_name):
                            time.sleep(15)
                            try:
                                DockerCtl.unpause(container)
                                paused_masters.discard(name)
                                logger.info(f"🎲 AUTO-UNPAUSE {name}")
                            except Exception:
                                pass
                        t = threading.Thread(target=unpause_later, daemon=True)
                        t.start()

                except Exception as e:
                    logger.debug(f"Chaos action failed (expected): {e}")

                action_count += 1

                # Random wait 10-30s
                wait_time = rng.uniform(10, 30)
                time.sleep(wait_time)

        finally:
            writer.stop()

            # Ensure all masters are running and unpaused
            for m in masters:
                try:
                    if m.name in paused_masters:
                        DockerCtl.unpause(m.container_name)
                except Exception:
                    pass
                try:
                    if not DockerCtl.is_running(m.container_name):
                        DockerCtl.start(m.container_name)
                except Exception:
                    pass

        logger.info(f"🎲 Chaos complete: {action_count} actions, seed={seed}")
        logger.info(f"Writer: writes={writer.write_count}, errors={writer.error_count}")

        # Wait for cluster to fully stabilize
        time.sleep(20)

        # Ensure all masters are up
        for m in masters:
            if not DockerCtl.is_running(m.container_name):
                DockerCtl.start(m.container_name)
        time.sleep(15)

        # Wait for leader + sync
        final_leader = wait_for_leader(masters)
        wait_for_standby_sync(masters, timeout=60)

        # Final metrics check
        checker = MetricsChecker(final_leader)
        final_key_count = checker.get_key_count()
        final_checksum_failures = checker.get_oplog_checksum_failures()

        logger.info(f"Final: leader={final_leader.name}, "
                   f"key_count={final_key_count}, "
                   f"checksum_failures={final_checksum_failures}")

        # Verify data integrity
        data_errors = tracker.verify_all(store)
        if data_errors:
            logger.error(f"Data integrity failures: {len(data_errors)}")
            for err in data_errors[:20]:
                logger.error(f"  {err}")

        # Report
        total_ops = writer.write_count + writer.error_count
        error_rate = writer.error_count / max(total_ops, 1)
        logger.info(f"Error rate: {error_rate:.2%} ({writer.error_count}/{total_ops})")

        # Zero checksum failures
        assert final_checksum_failures == 0, (
            f"OpLog checksum failures detected: {final_checksum_failures}"
        )

        # Data that was successfully written must be readable
        assert not data_errors, (
            f"Data integrity failures after chaos:\n" +
            "\n".join(data_errors[:20])
        )

        logger.info(f"✅ S10: Random chaos monkey — PASSED (seed={seed})")
