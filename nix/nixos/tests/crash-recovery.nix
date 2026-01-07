{ pkgs, ... }:
{
  name = "portalbd-crash-recovery";

  nodes.machine = { pkgs, ... }: {
    boot.kernelModules = [ "nbd" ];

    services.minio = {
      enable = true;
      rootCredentialsFile = pkgs.writeText "minio-credentials" ''
        MINIO_ROOT_USER=minioadmin
        MINIO_ROOT_PASSWORD=minioadmin
      '';
    };

    services.portalbd.instances.test = {
      enable = true;
      settings = {
        device.disk_size_gb = 1;
        nbd.address = "127.0.0.1:10809";
        socket = "/run/portalbd-test/portalbd.sock";
        storage.url = "s3://portalbd-test/data";
        # Fast flush for testing crash recovery
        lsm = {
          flush_interval_ms = 100;
          l0_max_ssts = 64;
        };
      };
      environmentFile = pkgs.writeText "portalbd-env" ''
        AWS_ACCESS_KEY_ID=minioadmin
        AWS_SECRET_ACCESS_KEY=minioadmin
        AWS_ENDPOINT=http://127.0.0.1:9000
        AWS_REGION=us-east-1
        AWS_ALLOW_HTTP=true
      '';
    };

    environment.systemPackages = with pkgs; [
      nbd
      e2fsprogs
      util-linux
      minio-client
    ];
  };

  testScript = ''
    import time

    machine.wait_for_unit("minio.service")
    machine.wait_for_open_port(9000)

    # Create bucket
    machine.succeed(
      "${pkgs.minio-client}/bin/mc alias set local http://127.0.0.1:9000 minioadmin minioadmin",
      "${pkgs.minio-client}/bin/mc mb local/portalbd-test",
    )

    machine.wait_for_unit("portalbd-test.service")
    machine.wait_for_open_port(10809)

    # Connect and create filesystem
    machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N portalbd")
    machine.succeed("mkfs.ext4 -F /dev/nbd0")
    machine.succeed("mkdir -p /mnt/test")
    machine.succeed("mount /dev/nbd0 /mnt/test")

    # Write test data with known patterns
    machine.succeed("echo 'pre-crash-data' > /mnt/test/pre-crash.txt")
    machine.succeed("dd if=/dev/urandom of=/mnt/test/pre-crash.dat bs=1M count=5")
    pre_crash_checksum = machine.succeed("sha256sum /mnt/test/pre-crash.dat").strip()

    # Sync to ensure data is persisted
    machine.succeed("sync")
    time.sleep(1)  # Allow flush to complete

    # Unmount before crash
    machine.succeed("umount /mnt/test")
    machine.succeed("nbd-client -d /dev/nbd0")
    machine.wait_until_succeeds("test $(cat /sys/block/nbd0/size) = 0")

    # Simulate crash by killing portalbd with SIGKILL (not graceful)
    machine.succeed("systemctl kill -s KILL portalbd-test.service")
    machine.wait_until_fails("systemctl is-active portalbd-test.service")

    # Restart portalbd
    machine.succeed("systemctl start portalbd-test.service")
    machine.wait_for_unit("portalbd-test.service")
    machine.wait_for_open_port(10809)

    # Reconnect and verify pre-crash data
    machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N portalbd")
    machine.succeed("mount /dev/nbd0 /mnt/test")

    output = machine.succeed("cat /mnt/test/pre-crash.txt")
    assert "pre-crash-data" in output, f"Pre-crash data missing: {output}"

    restored_checksum = machine.succeed("sha256sum /mnt/test/pre-crash.dat").strip()
    assert pre_crash_checksum == restored_checksum, f"Pre-crash checksum mismatch: {pre_crash_checksum} vs {restored_checksum}"

    # Write new data after recovery
    machine.succeed("echo 'post-recovery-data' > /mnt/test/post-recovery.txt")
    machine.succeed("sync")

    # Verify filesystem integrity
    machine.succeed("umount /mnt/test")
    machine.succeed("e2fsck -f -n /dev/nbd0")

    # Crash again during active writes
    machine.succeed("mount /dev/nbd0 /mnt/test")
    machine.execute("dd if=/dev/urandom of=/mnt/test/during-crash.dat bs=1M count=50 &")
    time.sleep(0.5)  # Let some writes happen

    # Force unmount and crash
    machine.succeed("umount -f /mnt/test || true")
    machine.succeed("nbd-client -d /dev/nbd0 || true")
    machine.succeed("systemctl kill -s KILL portalbd-test.service")
    machine.wait_until_fails("systemctl is-active portalbd-test.service")

    # Restart and verify system is still usable
    machine.succeed("systemctl start portalbd-test.service")
    machine.wait_for_unit("portalbd-test.service")
    machine.wait_for_open_port(10809)

    machine.wait_until_succeeds("test $(cat /sys/block/nbd0/size) = 0")
    machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N portalbd")

    # Filesystem should be recoverable (may have journal replay)
    machine.succeed("e2fsck -f -y /dev/nbd0 || true")
    machine.succeed("mount /dev/nbd0 /mnt/test")

    # Pre-recovery data should still be intact
    output = machine.succeed("cat /mnt/test/post-recovery.txt")
    assert "post-recovery-data" in output, f"Post-recovery data missing after second crash: {output}"

    # Cleanup
    machine.succeed("umount /mnt/test")
    machine.succeed("nbd-client -d /dev/nbd0")
  '';
}
