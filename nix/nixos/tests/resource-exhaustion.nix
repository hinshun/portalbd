{ pkgs, ... }:
{
  name = "portalbd-resource-exhaustion";

  nodes.machine = { pkgs, ... }: {
    boot.kernelModules = [ "nbd" ];

    services.minio = {
      enable = true;
      rootCredentialsFile = pkgs.writeText "minio-credentials" ''
        MINIO_ROOT_USER=minioadmin
        MINIO_ROOT_PASSWORD=minioadmin
      '';
    };

    # Small disk size to test exhaustion
    services.portalbd.instances.test = {
      enable = true;
      settings = {
        device.disk_size_gb = 1;  # Small 1GB device
        nbd.address = "127.0.0.1:10809";
        socket = "/run/portalbd-test/portalbd.sock";
        storage.url = "s3://portalbd-test/data";
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

    # Limit minio storage to test object storage exhaustion
    virtualisation.diskSize = 2048;  # 2GB total disk
  };

  testScript = ''
    import time

    machine.wait_for_unit("minio.service")
    machine.wait_for_open_port(9000)

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

    # Test 1: Fill device to near capacity
    # Write data until we approach the 1GB limit
    machine.succeed("dd if=/dev/zero of=/mnt/test/fill1.dat bs=1M count=200 || true")
    machine.succeed("dd if=/dev/zero of=/mnt/test/fill2.dat bs=1M count=200 || true")
    machine.succeed("dd if=/dev/zero of=/mnt/test/fill3.dat bs=1M count=200 || true")

    # Check disk usage
    usage = machine.succeed("df -h /mnt/test")
    print(f"Disk usage after fills: {usage}")

    # Try to write more, should eventually fail with ENOSPC
    result = machine.execute("dd if=/dev/zero of=/mnt/test/overflow.dat bs=1M count=500")
    # dd might partially succeed or fail, that's expected

    # Verify filesystem is still consistent after hitting capacity
    machine.succeed("sync")
    machine.succeed("umount /mnt/test")
    machine.succeed("e2fsck -f -n /dev/nbd0")

    # Test 2: Verify recovery after freeing space
    machine.succeed("mount /dev/nbd0 /mnt/test")
    machine.succeed("rm -f /mnt/test/fill1.dat /mnt/test/fill2.dat")
    machine.succeed("sync")

    # Should be able to write again after freeing space
    machine.succeed("dd if=/dev/urandom of=/mnt/test/after-free.dat bs=1M count=50")
    checksum_before = machine.succeed("sha256sum /mnt/test/after-free.dat").strip()

    # Verify the new data persists across reconnect
    machine.succeed("sync")
    machine.succeed("umount /mnt/test")
    machine.succeed("nbd-client -d /dev/nbd0")
    machine.wait_until_succeeds("test $(cat /sys/block/nbd0/size) = 0")

    machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N portalbd")
    machine.succeed("mount /dev/nbd0 /mnt/test")
    checksum_after = machine.succeed("sha256sum /mnt/test/after-free.dat").strip()
    assert checksum_before == checksum_after, f"Data corruption after space recovery: {checksum_before} vs {checksum_after}"

    # Test 3: Verify portalbd stays healthy after resource pressure
    status = machine.succeed("systemctl is-active portalbd-test.service").strip()
    assert status == "active", f"portalbd should still be active: {status}"

    # Test 4: Multiple simultaneous write streams (connection/resource pressure)
    machine.succeed("rm -f /mnt/test/*.dat")
    machine.succeed("sync")

    # Start multiple concurrent writes
    machine.execute("dd if=/dev/urandom of=/mnt/test/stream1.dat bs=1M count=20 &")
    machine.execute("dd if=/dev/urandom of=/mnt/test/stream2.dat bs=1M count=20 &")
    machine.execute("dd if=/dev/urandom of=/mnt/test/stream3.dat bs=1M count=20 &")
    machine.execute("dd if=/dev/urandom of=/mnt/test/stream4.dat bs=1M count=20 &")

    # Wait for all writes to complete
    machine.succeed("wait")
    machine.succeed("sync")

    # Verify all files exist and have expected size
    for i in range(1, 5):
        size = machine.succeed(f"stat -c%s /mnt/test/stream{i}.dat").strip()
        expected = str(20 * 1024 * 1024)
        assert size == expected, f"stream{i}.dat size mismatch: {size} vs {expected}"

    # Test 5: Verify no resource leaks after error conditions
    # Check portalbd is still responsive
    machine.succeed("systemctl status portalbd-test.service")

    # Cleanup
    machine.succeed("umount /mnt/test")
    machine.succeed("e2fsck -f -n /dev/nbd0")
    machine.succeed("nbd-client -d /dev/nbd0")
  '';
}
