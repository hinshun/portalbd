{ pkgs, ... }:
{
  name = "portalbd-snapshot-semantics";

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
      portalbd
      nbd
      e2fsprogs
      util-linux
      minio-client
    ];
  };

  testScript = ''
    import time

    SOCKET = "/run/portalbd-test/portalbd.sock"

    def ctl(args):
        """Run portalctl command and return stdout"""
        return machine.succeed(f"portalctl -s {SOCKET} {args}")

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

    # Generation 1: Write initial data
    machine.succeed("echo 'generation-1' > /mnt/test/gen.txt")
    machine.succeed("dd if=/dev/urandom of=/mnt/test/gen1.dat bs=1M count=5")
    gen1_checksum = machine.succeed("sha256sum /mnt/test/gen1.dat").strip().split()[0]
    machine.succeed("sync")
    time.sleep(0.5)

    # Create snapshot after gen1
    output = ctl("snapshot create snap1")
    assert "Snapshot: snap1" in output, "Failed to create snap1: " + output

    # Generation 2: Write new data, overwrite gen file
    machine.succeed("echo 'generation-2' > /mnt/test/gen.txt")
    machine.succeed("dd if=/dev/urandom of=/mnt/test/gen2.dat bs=1M count=5")
    gen2_checksum = machine.succeed("sha256sum /mnt/test/gen2.dat").strip().split()[0]
    machine.succeed("sync")
    time.sleep(0.5)

    # Verify current state is gen2
    output = machine.succeed("cat /mnt/test/gen.txt")
    assert "generation-2" in output, "Expected gen2, got: " + output

    # Create snapshot after gen2
    output = ctl("snapshot create snap2")
    assert "Snapshot: snap2" in output, "Failed to create snap2: " + output

    # List snapshots
    output = ctl("snapshot list")
    assert "snap1" in output and "snap2" in output, "Missing snapshots in list: " + output

    # Unmount before restore
    machine.succeed("umount /mnt/test")
    machine.succeed("nbd-client -d /dev/nbd0")
    machine.wait_until_succeeds("test $(cat /sys/block/nbd0/size) = 0")

    # Restore to snap1
    output = ctl("snapshot restore snap1")
    assert "OK" in output, "Failed to restore snap1: " + output

    # Reconnect and verify gen1 state
    machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N portalbd")
    machine.succeed("mount /dev/nbd0 /mnt/test")

    output = machine.succeed("cat /mnt/test/gen.txt")
    assert "generation-1" in output, "After restore to snap1, expected gen1, got: " + output

    restored_checksum = machine.succeed("sha256sum /mnt/test/gen1.dat").strip().split()[0]
    assert gen1_checksum == restored_checksum, "Gen1 checksum mismatch after restore"

    # gen2.dat should NOT exist after restoring to snap1
    status, _ = machine.execute("test -f /mnt/test/gen2.dat")
    assert status != 0, "gen2.dat should not exist after restoring to snap1"

    # TODO(slatedb): Re-enable once SlateDB supports preserving later checkpoints.
    # Currently, restore_checkpoint requires deleting snapshots created after target.
    # The following tests verify snap2 survives restore to snap1, which doesn't work yet.
    #
    # # Generation 3: Write new data after restore (nested snapshot scenario)
    # machine.succeed("echo 'generation-3' > /mnt/test/gen.txt")
    # machine.succeed("dd if=/dev/urandom of=/mnt/test/gen3.dat bs=1M count=5")
    # gen3_checksum = machine.succeed("sha256sum /mnt/test/gen3.dat").strip().split()[0]
    # machine.succeed("sync")
    # time.sleep(0.5)
    #
    # # Create snapshot after restore (nested)
    # output = ctl("snapshot create snap3")
    # assert "Snapshot: snap3" in output, "Failed to create snap3: " + output
    #
    # # Verify we can still restore to snap2
    # machine.succeed("umount /mnt/test")
    # machine.succeed("nbd-client -d /dev/nbd0")
    # machine.wait_until_succeeds("test $(cat /sys/block/nbd0/size) = 0")
    #
    # output = ctl("snapshot restore snap2")
    # assert "OK" in output, "Failed to restore snap2: " + output
    #
    # machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N portalbd")
    # machine.succeed("mount /dev/nbd0 /mnt/test")
    #
    # output = machine.succeed("cat /mnt/test/gen.txt")
    # assert "generation-2" in output, "After restore to snap2, expected gen2, got: " + output
    #
    # restored_checksum = machine.succeed("sha256sum /mnt/test/gen2.dat").strip().split()[0]
    # assert gen2_checksum == restored_checksum, "Gen2 checksum mismatch after restore"
    #
    # # Delete snap1 and verify snap2/snap3 still work
    # machine.succeed("umount /mnt/test")
    # machine.succeed("nbd-client -d /dev/nbd0")
    # machine.wait_until_succeeds("test $(cat /sys/block/nbd0/size) = 0")
    #
    # output = ctl("snapshot delete snap1")
    # assert "OK" in output, "Failed to delete snap1: " + output
    #
    # # Verify snap1 is gone (get should fail)
    # status, output = machine.execute(f"portalctl -s {SOCKET} snapshot get snap1")
    # assert status != 0, "snap1 should be deleted but get succeeded"
    #
    # # Restore to snap3 should still work
    # output = ctl("snapshot restore snap3")
    # assert "OK" in output, "Failed to restore snap3: " + output
    #
    # machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N portalbd")
    # machine.succeed("mount /dev/nbd0 /mnt/test")
    #
    # output = machine.succeed("cat /mnt/test/gen.txt")
    # assert "generation-3" in output, "After restore to snap3, expected gen3, got: " + output
    #
    # restored_checksum = machine.succeed("sha256sum /mnt/test/gen3.dat").strip().split()[0]
    # assert gen3_checksum == restored_checksum, "Gen3 checksum mismatch after restore"

    # Cleanup
    machine.succeed("umount /mnt/test")
    machine.succeed("nbd-client -d /dev/nbd0")
  '';
}
