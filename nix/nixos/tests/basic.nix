{ pkgs, ... }:
{
  name = "portalbd-basic";

  nodes.machine = { pkgs, ... }: {
    boot.kernelModules = [ "nbd" ];

    services.portalbd.instances.test = {
      enable = true;
      settings = {
        device.disk_size_gb = 1;
        nbd.address = "127.0.0.1:10809";
        socket = "/run/portalbd-test/portalbd.sock";
      };
    };

    environment.systemPackages = with pkgs; [
      nbd
      e2fsprogs
      util-linux
    ];
  };

  testScript = ''
    machine.wait_for_unit("portalbd-test.service")
    machine.wait_for_open_port(10809)

    # Connect NBD client
    machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N portalbd")
    machine.succeed("blockdev --getsize64 /dev/nbd0")

    # Verify size matches configured 1GB
    size = machine.succeed("blockdev --getsize64 /dev/nbd0").strip()
    expected_size = str(1 * 1024 * 1024 * 1024)
    assert size == expected_size, f"Size mismatch: got {size}, expected {expected_size}"

    # Create filesystem
    machine.succeed("mkfs.ext4 -F /dev/nbd0")

    # Mount and write test data
    machine.succeed("mkdir -p /mnt/test")
    machine.succeed("mount /dev/nbd0 /mnt/test")
    machine.succeed("echo 'Hello from portalbd!' > /mnt/test/hello.txt")
    machine.succeed("dd if=/dev/urandom of=/mnt/test/random.dat bs=1M count=10")
    original_checksum = machine.succeed("sha256sum /mnt/test/random.dat").strip()

    # Unmount and disconnect
    machine.succeed("sync")
    machine.succeed("umount /mnt/test")
    machine.succeed("nbd-client -d /dev/nbd0")

    # Wait for full disconnect
    machine.wait_until_succeeds("test $(cat /sys/block/nbd0/size) = 0")

    # Reconnect and verify data
    machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N portalbd")
    machine.succeed("mount /dev/nbd0 /mnt/test")

    output = machine.succeed("cat /mnt/test/hello.txt")
    assert "Hello from portalbd!" in output, f"Content mismatch: {output}"

    restored_checksum = machine.succeed("sha256sum /mnt/test/random.dat").strip()
    assert original_checksum == restored_checksum, f"Checksum mismatch: {original_checksum} vs {restored_checksum}"

    # Run filesystem check
    machine.succeed("umount /mnt/test")
    machine.succeed("e2fsck -f -n /dev/nbd0")

    # Cleanup
    machine.succeed("nbd-client -d /dev/nbd0")
  '';
}
