# NixOS test for recording mkfs.ext4 trace.
{
  name = "portalbd-trace-mkfs";

  nodes.machine = { pkgs, config, ... }: {
    boot.kernelModules = [ "nbd" ];

    services.portalbd.instances.test = {
      enable = true;
      package = pkgs.portalbd;
      settings = {
        device.disk_size_gb = 1;
        nbd.address = "127.0.0.1:10809";
        socket = "/run/portalbd-test/portalbd.sock";
        record = {
          path = "/var/lib/portalbd-test/mkfs.trace.json";
          workload = "mkfs.ext4";
        };
      };
    };

    environment.systemPackages = with pkgs; [ nbd e2fsprogs ];
  };

  testScript = ''
    machine.wait_for_unit("portalbd-test.service")
    machine.wait_for_open_port(10809)

    # Connect NBD client and run mkfs.ext4
    machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N portalbd")
    machine.succeed("mkfs.ext4 -F /dev/nbd0")
    machine.succeed("sync")

    # Disconnect NBD client
    machine.succeed("nbd-client -d /dev/nbd0")
    machine.wait_until_succeeds("test $(cat /sys/block/nbd0/size) = 0")

    # Stop portalbd to trigger trace save
    machine.succeed("systemctl stop portalbd-test.service")

    # Wait for trace file and copy it
    machine.wait_until_succeeds("test -f /var/lib/portalbd-test/mkfs.trace.json", timeout=30)
    machine.copy_from_vm("/var/lib/portalbd-test/mkfs.trace.json")
  '';
}
