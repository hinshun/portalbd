# NixOS test for recording minecraft server startup trace.
{
  name = "portalbd-trace-minecraft";

  nodes.machine = { pkgs, config, ... }: {
    boot.kernelModules = [ "nbd" ];
    virtualisation.memorySize = 4096;

    services.portalbd.instances.test = {
      enable = true;
      package = pkgs.portalbd;
      settings = {
        device.disk_size_gb = 4;
        nbd.address = "127.0.0.1:10809";
        socket = "/run/portalbd-test/portalbd.sock";
        record = {
          path = "/var/lib/portalbd-test/minecraft.trace.json";
          workload = "minecraft";
        };
      };
    };

    services.minecraft-server = {
      enable = true;
      eula = true;
      dataDir = "/mnt/nbd/minecraft";
    };

    # Don't start minecraft automatically - we start it after mounting NBD
    systemd.services.minecraft-server.wantedBy = pkgs.lib.mkForce [];

    environment.systemPackages = with pkgs; [ nbd e2fsprogs jq ];
  };

  testScript = ''
    machine.wait_for_unit("portalbd-test.service")
    machine.wait_for_open_port(10809)

    # Set up NBD device
    machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N portalbd")
    machine.succeed("mkfs.ext4 -F /dev/nbd0")
    machine.succeed("mkdir -p /mnt/nbd && mount /dev/nbd0 /mnt/nbd")
    machine.succeed("mkdir -p /mnt/nbd/minecraft && chown minecraft:minecraft /mnt/nbd/minecraft")

    # Start minecraft server and wait for it to be ready
    machine.succeed("systemctl start minecraft-server.service")
    machine.wait_until_succeeds("journalctl -u minecraft-server | grep -q 'Time elapsed'", timeout=120)

    # Shutdown
    machine.succeed("systemctl stop minecraft-server.service")
    machine.succeed("umount /mnt/nbd && nbd-client -d /dev/nbd0")
    machine.succeed("systemctl stop portalbd-test.service")

    # Wait for trace file to be written and copy it
    machine.wait_until_succeeds("test -f /var/lib/portalbd-test/minecraft.trace.json", timeout=30)
    machine.copy_from_vm("/var/lib/portalbd-test/minecraft.trace.json")
  '';
}
