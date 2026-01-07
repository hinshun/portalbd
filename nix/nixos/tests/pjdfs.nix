{ pkgs, ... }:
let
  pjdfstest = pkgs.pjdfstest;
in
{
  name = "portalbd-pjdfstest";

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
      pjdfstest
      perl
      openssl
    ];
  };

  testScript = ''
    machine.wait_for_unit("portalbd-test.service")
    machine.wait_for_open_port(10809)

    # Connect NBD client
    machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N portalbd")

    # Create ext4 filesystem
    machine.succeed("mkfs.ext4 -F /dev/nbd0")

    # Mount the filesystem
    machine.succeed("mkdir -p /mnt/test")
    machine.succeed("mount /dev/nbd0 /mnt/test")

    # Run pjdfstest from within the mounted filesystem
    # pjdfstest is in PATH from environment.systemPackages
    machine.succeed("cd /mnt/test && prove -rv ${pjdfstest}/share/pjdfstest/tests")

    # Cleanup
    machine.succeed("umount /mnt/test")
    machine.succeed("nbd-client -d /dev/nbd0")
  '';
}
