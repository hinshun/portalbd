# NixOS test for recording pgbench trace.
{ pkgs, ... }:
{
  name = "portalbd-trace-pgbench";

  nodes.machine = { pkgs, config, ... }: {
    boot.kernelModules = [ "nbd" ];
    virtualisation.memorySize = 2048;

    services.portalbd.instances.test = {
      enable = true;
      settings = {
        device.disk_size_gb = 2;
        nbd.address = "127.0.0.1:10809";
        socket = "/run/portalbd-test/portalbd.sock";
        record = {
          path = "/var/lib/portalbd-test/pgbench.trace.json";
          workload = "pgbench";
        };
      };
    };

    environment.systemPackages = with pkgs; [ nbd e2fsprogs postgresql ];

    users.users.postgres = { isSystemUser = true; group = "postgres"; };
    users.groups.postgres = {};
  };

  testScript = ''
    machine.wait_for_unit("portalbd-test.service")
    machine.wait_for_open_port(10809)

    # Set up NBD device with filesystem
    machine.succeed("nbd-client 127.0.0.1 10809 /dev/nbd0 -N portalbd")
    machine.succeed("mkfs.ext4 -F /dev/nbd0")
    machine.succeed("mkdir -p /mnt/nbd && mount /dev/nbd0 /mnt/nbd")
    machine.succeed("mkdir /mnt/nbd/pgdata && chown postgres:postgres /mnt/nbd/pgdata && chmod 700 /mnt/nbd/pgdata")

    # Initialize and configure PostgreSQL
    machine.succeed("sudo -u postgres ${pkgs.postgresql}/bin/initdb -D /mnt/nbd/pgdata")
    machine.succeed("echo \"unix_socket_directories = '/mnt/nbd/pgdata'\" >> /mnt/nbd/pgdata/postgresql.conf")
    machine.succeed("sudo -u postgres ${pkgs.postgresql}/bin/pg_ctl -D /mnt/nbd/pgdata -l /mnt/nbd/pgdata/logfile start")
    machine.wait_until_succeeds("sudo -u postgres ${pkgs.postgresql}/bin/psql -h /mnt/nbd/pgdata -c 'SELECT 1'")

    # Run pgbench
    machine.succeed("sudo -u postgres ${pkgs.postgresql}/bin/createdb -h /mnt/nbd/pgdata pgbench")
    machine.succeed("sudo -u postgres ${pkgs.postgresql}/bin/pgbench -h /mnt/nbd/pgdata -i -s 2 pgbench")
    machine.succeed("sudo -u postgres ${pkgs.postgresql}/bin/pgbench -h /mnt/nbd/pgdata -T 10 -c 2 -j 2 pgbench")

    # Shutdown PostgreSQL and unmount
    machine.succeed("sudo -u postgres ${pkgs.postgresql}/bin/psql -h /mnt/nbd/pgdata -c 'CHECKPOINT'")
    machine.succeed("sudo -u postgres ${pkgs.postgresql}/bin/pg_ctl -D /mnt/nbd/pgdata stop -m fast")
    machine.succeed("umount /mnt/nbd && nbd-client -d /dev/nbd0")

    # Stop portalbd to trigger trace save
    machine.succeed("systemctl stop portalbd-test.service")

    # Wait for trace file and copy it
    machine.wait_until_succeeds("test -f /var/lib/portalbd-test/pgbench.trace.json", timeout=30)
    machine.copy_from_vm("/var/lib/portalbd-test/pgbench.trace.json")
  '';
}
