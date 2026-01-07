{ config, lib, pkgs, ... }:

let
  cfg = config.services.portalbd;
  settingsFormat = pkgs.formats.toml { };

  instanceModule = { name, config, ... }: {
    options = {
      enable = lib.mkEnableOption "this portalbd instance";

      package = lib.mkPackageOption pkgs "portalbd" { };

      environmentFile = lib.mkOption {
        type = lib.types.nullOr lib.types.path;
        default = null;
        description = ''
          Environment file to load before starting portalbd. Useful for secrets
          like cloud credentials that can be referenced via environment variables.
        '';
      };

      settings = lib.mkOption {
        type = lib.types.submodule {
          freeformType = settingsFormat.type;

          options = {
            device = {
              disk_size_gb = lib.mkOption {
                type = lib.types.ints.positive;
                description = "Virtual disk size in GB.";
              };
            };

            storage = {
              url = lib.mkOption {
                type = lib.types.nullOr lib.types.str;
                default = null;
                example = "s3://my-bucket/portalbd-data";
                description = ''
                  Storage backend URL. Supports:
                  - S3: `s3://bucket/path`
                  - Azure: `azure://container/path`
                  - GCS: `gs://bucket/path`
                  - Local: `file:///path/to/storage`
                  - Memory (default if null): in-memory storage
                '';
              };
            };

            nbd = {
              address = lib.mkOption {
                type = lib.types.str;
                default = "127.0.0.1:10809";
                description = "NBD server bind address.";
              };
            };

            socket = lib.mkOption {
              type = lib.types.path;
              default = "/run/portalbd-${name}/portalbd.sock";
              description = "Path to the control socket.";
            };

            lsm = lib.mkOption {
              type = lib.types.nullOr (lib.types.submodule {
                options = {
                  flush_interval_ms = lib.mkOption {
                    type = lib.types.nullOr lib.types.ints.positive;
                    default = null;
                    description = "Interval between flushes in milliseconds.";
                  };

                  l0_sst_size_bytes = lib.mkOption {
                    type = lib.types.nullOr lib.types.ints.positive;
                    default = null;
                    description = "Target size for L0 SST files in bytes.";
                  };

                  l0_max_ssts = lib.mkOption {
                    type = lib.types.nullOr lib.types.ints.positive;
                    default = null;
                    description = "Max SST files in L0 before compaction.";
                  };

                  max_unflushed_bytes = lib.mkOption {
                    type = lib.types.nullOr lib.types.ints.positive;
                    default = null;
                    description = "Max unflushed data before forcing flush.";
                  };

                  filter_bits_per_key = lib.mkOption {
                    type = lib.types.nullOr lib.types.ints.positive;
                    default = null;
                    description = "Bloom filter bits per key.";
                  };

                  block_cache_size_bytes = lib.mkOption {
                    type = lib.types.nullOr lib.types.ints.positive;
                    default = null;
                    description = "Block cache size in bytes.";
                  };
                };
              });
              default = null;
              description = "LSM tree tuning options.";
            };

            record = lib.mkOption {
              type = lib.types.nullOr (lib.types.submodule {
                options = {
                  path = lib.mkOption {
                    type = lib.types.path;
                    description = "Path to write the trace file.";
                  };

                  workload = lib.mkOption {
                    type = lib.types.str;
                    default = "unnamed";
                    description = "Workload name for the trace (e.g., 'mkfs.ext4', 'pgbench').";
                  };
                };
              });
              default = null;
              description = "Operation recording configuration. When set, NBD operations are recorded to a trace file.";
            };
          };
        };
        default = { };
        description = "Configuration for portalbd in TOML format.";
      };
    };
  };

  # Filter out null values from the settings
  filterNulls = attrs:
    lib.filterAttrsRecursive (_: v: v != null) (
      lib.mapAttrs (
        _: v:
        if lib.isAttrs v then filterNulls v else v
      ) attrs
    );

  mkConfigFile = name: instanceCfg:
    settingsFormat.generate "portalbd-${name}.toml" (filterNulls instanceCfg.settings);

  mkService = name: instanceCfg: lib.nameValuePair "portalbd-${name}" {
    description = "portalbd NBD Server (${name})";
    after = [ "network-online.target" ];
    wants = [ "network-online.target" ];
    wantedBy = [ "multi-user.target" ];

    serviceConfig = {
      Type = "simple";
      ExecStart = "${instanceCfg.package}/bin/portalbd --config ${mkConfigFile name instanceCfg}";
      Restart = "always";
      RestartSec = 5;
      RuntimeDirectory = "portalbd-${name}";
      StateDirectory = "portalbd-${name}";
    } // lib.optionalAttrs (instanceCfg.environmentFile != null) {
      EnvironmentFile = instanceCfg.environmentFile;
    };
  };

in {
  options.services.portalbd = {
    instances = lib.mkOption {
      type = lib.types.attrsOf (lib.types.submodule instanceModule);
      default = { };
      description = "Attribute set of portalbd instances.";
      example = lib.literalExpression ''
        {
          main = {
            enable = true;
            settings = {
              device.disk_size_gb = 100;
              storage.url = "s3://my-bucket/main";
              nbd.address = "127.0.0.1:10809";
            };
          };
          backup = {
            enable = true;
            settings = {
              device.disk_size_gb = 50;
              storage.url = "s3://my-bucket/backup";
              nbd.address = "127.0.0.1:10810";
            };
          };
        }
      '';
    };
  };

  config = {
    systemd.services = lib.mapAttrs' mkService
      (lib.filterAttrs (_: instanceCfg: instanceCfg.enable) cfg.instances);
  };
}
