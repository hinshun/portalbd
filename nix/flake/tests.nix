{ inputs, lib, ... }:
{
  perSystem =
    { pkgs, system, ... }:
    let
      # Allow unfree packages (e.g., minecraft-server) in tests
      testPkgs = import inputs.nixpkgs {
        inherit system;
        config.allowUnfree = true;
        overlays = [ inputs.self.overlays.default ];
      };

      mkTest =
        name:
        lib.nixos.evalTest {
          imports = [ (../nixos/tests + "/${name}.nix") ];
          hostPkgs = testPkgs;
          defaults = {
            imports = [ inputs.self.nixosModules.default ];
            nixpkgs.hostPlatform = system;
            nixpkgs.pkgs = testPkgs;
            services.portalbd.instances.test.package = lib.mkDefault inputs.self.packages.${system}.default;
          };
        };

      mkCsiTest =
        name:
        lib.nixos.evalTest {
          imports = [ (../nixos/tests + "/${name}.nix") ];
          hostPkgs = testPkgs;
          defaults = {
            nixpkgs.overlays = [ inputs.self.overlays.default ];
          };
        };

      tests = {
        basic = mkTest "basic";
        crash-recovery = mkTest "crash-recovery";
        snapshot-semantics = mkTest "snapshot-semantics";
        resource-exhaustion = mkTest "resource-exhaustion";
        pjdfs = mkTest "pjdfs";
        trace-mkfs = mkTest "trace-mkfs";
        trace-pgbench = mkTest "trace-pgbench";
        trace-minecraft = mkTest "trace-minecraft";
      };

      csiTests = {
        csi-node = mkCsiTest "csi-node";
        csi-k3s = mkCsiTest "csi-k3s";
        network-test = mkCsiTest "network-test";
      };

      allTests = tests // csiTests;

      # Trace tests output to target/traces/ via -o flag
      traceTests = [ "trace-mkfs" "trace-pgbench" "trace-minecraft" ];

      mkApp = name: test:
        let
          isTraceTest = builtins.elem name traceTests;
          script = pkgs.writeShellScript "test-${name}" ''
            ${lib.optionalString isTraceTest ''
              mkdir -p target/traces
            ''}
            exec ${test.config.result.driver}/bin/nixos-test-driver \
              ${lib.optionalString isTraceTest "-o target/traces"} \
              "$@"
          '';
        in {
          type = "app";
          program = "${script}";
        };
    in
    {
      apps = lib.mapAttrs' (
        name: test:
        lib.nameValuePair "test-${name}" (mkApp name test)
      ) allTests;
    };
}
