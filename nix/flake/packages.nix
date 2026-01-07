{ inputs, ... }:
{
  perSystem = { pkgs, ... }:
    let
      craneLib = inputs.crane.mkLib pkgs;
      portalbd = pkgs.callPackage "${inputs.self}/nix/package.nix" {
        inherit craneLib;
      };
    in {
      packages = {
        inherit portalbd;
        default = portalbd;
      };

      devShells.default = craneLib.devShell {
        packages = with pkgs; [
          pkg-config
          protobuf
          openssl
          rustfmt
          clippy
        ];
      };
    };
}
