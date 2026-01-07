{ inputs, ... }:
{
  flake.overlays = {
    portalbd = final: prev: {
      portalbd = final.callPackage "${inputs.self}/nix/package.nix" {
        craneLib = inputs.crane.mkLib final;
      };
      pjdfstest = final.callPackage "${inputs.self}/nix/pjdfstest.nix" { };
    };

    default = inputs.self.overlays.portalbd;
  };
}
