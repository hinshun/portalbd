{ inputs, ... }:
{
  flake.nixosModules = {
    portalbd = import ../nixos/portalbd.nix;
    default = inputs.self.nixosModules.portalbd;
  };
}
