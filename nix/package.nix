{
  lib,
  craneLib,
  pkg-config,
  openssl,
  protobuf,
}:

let
  src = lib.cleanSourceWith {
    src = ./..;
    filter = path: type:
      # Include proto files in addition to cargo sources
      (lib.hasSuffix ".proto" path) || (craneLib.filterCargoSources path type);
  };

  inherit (craneLib.crateNameFromCargoToml { cargoToml = ../portalbd/Cargo.toml; }) pname version;

  commonArgs = {
    inherit src pname version;
    cargoLock = ../Cargo.lock;
    # Hashes for git dependencies (required when commit is not on default branch)
    outputHashes = {
      "git+https://github.com/jachewz/slatedb.git?branch=restore-checkpoint#ba36824eea9483ce063421cfa7b031d90b12b85f" = "sha256-rn9x8XeyerwowV6v3Bhoh524rGgj3oRN0y77ns1LDyQ=";
    };
    # Build workspace excluding DST crate (requires tokio_unstable, dev-only)
    cargoExtraArgs = "--workspace --exclude portalbd-dst --exclude portalbd-trace";
    strictDeps = true;
    nativeBuildInputs = [ pkg-config protobuf ];
    buildInputs = [ openssl ];
  };

  cargoArtifacts = craneLib.buildDepsOnly commonArgs;
in
craneLib.buildPackage (commonArgs // {
  inherit cargoArtifacts;
})
