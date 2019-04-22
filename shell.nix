{ compiler ? "ghc843" }:

with rec {
  pkgs = (import ./nix/nixpkgs.nix {
    inherit compiler;
  });
  hp = pkgs.haskellPackages;
  drv = hp.hw-kafka-client;
};

hp.shellFor {
  packages = p: [drv];
  buildInputs = [hp.cabal-install hp.hdevtools];
}
