{ nixpkgs ? import <nixpkgs> {} }:

let
  inherit (nixpkgs) pkgs;
  inherit (pkgs) haskell;

  haskellPackages = pkgs.haskell.packages.ghc843;

  drv = haskellPackages.callCabal2nix "hw-kafka-client" ./. {};

in
  drv