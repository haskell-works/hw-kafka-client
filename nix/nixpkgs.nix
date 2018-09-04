{ compiler ? "ghc843" }:

with rec {
  fetchFromGitHub = (
    (import <nixpkgs> { config = {}; overlays = []; }).fetchFromGitHub);
  _nixpkgs = fetchFromGitHub {
    owner  = "NixOS";
    repo   = "nixpkgs";
    rev    = "e0d1c6315aa699cf063af6d3661b91c814686b3c";
    sha256 = "0ni4klvxygyxq75mr69xrrbg5166fwmq9rm6vc46x2ww6p0kz652";
  };
};

import _nixpkgs {
  config = {
    packageOverrides = super: let self = super.pkgs; in {
      haskellPackages = super.haskell.packages.${compiler}.override {
        overrides = import ./overrides.nix { pkgs = self; };
      };

    };
  };
  overlays = [];
}
