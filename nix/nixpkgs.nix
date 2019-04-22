{ compiler ? "ghc843" }:

with rec {
  fetchNixpkgs = import ./fetchNixpkgs.nix;

  _nixpkgs = fetchNixpkgs {
    rev = "da0c385a691d38b56b17eb18b852c4cec2050c24";
    sha256 = "0svhqn139cy2nlgv4kqv1bsxza2dcm0yylrhnmanw4p73gv85caf";
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
