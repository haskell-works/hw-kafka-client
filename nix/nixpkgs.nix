{ compiler ? "ghc8107" }:

with rec {
  sources = import ./sources.nix;
  nivOverlay = _: pkgs: {
    niv = (import sources.niv { }).niv; # use the sources :)
  };
};

import sources.nixpkgs {
  config = {
    packageOverrides = super:
      let self = super.pkgs;
      in {
        haskellPackages = super.haskell.packages.${compiler}.override {
          overrides = import ./overrides.nix { pkgs = self; };
        };
      };
  };
  # Following lines are useful for:
  #   Known issues:
  #  - Support for OpenSSL 1.0.2 ended with 2019.

  # You can install it anyway by allowing this package, using the
  # following methods:

  # a) To temporarily allow all insecure packages, you can use an environment
  #    variable for a single invocation of the nix tools:

  #      $ export NIXPKGS_ALLOW_INSECURE=1

  # b) for `nixos-rebuild` you can add ‘openssl-1.0.2u’ to
  #    `nixpkgs.config.permittedInsecurePackages` in the configuration.nix,
  #    like so:

  #      {
  #        nixpkgs.config.permittedInsecurePackages = [
  #          "openssl-1.0.2u"
  #        ];
  #      }

  # c) For `nix-env`, `nix-build`, `nix-shell` or any other Nix command you can add
  #    ‘openssl-1.0.2u’ to `permittedInsecurePackages` in
  #    ~/.config/nixpkgs/config.nix, like so:

  #      {
  #        permittedInsecurePackages = [
  #          "openssl-1.0.2u"
  #        ];
  #      }
  config.permittedInsecurePackages = [ "openssl-1.0.2u" ];
  overlays = [ nivOverlay ];
}
