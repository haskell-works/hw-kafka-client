{ pkgs }:

self: super:

with { inherit (pkgs.stdenv) lib; };

with pkgs.haskell.lib;

{
  hw-kafka-client = (
    with rec {
      hw-kafka-clientSource = pkgs.lib.cleanSource ../.;
      hw-kafka-clientBasic = self.callCabal2nix "hw-kafka-client" hw-kafka-clientSource {};
    };
    overrideCabal hw-kafka-clientBasic (old: {
      enableLibraryProfiling = false;
      preConfigure = "sed -i -e /extra-lib-dirs/d -e /include-dirs/d -e /librdkafka/d hw-kafka-client.cabal";
      configureFlags = ''
        --extra-include-dirs=${pkgs.rdkafka}/include/librdkafka
        
        --extra-prog-path=${pkgs.rdkafka}/lib

        --extra-lib-dirs=${pkgs.rdkafka}/lib
      '';
    })
  );
}
