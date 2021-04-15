with import ./nix/nixpkgs.nix { };

pkgs.mkShell {
  buildInputs = with pkgs; [
    openssl
    zlib
    rdkafka
    nettools
  ];

  shellHook = ''
    PATH=~/.cabal/bin:$PATH
    LD_LIBRARY_PATH=${pkgs.zlib}/lib:$LD_LIBRARY_PATH
    export LIBRARY_PATH=${pkgs.rdkafka}/lib
    export C_INCLUDE_PATH=${pkgs.rdkafka}/include

    export KAFKA_TEST_BROKER=$(ifconfig | sed -En 's/127.0.0.1//;s/.*inet (addr:)?(([0-9]*\.){3}[0-9]*).*/\2/p' | head -n 1)
  '';
}
