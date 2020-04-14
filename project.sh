#!/usr/bin/env bash

CABAL_FLAGS="-j8"

cmd="$1"

shift

cabal-install() {
  cabal v2-install \
    -j8 \
    --installdir="$HOME/.local/bin" \
    --overwrite-policy=always \
    --disable-documentation \
    $CABAL_FLAGS "$@"
}

cabal-build() {
  cabal v2-build \
    --enable-tests \
    --write-ghc-environment-files=ghc8.4.4+ \
    $CABAL_FLAGS "$@"
}

cabal-test() {
  cabal v2-test \
    --enable-tests \
    --test-show-details=direct \
    --test-options='+RTS -g1' \
    $CABAL_FLAGS "$@"
}

cabal-exec() {
  cabal v2-exec "$(echo *.cabal | cut -d . -f 1)" "$@"
}

cabal-bench() {
  cabal v2-bench -j8 \
    $CABAL_FLAGS "$@"
}

cabal-repl() {
  cabal v2-repl \
    $CABAL_FLAGS "$@"
}

cabal-clean() {
  cabal v2-clean
}

case "$cmd" in
  install)
    cabal-install
    ;;

  build)
    cabal-build
    ;;

  exec)
    cabal-exec
    ;;

  test)
    cabal-build
    cabal-test
    ;;

  bench)
    cabal-bench
    ;;

  repl)
    cabal-repl
    ;;

  clean)
    cabal-clean
    ;;

  *)
    echo "Unrecognised command: $cmd"
    exit 1
esac
