#!/bin/bash
set -e

pkg=$(cat *.cabal | grep -e "^name" | tr -s " " | cut -d' ' -f2)
ver=$(cat *.cabal | grep -e "^version" | tr -s " " | cut -d' ' -f2)

if [ -z "$pkg" ]; then
  echo "Unable to determine package name"
  exit 1
fi

if [ -z "$ver" ]; then
  echo "Unable to determine package version"
  exit 1
fi

echo "Detected package: $pkg-$ver"

mkdir -p mkdir /tmp/doc

cp -R $(stack path --local-install-root)/doc/$pkg-$ver/ /tmp/doc/$pkg-$ver
