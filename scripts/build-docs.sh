#!/bin/sh
set -e

dir=$(mktemp -d dist-docs.XXXXXX)
trap 'rm -r "$dir"' EXIT

cabal v2-haddock --builddir="$dir" --haddock-for-hackage --haddock-option=--hyperlinked-source
# Starting with cabal 2.0, `--publish` is needed for uploading to non-candidate releases
cabal upload -d $dir/*-docs.tar.gz --publish
