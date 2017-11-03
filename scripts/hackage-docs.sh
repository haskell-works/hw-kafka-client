#!/bin/bash
set -e

# This if stack-enabled fork of https://github.com/ekmett/lens/blob/master/scripts/hackage-docs.sh

if [ "$#" -ne 1 ]; then
  echo "Usage: scripts/hackage-docs.sh HACKAGE_USER"
  exit 1
fi

user=$1

cabal_file=$(find . -maxdepth 1 -name "*.cabal" -print -quit)
if [ ! -f "$cabal_file" ]; then
  echo "Run this script in the top-level package directory"
  exit 1
fi

pkg=$(awk -F ":[[:space:]]*" 'tolower($1)=="name"    { print $2 }' < "$cabal_file")
ver=$(awk -F ":[[:space:]]*" 'tolower($1)=="version" { print $2 }' < "$cabal_file")

if [ -z "$pkg" ]; then
  echo "Unable to determine package name"
  exit 1
fi

if [ -z "$ver" ]; then
  echo "Unable to determine package version"
  exit 1
fi

echo "Detected package: $pkg-$ver"

dir=$(mktemp -d build-docs.XXXXXX)
trap 'rm -r "$dir"' EXIT

export PATH=$(stack path --bin-path)

ghc --version
cabal --version
stack --version

if haddock --hyperlinked-source >/dev/null
then
  echo "Using fancy hyperlinked source"
  HYPERLINK_FLAG="--haddock-option=--hyperlinked-source"
else
  echo "Using boring hyperlinked source"
  HYPERLINK_FLAG="--hyperlink-source"
fi

# Cabal dist in temporary location
builddir=$dir/dist

# Build dependencies haddocks with stack, so we get links
stack haddock --only-dependencies

# Configure using stack databases
snapshotpkgdb=$(stack path --snapshot-pkg-db)
localpkgdb=$(stack path --local-pkg-db)
cabal configure -v2 --builddir=$builddir --package-db=clear --package-db=global --package-db=$snapshotpkgdb --package-db=$localpkgdb

# Build Hadckage compatible docs
cabal haddock -v2 --builddir=$builddir $HYPERLINK_FLAG --html-location='/package/$pkg-$version/docs' --contents-location='/package/$pkg-$version'

# Copy into right directory
cp -R $builddir/doc/html/$pkg/ $dir/$pkg-$ver-docs

# Tar and gzip
tar cvz -C $dir --format=ustar -f $dir/$pkg-$ver-docs.tar.gz $pkg-$ver-docs

# Upload
curl -X PUT \
     -H 'Content-Type: application/x-tar' \
     -H 'Content-Encoding: gzip' \
     -u "$user" \
     --data-binary "@$dir/$pkg-$ver-docs.tar.gz" \
     "https://hackage.haskell.org/package/$pkg-$ver/docs"
