#!/bin/bash

RDKAFKA_VER="0d540ab4e78a3e3661fe07ee328e2f61fb77f2c3"

PRJ=$PWD
DST="$PRJ/.librdkafka"
VERSION_FILE="$DST/version.txt"

if [ -f $VERSION_FILE ]; then
    echo "Found librdkafka: $(cat $VERSION_FILE), expected: $RDKAFKA_VER"
else
    echo "librdkafka not found in $DST"
fi

if [ -f $VERSION_FILE ] && [ "$(cat $VERSION_FILE)" == $RDKAFKA_VER ]; then
    echo "Required version found, using it"
    sudo cp -r $DST/* /usr/local/
    exit 0
fi

echo "Making librdkafka ($RDKAFKA_VER)"
SRC=`mktemp -d 2>/dev/null || mktemp -d -t 'rdkafka'`
git clone https://github.com/edenhill/librdkafka "$SRC"
cd $SRC
git reset $RDKAFKA_VER --hard

./configure --prefix $DST
cd src
make && make install
sudo cp -r $DST/* /usr/local/

echo "Writing version file to $VERSION_FILE"
echo $RDKAFKA_VER > $VERSION_FILE
