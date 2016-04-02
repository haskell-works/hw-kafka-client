#!/bin/bash

SRC=`mktemp -d 2>/dev/null || mktemp -d -t 'src'`

cd ${SRC}
git clone --depth 1 https://github.com/edenhill/librdkafka librdkafka

cd librdkafka
./configure
cd src
make
sudo make install
