#!/bin/bash

set +e
_image=fpco/stack-build:latest
docker run --rm -v $PWD:/bld -it $_image bash -c "cd /bld && ./scripts/build-librdkafka.sh"
echo "Building the project"
stack --docker --docker-image=$_image test \
        --extra-include-dirs "$PWD/.librdkafka/include/librdkafka" \
        --extra-lib-dirs "$PWD/.librdkafka/lib"