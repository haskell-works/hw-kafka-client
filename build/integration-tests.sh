#!/bin/bash

export DOCKER_IP=`ifconfig | grep "inet addr:" | grep "Bcast:0.0.0.0" | cut -d: -f2 | awk '{ print $1}'`
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib

stack clean
KAFKA_TEST_BROKER=$DOCKER_IP stack test
