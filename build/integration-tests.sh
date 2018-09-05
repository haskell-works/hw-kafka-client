#!/bin/bash

nix-shell --run " export DOCKER_IP=`ifconfig | grep "inet addr:" | grep "Bcast:0.0.0.0" | cut -d: -f2 | awk '{ print $1}'`; \
                  KAFKA_TEST_BROKER=$DOCKER_IP; \
                  docker-compose up -d; \
                  sleep 2; \
                  nix-build; \
                "