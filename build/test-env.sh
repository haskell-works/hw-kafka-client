#!/bin/bash

 export HOST_IP=`ifconfig | grep "inet addr:" | grep "Bcast:0.0.0.0" | cut -d: -f2 | awk '{ print $1}'`
 docker-compose up -d
 sleep 2