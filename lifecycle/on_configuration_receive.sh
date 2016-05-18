#!/bin/bash
zookeeper_host=$1
zookeeper_port=$2
sed -i "s/^.*zookeeper\.connect=.*$/zookeeper.connect=`echo $zookeeper_host:$zookeeper_port`/g" /opt/kafka/config/server.properties
sudo restart kafka