#!/bin/bash
brokid=$1
zookeeper_host=$2
zookeeper_port=$3
sed -i "s/^.*broker\.id=.*$/broker.id=`echo $brokid`/g" /opt/kafka/config/server.properties
sed -i "s/^.*broker\.id=.*$/broker.id=`echo $brokid`/g" /tmp/kafka-logs/meta.properties
sed -i "s/^.*zookeeper\.connect=.*$/zookeeper.connect=`echo $zookeeper_host:$zookeeper_port`/g" /opt/kafka/config/server.properties
sudo stop kafka
sudo start kafka