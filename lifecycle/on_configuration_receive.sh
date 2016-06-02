#!/bin/bash
brokid=$1
zookeeper_host=$2
zookeeper_port=$3

sudo stop kafka

sed -i "s/^.*broker\.id=.*$/broker.id=`echo $brokid`/g" /opt/kafka/config/server.properties

if [ -f /tmp/kafka-logs/meta.properties ]; then
	sed -i "s/^.*broker\.id=.*$/broker.id=`echo $brokid`/g" /tmp/kafka-logs/meta.properties
fi
sed -i "s/^.*zookeeper\.connect=.*$/zookeeper.connect=`echo $zookeeper_host:$zookeeper_port`/g" /opt/kafka/config/server.properties

sudo start kafka