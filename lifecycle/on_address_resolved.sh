#!/bin/bash
correctAddr=$1
sed -i "s/^.*advertised\.host\.name=.*$/advertised.host.name=`echo $correctAddr`/g" /opt/kafka/config/server.properties