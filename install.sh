#!/bin/bash

mkdir /opt/monitoring-configurator
cp -r ./* /opt/monitoring-configurator/

cd /opt/monitoring-configurator
mv ./systemd/monitoring-configurator.service /etc/systemd/system/monitoring-configurator.service
#rm -rf ./upstart

npm install
systemctl start monitoring-configurator
