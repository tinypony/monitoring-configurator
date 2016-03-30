#!/bin/bash

mkdir /opt/monitoring-configurator
cp -r ./* /opt/monitoring-configurator/

cd /opt/monitoring-configurator
npm install

if [[ -d "/usr/lib/systemd" ]]; then
        echo "Use systemd"
	mv ./systemd/monitoring-configurator.service /etc/systemd/system/monitoring-configurator.service
        systemctl start monitoring-configurator
elif [[ -d "/usr/share/upstart" ]]; then
        echo "Use upstart"
        mv ./upstart/monitoring-configurator.conf /etc/init/monitoring-configurator.conf
fi
