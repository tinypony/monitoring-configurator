#!/bin/bash

mkdir /opt/monitoring-configurator
cp -r ./* /opt/monitoring-configurator/

cd /opt/monitoring-configurator
npm install

if hash systemctl 2>/dev/null; then
        echo "Use systemd"
	cp ./systemd/monitoring-configurator.service /etc/systemd/system/monitoring-configurator.service
        systemctl start monitoring-configurator
elif [[ -d "/usr/share/upstart" ]]; then
        echo "Use upstart"
        cp ./upstart/monitoring-configurator.conf /etc/init/monitoring-configurator.conf
        start monitoring-configurator
fi
