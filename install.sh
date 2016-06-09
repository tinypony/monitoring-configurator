#!/bin/bash

#make sure the tools are in place
add-apt-repository ppa:fkrull/deadsnakes-python2.7
apt-get update
apt-get upgrade -y
apt-get install python-dev python-pip -y

pip install pykafka


mkdir -p /opt/monitoring-configurator
cp -r . /opt/monitoring-configurator/

cd /opt/monitoring-configurator
chmod u+x ./lifecycle/*.sh
npm install --production

if hash systemctl 2>/dev/null; then
     echo "Use systemd"
	cp ./systemd/monitoring-configurator.service /etc/systemd/system/monitoring-configurator.service
        systemctl start monitoring-configurator
elif [[ -d "/usr/share/upstart" ]]; then
        echo "Use upstart"
        cp ./upstart/monitoring-configurator.conf /etc/init/monitoring-configurator.conf
        cp ./upstart/hostname-configurator.conf /etc/init/hostname-configurator.conf
        start monitoring-configurator
fi
