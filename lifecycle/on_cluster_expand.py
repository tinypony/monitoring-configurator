#!/usr/bin/python

import subprocess
import json
import argparse

KAFKA_BIN = '/opt/kafka/bin'
topics_file_path='/tmp/topics_to_rebalance.json'
rebalance_config_path='/tmp/rebalance_config.json'

def create_topics_file(dest_path):
    kafka_script = '%s/kafka-topics.sh' % (KAFKA_BIN,)
    p = subprocess.Popen([kafka_script, '--zookeeper', 'localhost:2181', '--list'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()

    topics_dict = { 'topics': [], 'version': 1 }

    for line in out.split('\n'):
        if len(line) is 0:
            pass
        else:
            topics_dict['topics'].append({'topic': line})

    with open(dest_path, 'w+') as f:
        f.write(json.dumps(topics_dict))
        print 'success'

def generate_rebalance_config(topics_json, rebalance_config_path, broker_list):
    kafka_script = '%s/kafka-reassign-partitions.sh' % (KAFKA_BIN,)
    p = subprocess.Popen([kafka_script, '--zookeeper', 'localhost:2181', '--topics-to-move-json-file', topics_json, '--broker-list', broker_list, '--generate'],\
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    show = False

    with open(rebalance_config_path, 'w+') as f:
        for line in out.split('\n'):
            if show and len(line) != 0:
                f.write(line)
            elif "Proposed" in line:
                show = True

def execute_rebalance(rebalance_config_json):
    kafka_script = '%s/kafka-reassign-partitions.sh' % (KAFKA_BIN,)
    p = subprocess.Popen([kafka_script, '--zookeeper', 'localhost:2181', '--reassignment-json-file', rebalance_config_json, '--execute'],\
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    print out
    print err

parser = argparse.ArgumentParser(description='Rebalances all kafka topics across all available nodes')
parser.add_argument('--brokers', required=True, dest='broker_list', type=str, help='Comma separated ids of the brokers to distribute partitions across')
args = parser.parse_args()

create_topics_file(topics_file_path)
generate_rebalance_config(topics_file_path, rebalance_config_path, args.broker_list)
execute_rebalance(rebalance_config_path)