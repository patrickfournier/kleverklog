# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import json
import logging

import six

from kafka import KafkaProducer

class KafkaHandler(logging.Handler):
    def __init__(self, host, *args, **kwargs):
        super(KafkaHandler, self).__init__(*args, **kwargs)
        self.kafka_producer = KafkaProducer(bootstrap_servers=host)

    def emit(self, record):
        message = self.format(record)
        event_dict = {
            'klog_level': record.levelname.upper(),
            'klog_time': record.created,
            'klog_message': message,
        }

        for attribute, value in six.iteritems(vars(record)):
            event_dict[attribute] = value

        json_dump = json.dumps(event_dict)

        self.kafka_producer.send(str(record.name).encode('utf-8') + '.json',
                           json_dump.encode('utf-8'))
        self.kafka_producer.send('all.json', json_dump.encode('utf-8'))

        self.kafka_producer.send(str(record.name).encode('utf-8') + '.txt',
                           message.encode('utf-8'))
        self.kafka_producer.send('all.txt', message.encode('utf-8'))

        self.flush()

    def flush(self):
        self.kafka_producer.flush()
