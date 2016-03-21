# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import json
import threading

import six

from twisted.logger import formatEvent, globalLogPublisher
from kafka import KafkaProducer

lock = threading.RLock()

def kafka_observer(event):
    repr_slots = ['log_logger', 'log_source', 'log_failure']
    event_dict = dict()

    for slot in repr_slots:
        if slot in event:
            event_dict[slot] = repr(event[slot])

    event_dict['log_level'] = event['log_level'].name

    for slot in six.iterkeys(event):
        if slot not in event_dict:
            event_dict[slot] = str(event[slot])

    json_dump = json.dumps(event_dict);
    string = formatEvent(event)

    lock.acquire()
    try:
        KafkaLogService.producer.send(str(event['log_namespace']).encode('utf-8') + '.json',
                                      json_dump.encode('utf-8'))
        KafkaLogService.producer.send('all.json', json_dump.encode('utf-8'))

        KafkaLogService.producer.send(str(event['log_namespace']).encode('utf-8') + '.txt',
                                      string.encode('utf-8'))
        KafkaLogService.producer.send('all.txt', string.encode('utf-8'))
    finally:
        lock.release()

class KafkaLogService(object):
    @classmethod
    def activate(cls, host):
        cls.producer = KafkaProducer(bootstrap_servers=host)
        globalLogPublisher.addObserver(kafka_observer)
