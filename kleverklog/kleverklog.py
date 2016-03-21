# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import argparse
import datetime
import json
import os
import re
import select
import sys
import time

import six

from colored import fg, bg, attr

from kafka import KafkaConsumer

colors = {'debug': attr(0),
          'info': fg('cyan'),
          'warn': fg('orange_1'),
          'error': fg('red')+bg('white'),
          'critical': fg('magenta_1'),
}
match_color = bg('yellow')+attr(1)
msg_format = "{level} {time}: {msg} [{topic}:{offset}]"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("topic")
    parser.add_argument("-H", "--host", type=str, help="Kafka server and port.",
                        default="localhost:9092")
    parser.add_argument("-r", "--replay", action="store_true",
                        help="Display all available log entries.", default=False)
    parser.add_argument("-m", "--match", type=str, help="Initial match pattern.", default=None)
    args = parser.parse_args()

    pattern = args.match

    if args.replay:
        auto_offset_reset = 'earliest'
    else:
        auto_offset_reset = 'latest'

    consumer = KafkaConsumer(args.topic,
                             group_id=None,
                             bootstrap_servers=args.host,
                             value_deserializer=lambda m: json.loads(m.decode('ascii')),
                             auto_offset_reset=auto_offset_reset)

    while True:
        messages = consumer.poll(250)
        for tp in six.itervalues(messages):
            for message in tp:
                if message.value['log_level'] in colors:
                    c = colors[message.value['log_level']]
                else:
                    c = 0

                params = {'topic': message.topic,
                          'offset': message.offset,
                          'level': message.value['log_level'].upper()}
                params['time'] = str(datetime.datetime.fromtimestamp(float(message.value['log_time'])))
                try:
                    params['msg'] = message.value['log_format'].format(**(message.value))
                except:
                    params['msg'] = str(message.value)

                if pattern and re.search(pattern, params['msg']) is not None:
                    c += match_color

                msg = msg_format.format(**params)
                print(c+msg+attr(0))

        po = select.poll()
        po.register(sys.stdin, select.POLLIN)
        if po.poll(0):
            ch = sys.stdin.read(1)
            if ch == 'm':
                pattern = sys.stdin.readline().rstrip('\n').encode('utf-8')
                pattern = pattern.rstrip('\n').encode('utf-8')
            elif ch == 'r':
                offset = sys.stdin.readline().rstrip('\n').encode('utf-8')
                offset = int(offset)
                for tp in consumer.assignment():
                    position = consumer.position(tp)
                    consumer.seek(tp, position-offset)
            elif ch == 'R':
                for tp in consumer.assignment():
                    consumer.seek_to_beginning(tp)
            elif ch == 'p':
                for tp in consumer.assignment():
                    consumer.pause(tp)
            elif ch == 'P':
                for tp in consumer.assignment():
                    consumer.resume(tp)
            elif ch == 'q':
                # FIXME: kafka currently (1.0.1) raises an exception on close
                # consumer.close()
                exit()


if __name__ == "__main__":
    main()
