# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import argparse
import datetime
import json
import re
import select
import sys

import six

from colored import fg, bg, attr

from kafka import KafkaConsumer

colors = {'DEBUG': attr(0),
          'INFO': fg('cyan'),
          'WARNING': fg('orange_1'),
          'ERROR': fg('red')+bg('white'),
          'CRITICAL': fg('magenta_1'),
}
match_color = bg('yellow')+attr(1)
msg_format = "{level} {time}: {msg} [{topic}:{offset}]"

def json_value_deserializer(m):
    try:
        v = json.loads(m.decode('ascii'))
    except ValueError:
        v = '[ValueError in log] ' + m.decode('ascii')
    return v

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("topic")
    parser.add_argument("-H", "--host", type=str, help="Kafka server and port.",
                        default="localhost:9092")
    parser.add_argument("-r", "--replay", action="store_true",
                        help="Display all available log entries.",
                        default=False)
    parser.add_argument("-m", "--match", type=str, help="Initial match pattern.",
                        default=None)
    args = parser.parse_args()

    pattern = args.match

    if args.replay:
        auto_offset_reset = 'earliest'
    else:
        auto_offset_reset = 'latest'

    if args.topic[-5:] == '.json':
        value_deserializer = json_value_deserializer
    else:
        value_deserializer = None

    consumer = KafkaConsumer(args.topic,
                             group_id=None,
                             bootstrap_servers=args.host,
                             value_deserializer=value_deserializer,
                             auto_offset_reset=auto_offset_reset)

    while True:
        messages = consumer.poll(250)
        for tp in six.itervalues(messages):
            for message in tp:
                if isinstance(message.value, dict):
                    if message.value['klog_level'] in colors:
                        c = colors[message.value['klog_level']]
                    else:
                        c = attr(0)

                    params = {'topic': message.topic,
                              'offset': message.offset,
                              'level': message.value['klog_level'].upper()}
                    params['time'] = str(datetime.datetime.fromtimestamp(float(message.value['klog_time'])))

                    params['msg'] = message.value['klog_message']

                    if pattern and re.search(pattern, params['msg']) is not None:
                        c += match_color

                    msg = msg_format.format(**params)
                else:
                    c = attr(0)
                    msg = message.value

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
                    consumer.seek(tp, max(0, position-offset))
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
                #consumer.close()
                exit()


if __name__ == "__main__":
    main()
