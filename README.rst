Klever KLog
===========

While developing a program, have you ever scrolled and scrolled
through your progam output looking for a trace buried in hundreds of
lines of log? If so, kleverklog is for you.

Klever KLog uses Apache Kafka to store your program log and provides a
Kafka client to search, filter and navigate your logs.

Usage
-----

To use Klever KLog, you need a Kafka server. It is easy to setup. You can run
one locally, without root privileges. Just follow the `quickstart
instructions in the Kafka documentation`_.

.. _`quickstart instructions in the Kafka documentation`: http://kafka.apache.org/documentation.html#quickstart

Klever KLog supports Python `logging` and Twisted `twisted.logger`
logging systems. Depending of which one you use, setup is slightly
different.

If you use Python `logging`:

.. code:: python

   import logging
   from kleverklog.python_logging import KafkaHandler

   logger = logging.getLogger('your logger name')
   logger.setLevel(logging.DEBUG) # or any other level you like
   kh = KafkaHandler('localhost:9092')
   logger.addHandler(kh)

If you are using `twisted.logger`:

.. code:: python

    from kleverklog.twisted_logger import KafkaLogService

    KafkaLogService.activate('localhost:9092')

Your program will now send its log entries to Kafka. To view them,
start the log viewer in a terminal:

.. code:: bash

   kleverklog all.json

Run your application. The logs should be displayed by Klever KLog,
colored by log level.

In the Klever KLog viewer, you can give the following commands:

`mPATTERN`
  Set the match pattern to PATTERN. New log lines containing PATTERN
  will be highlighted.

`rN`
  Redisplay the last NN log lines, highlighting the ones matching
  the match pattern.

`R`
  Redisplay the whole log, highlighting the lines matching
  the match pattern.

`p`
  Pause the log display.

`P`
  Resume the log display.

`q`
  Quit Klever KLog
