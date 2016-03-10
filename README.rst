Klever KLog
===========

While developing a program, have you ever scrolled and scrolled
through your progam output looking for a trace buried in hundreds of
lines of log? If so, kleverklog is for you.

Klever KLog uses Apache Kafka to store your program log and provides a
Kafka client to search, filter and navigate your logs.

Usage
-----

.. note::

   Klever KLog currently only works with twisted logging system but
   expansion is under way.

To use Klever KLog, you need a Kafka server. It is easy to setup. You can run
one locally, without root privileges. Just follow the `quickstart
instructions in the Kafka documentation`_.

.. _`quickstart instructions in the Kafka documentation`: http://kafka.apache.org/documentation.html#quickstart

In your program, send your logs to Kafka:

.. code:: python

    from kleverklog import KafkaLogService

    # Somewhere in your program initialization
    KafkaLogService.activate('localhost:9092')

Then, start the log viewer:

.. code:: bash

   python -m kleverklog

Start your application. The logs should be displayed by Klever KLog,
colored by log level.

In Klever KLog, you can give the following commands:

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
