# Copyright 2012 Johan Rydberg.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Logging utilities."""

import logging
import logging.handlers
import sys
import errno

import multiprocessing, logging, sys, re, os, StringIO, threading, time, Queue
from logging.handlers import RotatingFileHandler
import multiprocessing, threading, logging, sys, traceback


FORMAT_STRING = '%(asctime)s pid-%(process)-6d %(levelname)-6s [%(name)s] %(message)s'
DATE_FORMAT = "%Y-%m-%d %H:%M:%S%z"

 
class StreamToLogger(object):
    """Fake file-like stream object that redirects writes to a logger
    instance.
    """
    
    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.log_level = log_level
        self.linebuf = ''
        
    def write(self, buf):
        """Write data."""
        for line in buf.rstrip().splitlines():
            self.logger.log(self.log_level, line.rstrip())
 
    def flush(self):
        """Flush the file."""
        # We do not buffer anything.


class ChildLogHandler(logging.Handler):
    """Log handler that sends log records via a queue."""

    def __init__(self, queue):
        logging.Handler.__init__(self)
        self.queue = queue
        
    def send(self, s):
        self.queue.put_nowait(s)

    def _format_record(self, record):
        # ensure that exc_info and args
        # have been stringified.  Removes any chance of
        # unpickleable things inside and possibly reduces
        # message size sent over the pipe
        if record.args:
            record.msg = record.msg % record.args
            record.args = None
        if record.exc_info:
            self.format(record)
            record.exc_info = None

        return record

    def emit(self, record):
        try:
            s = self._format_record(record)
            self.send(s)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)


class LogConsumer(threading.Thread):

    def __init__(self, logq, logger):
        threading.Thread.__init__(self)
        self.queue = logq
        self.logger = logger
        self.daemon = True
        self.shutdown = False
        self.polltime = 1
        self.start()
           
    def run(self):
        while (self.shutdown == False) or (self.queue.empty() == False):
            # so we block for a short period of time so that we can
            # check for the shutdown cases.
            try:
                record = self.queue.get(True, self.polltime)
                self.logger.handle(record)
            except Queue.Empty, e:
                pass
            except IOError, e:
                if e.errno != errno.EINTR:
                    raise
