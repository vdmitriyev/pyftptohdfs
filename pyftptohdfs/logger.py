#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import uuid
import time
from datetime import datetime

class Logger(object):

    def __init__(self):
        """ Initializing log file with random name"""

        self.terminal = sys.stdout
        suffix = '{0}-{1}'.format(datetime.fromtimestamp(time.time()).strftime('%Y%m%d-%H%M'), str(uuid.uuid1())[:2])
        self.log = open('logfile-{0}.log'.format(suffix), "a")

    def write(self, message):
        """ Overriding writing method to write to file and stdout at once"""

        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        """flush method is needed for python 3 compatibility
           handles the flush command by doing nothing
           you might want to specify some extra behavior here
        """

        pass
