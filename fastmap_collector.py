#!/usr/bin/python

# from epicscollect import collector
from lib import collector
from optparse import OptionParser

import sys
xrf_prefix= '13SDD1:'
if len(sys.argv) > 1:
    xrf_prefix = sys.argv[1]

t = collector.TrajectoryScan(xrf_prefix=xrf_prefix)
t.mainloop()
