#!/usr/bin/python

# from epicscollect import collector
from lib import collector
from optparse import OptionParser

usage = "usage: %prog [options] file(s)"
parser = OptionParser(usage=usage, prog="fastmap_collector",
                      version="larch command-line version 0.2")

parser.add_option("-x", "--xrf", dest="xrf_prefix", action="store_true",
                  default='13SDD1:',
                  help="set xrf_prefix, default = 13SDD1:")


(opts, args) = parser.parse_args()

t = collector.TrajectoryScan(xrf_prefix=opts.xrf_prefix)
t.mainloop()
