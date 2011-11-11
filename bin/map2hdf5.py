from epicscollect.io import H5Writer
import sys

for dirname in sys.argv[1:]:
    writer = H5Writer(folder=dirname)
    writer.process()


