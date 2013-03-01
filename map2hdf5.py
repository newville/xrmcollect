import sys
from lib.io.xrm_mapfile import GSEXRM_MapFile

mapdir = sys.argv[1]
print sys.argv

g = GSEXRM_MapFile(folder=mapdir)
g.process() # maxrow=100)
g.close()


# h = GSEXRM_MapFile(folder='MapDir')
# h.process(maxrow=25)


