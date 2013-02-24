from lib.io.xrm_mapfile import GSEXRM_MapFile
g = GSEXRM_MapFile(folder='MapDir')
g.process() # maxrow=100)
g.close()


# h = GSEXRM_MapFile(folder='MapDir')
# h.process(maxrow=25)


