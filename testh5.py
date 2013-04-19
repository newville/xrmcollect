import os, time

from lib.gui.xrm_mapfile import GSEXRM_MapFile
fname = '39t30ASA_39t30roots.002.h5'
if os.path.exists(fname):
    print os.stat(fname).st_mtime

m = GSEXRM_MapFile(fname)
print m
print 'File is Valid? ', m.valid

print 'File has new data available? ',  m.folder_has_newdata()

# if  m.folder_has_newdata():
m.process(maxrow=3, force=True)


time.sleep(2)

print 'continue'
print m.check_hostid(),  m.folder_has_newdata() 
m.process(maxrow=10, force=True)

