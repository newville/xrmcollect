#!/usr/bin/python
import sys
import time
import epics
import numpy
from ..utils import OrderedDict, debugtime


class XSP3(epics.Device):
    """very simple XSPRESS3 interface"""
    attrs = ('NumImages','Acquire','ERASE', 'DetectorState_RBV')

    pathattrs = ('FilePath', 'FileTemplate',
                 'FileName', 'FileNumber', 
                 'Capture',  'NumCapture')

    def __init__(self, prefix, filesaver='HDF5:'):
        attrs = list(self.attrs)
        attrs.extend(['%s%s' % (filesaver,p) for p in self.pathattrs])

        self.filesaver = filesaver
        self._prefix = prefix
        print prefix

        print attrs
        
        
        epics.Device.__init__(self, prefix, attrs=attrs, delim='')

        time.sleep(0.1)

    def start(self):
        "Start Struck"
        
        self.ERASE = 1
        time.sleep(.05)
        self.FileCaptureOn()
        self.Acquire = 1

    def stop(self):
        "Stop Struck Collection"
        self.Acquire = 0
        self.FileCaptureOff()

    def filePut(self, attr, value, **kw):
        return self.put("%s%s" % (self.filesaver, attr), value, **kw)

    def setFilePath(self,pathname):
        return self.filePut('FilePath', pathname)

    def setFileTemplate(self,fmt):
        return self.filePut('FileTemplate', fmt)

    def setFileWriteMode(self,mode):
        return self.filePut('FileWriteMode', mode)

    def setFileName(self,fname):
        return self.filePut('FileName', fname)

    def nextFileNumber(self):
        self.setFileNumber(1+self.fileGet('FileNumber'))

    def setFileNumber(self, fnum=None):
        if fnum is None:
            self.filePut('AutoIncrement', 1)
        else:
            self.filePut('AutoIncrement', 0)
            return self.filePut('FileNumber',fnum)

    def getLastFileName(self):
        return self.fileGet('FullFileName_RBV',as_string=True)

    def FileCaptureOn(self):
        return self.filePut('Capture', 1)

    def FileCaptureOff(self):
        return self.filePut('Capture', 0)

    def setFileNumCapture(self,n):
        return self.filePut('NumCapture', n)

    def FileWriteComplete(self):
        return (0==self.fileGet('WriteFile_RBV') )

    def getFileTemplate(self):
        return self.fileGet('FileTemplate_RBV',as_string=True)

    def getFileName(self):
        return self.fileGet('FileName_RBV',as_string=True)

    def getFileNumber(self):
        return self.fileGet('FileNumber_RBV')

    def getFilePath(self):
        return self.fileGet('FilePath_RBV',as_string=True)

    def getFileNameByIndex(self,index):
        return self.getFileTemplate() % (self.getFilePath(), self.getFileName(), index)

if __name__ == '__main__':
    qv = MultiXMAP('13SDD1:', nmca=4)
    qv.Write_CurrentConfig(filename='QuadVortex.conf')
    time.sleep(0.2)
    qv.Write_CurrentConfig(filename='QuadVortex2.conf')
