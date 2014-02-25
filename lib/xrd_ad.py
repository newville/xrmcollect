#!/usr/bin/python
import sys
import time
import epics

class PerkinElmer_AD(epics.Device):
    camattrs = ('PEAcquireOffset', 'ImageMode', 'TriggerMode',
                'Acquire',  'AcquireTime', 'NumImages')

    pathattrs = ('FilePath', 'FileTemplate', 'FileWriteMode',
                 'FileName', 'FileNumber', 'FullFileName_RBV',
                 'Capture',  'NumCapture', 'WriteFile_RBV',
                 'AutoSave', 'EnableCallbacks',  'ArraySize0_RBV',
                 'FileTemplate_RBV', 'FileName_RBV', 'AutoIncrement')

    _nonpvs  = ('_prefix', '_pvs', '_delim', 'filesaver',
                'camattrs', 'pathattrs', '_nonpvs')

    def __init__(self,prefix, filesaver='netCDF1:'):
        camprefix = prefix + 'cam1:'
        epics.Device.__init__(self, camprefix, delim='',
                              mutable=False,
                              attrs=self.camattrs)

        for p in self.pathattrs:
            pvname = '%s%s%s' % (prefix, filesaver, p)
            self.add_pv(pvname, attr=p)

    def AcquireOffset(self, timeout=15):
        print 'NEED TO CLOSE SHUTTER!! '
        image_mode_save = self.ImageMode 
        trigger_mode_save = self.TriggerMode 
        self.ImageMode = 0
        self.TriggerMode = 0
        time.sleep(0.25)
        self.PEAcuireOffset = 1
        t0 = time.time()
        while self.PEAcuireOffset > 0 and time.time()-t0 < timeout:
            time.sleep(0.1)

        self.ImageMode = image_mode_save
        self.TriggerMode = trigger_mode_save

    def SetExposureTime(self, t):
        self.AcquireTime = t
        self.AcquireOffset()
        

    def filePut(self,attr, value, **kw):
        return self.put("%s%s" % (self.filesaver, attr), value, **kw)

    def fileGet(self, attr, **kw):
        return self.get("%s%s" % (self.filesaver, attr), **kw)

    def setFilePath(self, pathname):
        return self.filePut('FilePath', pathname)

    def setFileTemplate(self, fmt):
        return self.filePut('FileTemplate', fmt)

    def setFileWriteMode(self, mode):
        return self.filePut('FileWriteMode', mode)

    def setFileName(self, fname):
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

