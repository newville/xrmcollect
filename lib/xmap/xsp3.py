#!/usr/bin/python
import sys
import os
import time
from  epics import Device, caget, caput
import numpy
from ..utils import OrderedDict, debugtime

from ConfigParser import ConfigParser

MAX_ROIS = 16

class XSP3(Device):
    """very simple XSPRESS3 interface"""
    attrs = ('NumImages','Acquire','ERASE', 'TriggerMode',
             'DetectorState_RBV', 'NumImages_RBV')

    
    _nonpvs  = ('_prefix', '_pvs', '_delim', 'filesaver', 'fileroot',
                'pathattrs', '_nonpvs', 'nmca', 'dxps', 'mcas')

    pathattrs = ('FilePath', 'FileTemplate',
                 'FileName', 'FileNumber', 
                 'Capture',  'NumCapture')

    def __init__(self, prefix, nmca=4, filesaver='HDF5:',
                 fileroot='/home/xspress3/cars5/Data'):
        self.nmca = nmca
        attrs = list(self.attrs)
        attrs.extend(['%s%s' % (filesaver,p) for p in self.pathattrs])

        self.filesaver = filesaver
        self.fileroot = fileroot
        self._prefix = prefix

        Device.__init__(self, prefix, attrs=attrs, delim='')

        time.sleep(0.1)

    def get_rois(self):
        roidat = []
        for imca in range(1, self.nmca+1):
            roi = OrderedDict()
            pref = "%sC%i" % (self._prefix, imca)
            for iroi in range(1, MAX_ROIS+1):
                name = caget("%s_ROI%i:AttrName" % (pref, iroi), as_string=True)
                if name is not None and len(name) > 0:
                    lo = caget("%s_MCA_ROI%i_LLM" % (pref, iroi))
                    hi = caget("%s_MCA_ROI%i_HLM" % (pref, iroi))
                    roi[name] = (lo, hi)
            roidat.append(roi)
        return roidat

    
    def load_roi_file(self, filename='ROI.dat'):
        cp = ConfigParser()
        cp.read(filename)
        for iroi, opt in enumerate(cp.options('rois')):
            iroi += 1
            if iroi  > MAX_ROIS: break
            dat = cp.get('rois', opt)
            roiname, dat = [i.strip() for i in dat.split('|')]
            dat = [int(i.strip()) for i in dat.split()]
            nmcas = len(dat)/2
            for imca in range(nmcas):
                pv_nm = "%sC%i_ROI%i:AttrName" % (self._prefix, imca+1, iroi)
                pv_lo = "%sC%i_MCA_ROI%i_LLM" % (self._prefix, imca+1, iroi)
                pv_hi = "%sC%i_MCA_ROI%i_HLM" % (self._prefix, imca+1, iroi)
                caput(pv_nm, roiname)
                caput(pv_hi, dat[2*imca+1])
                caput(pv_lo, dat[2*imca])
                
                
    def roi_calib_info(self):
        buff = ['[rois]']
        add = buff.append
        roidat = self.get_rois()

        for i, k in enumerate(roidat[0].keys()):
            s = [list(roidat[m][k]) for m in range(self.nmca)]
            rd = repr(s).replace('],', '').replace('[', '').replace(']','').replace(',','')
            add("ROI%2.2i = %s | %s" % (i,k,rd))

        add('[calibration]')
        add("OFFSET = %s " % (' '.join(["0.00 "] * self.nmca)))
        add("SLOPE  = %s " % (' '.join(["0.10 "] * self.nmca)))                                        
        add("QUAD   = %s " % (' '.join(["0.00 "] * self.nmca)))

        add('[dxp]')
        return buff

    def useExternalTrigger(self):
        self.TriggerMode = 2

    def setTriggerMode(self, mode):
        self.TriggerMode = mode
        
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

    def setFilePath(self, pathname):
        fullpath = os.path.join(self.fileroot, pathname)
        return self.filePut('FilePath', fullpath)

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
