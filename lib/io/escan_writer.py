import os
import sys

import glob
import shutil

import time
import numpy
try:
    import json
except:
    import simplejson as json

from string import printable
from ConfigParser import  ConfigParser

from ..utils import debugtime
from ..config import FastMapConfig

from xmap_nc import read_xmap_netcdf
from file_utils import nativepath
from mapfolder import (readASCII, readMasterFile, readEnvironFile,
                        readScanConfig, readROIFile)


off_struck = 0
off_xmap   = 0

class EscanWriter(object):
    ScanFile   = 'Scan.ini'
    EnvFile    = 'Environ.dat'
    ROIFile    = 'ROI.dat'
    MasterFile = 'Master.dat'

    def __init__(self, folder=None, **kw):
        self.folder = folder
        self.master_header = None
        self.environ = None
        self.roidata = None
        self.scanconf = None
        self.last_row = 0
        self.clear()

    def ReadMaster(self):
        self.rowdata = []
        self.master_header = None

        if self.folder is not None:
            fname = os.path.join(nativepath(self.folder), self.MasterFile)
            if os.path.exists(fname):
                try:
                    header, rows = readMasterFile(fname)
                except:
                    return
                if len(header) < 1:
                    return
                self.master_header = header
                self.rowdata = rows
                self.starttime = self.master_header[0][6:]
        if self.environ is None:
            self.environ = readEnvironFile(os.path.join(self.folder, self.EnvFile))

        if self.roidata is None:
            # print 'Read ROI data from ', os.path.join(self.folder,self.ROIFile)
            self.roidata, calib = readROIFile(os.path.join(self.folder,self.ROIFile))

        if self.scanconf is None:
            fastmap = FastMapConfig()
            self.slow_positioners = fastmap.config['slow_positioners']
            self.fast_positioners = fastmap.config['fast_positioners']

            self.scanconf, self.generalconf = readScanConfig(self.folder)
            scan = self.scanconf
            self.mca_prefix = self.generalconf['xmap']

            self.dim = self.scanconf['dimension']
            self.comments = self.scanconf['comments']
            self.filename = self.scanconf['filename']

            self.pos1 = self.scanconf['pos1']

            self.ipos1 = -1
            for i, posname in enumerate(self.fast_positioners):
                if posname == self.pos1:
                    self.ipos1 = i

            if self.dim > 1:
                self.pos2 =self.scanconf['pos2']

    def make_header(self):
        def add(x):
            self.buff.append(x)
        yval0 = self.rowdata[0][0]

        add('; Epics Scan %s dimensional scan' % self.dim)
        if int(self.dim) == 2:
            add(';2D %s: %s' % (self.pos2,yval0))
        add('; current scan = 1')
        add('; scan dimension = %s' % self.dim)
        add('; scan prefix = FAST')
        add('; User Titles:')
        for i in self.comments.split('\\n'):
            add(';   %s' % i)
        add('; PV list:')
        for t in self.environ:  add("%s"% t)

        if self.scanconf is not None:
            add('; Scan Regions: Motor scan with        1 regions')
            add(';       Start       Stop       Step    Time')
            add(';     %(start1)s      %(stop1)s      %(step1)s     %(time1)s' % self.scanconf)

        add('; scan %s'  % self.master_header[0][6:])
        add(';====================================')

    def clear(self):
        self.buff = []

    def process(self, maxrow=None, verbose=False):
        # print '=== Escan Writer: ', self.folder, self.last_row
        self.ReadMaster()
        if self.last_row >= len(self.rowdata):
            return 0

        def add(x):
            self.buff.append(x)

        if self.last_row == 0 and len(self.rowdata)>0:
            self.make_header()

        if maxrow is None:
            maxrow = len(self.rowdata)
        while self.last_row <  maxrow:
            irow = self.last_row
            if verbose:
               print '>EscanWrite.process row %i of %i' % (self.last_row, len(self.rowdata))

            yval, xmapfile, struckfile, gatherfile, dtime = self.rowdata[irow]

            shead,sdata = readASCII(os.path.join(self.folder,struckfile))
            ghead,gdata = readASCII(os.path.join(self.folder,gatherfile))
            t0 = time.time()
            atime = -1
            while atime < 0 and time.time()-t0 < 10:
                try:
                    atime = time.ctime(os.stat(os.path.join(self.folder,
                                                            xmapfile)).st_ctime)
                    xmapdat     = read_xmap_netcdf(os.path.join(self.folder,xmapfile),verbose=False)
                    #print '.',
                    #if (1+irow) % 20 == 0: print
                    #sys.stdout.flush()
                except:
                    print 'xmap data failed to read'
                    self.clear()
                    atime = -1
                time.sleep(0.03)
            if atime < 0:
                return 0
            # print 'EscanWrite.process Found xmapdata in %.3f sec (%s)' % (time.time()-t0, xmapfile)

            xmdat = xmapdat.data[:]
            xmicr = xmapdat.inputCounts[:]
            xmocr = xmapdat.outputCounts[:]
            xm_tl = xmapdat.liveTime[:]
            xm_tr = xmapdat.realTime[:]

            gnpts, ngather  = gdata.shape
            snpts, nscalers = sdata.shape

            xnpts = xmdat.shape[0]
            # npts = min(snpts,gnpts,xnpts)
            npts = xnpts-1
            # print gdata.shape, sdata.shape, xmdat.shape, npts

            if irow == 0:
                self.npts0 = npts
                add('; scan ended at time: %s'  % atime)
                add('; npts = %i' % npts)
                add('; column labels:')
                p1label = self.slow_positioners[self.pos1]

                add('; P1 = {%s} --> %s (drive)' % (p1label, self.pos1))
                add('; D1 = {MCS Count Time} --> CountTime (ms)')
                add('; D2 = {MCA Real Time} --> RealTime (ms)')
                add('; D3 = {MCA Live Time} --> LiveTime (ms)')
                legend = ['P1','D1', 'D2', 'D3']
                struckPVs = [i.strip() for i in shead[-2][1:].split('|')]
                struckLabels = [i.strip() for i in shead[-1][1:].split('|')]
                for i,pvn in enumerate(struckPVs):
                    add('; D%i = {%s} --> %s' % (i+4,struckLabels[i],pvn))
                    legend.append('D%i' % (i+1))
                    idet = i+1
                idet = idet+3
                # RAW MCAs
                suf,rnam = ('','.R') # ): #  , ('(raw)','.R1')):
                mca="%smca1%s" % (self.mca_prefix, rnam)
                for iroi,label,roidat in self.roidata:
                    idet  = idet + 1
                    legend.append('D%i' % (idet))
                    add('; D%i = {%s%s} --> %s%i' % (idet,label,suf,mca,iroi))

                # Corrected MCAs
                suf,rnam = ('(corr)','.R') # ): #  , ('(raw)','.R1')):
                mca="%smca1%s" % (self.mca_prefix, rnam)
                for iroi,label,roidat in self.roidata:
                    idet  = idet + 1
                    legend.append('D%i' % (idet))
                    add('; D%i = {%s%s} --> %s%iC' % (idet,label,suf,mca,iroi))

                self.legend = ' '.join(legend)
            else:
                if npts > self.npts0:
                    npts = self.npts0
                if abs(self.npts0-npts) > 1:
                    print 'Broken Data : ', npts, self.npts0, irow
                    print ' > NPTS: (xps, struck, xmap, expected: ) =', npts, gnpts, snpts, xnpts, self.npts0
                add(';2D %s: %s' % (self.pos2, yval))
                add('; scan ended at time: %s'  % atime)
            add(';---------------------------------')
            add('; %s' % self.legend)

            points = range(1,npts)
            if off_xmap > 0:
                points = range(1, npts - off_xmap)
            if irow % 2 != 0:
                points.reverse()
            for ipt in points:
                xval = (gdata[ipt,self.ipos1] + gdata[ipt-1,self.ipos1])/2.0

                spt = min(ipt, len(sdata)-1)
                x = ['%.4f %.1f %.1f %.1f' % (xval, sdata[spt,0]*1.e-3,
                                              1000*xm_tr[ipt].mean(),
                                              1000*xm_tl[ipt].mean()) ]  #
                x.extend(['%i' %i for i in sdata[spt+off_struck,:]])
                icr_corr = xmicr[ipt+off_xmap,:] /  (1.e-10 + 1.0*xmocr[ipt+off_xmap,:])
                raw,cor = [],[]
                for iroi,lab,rb in self.roidata:
                    intens = numpy.array([xmdat[ipt+off_xmap, i, rb[i][0]:rb[i][1]].sum()  for i in range(4)])
                    raw.append( intens.sum() )
                    cor.append((intens*icr_corr).sum())
                x.extend(["%i"   % r for r in raw])
                x.extend(["%.4f" % r for r in cor])
                add(' '.join(x))
                # print ipt, raw

            self.last_row += 1
        # print "EscanWrite: ", len(self.buff), ' new lines'
        return len(self.buff)

if __name__ == '__main__':
    import sys
    dirname = '_TestScan'
    ms = MapScan(folder=dirname)
    ms.process(maxrow=2)
    f = open('tmp.001','w')
    f.write('\n'.join(ms.buff))
    f.close()

    print '==========================='
    ms.process(maxrow=3)
    f = open('tmp.002','w')
    f.write('\n'.join(ms.buff))
    f.close()

    print '==========================='
    ms.process()
    f = open('tmp.002','a')
    f.write('\n'.join(ms.buff))
    f.close()

    # ms.write()
    # FastMap2Escan(folder=dirname)

