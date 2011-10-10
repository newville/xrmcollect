import os
import sys
import copy
import glob
import shutil

import time
import numpy
import h5py

from string import printable
from ConfigParser import  ConfigParser

from xmap_nc import read_xmap_netcdf
from ..utils import debugtime
from ..config import FastMapConfig

from file_utils import nativepath
from mapfolder import (readASCII, readMasterFile, readEnvironFile,
                        readScanConfig, readROIFile)

off_struck = 0
off_xmap   = 0


class H5Writer(object):
    ScanFile   = 'Scan.ini'
    EnvFile    = 'Environ.dat'
    ROIFile    = 'ROI.dat'
    MasterFile = 'Master.dat'

    h5_attrs = {'Version': '1.1.0',
                'Title': 'Epics Scan Data',
                'Beamline': 'GSECARS, 13-IDE / APS'}

    def __init__(self, folder=None, **kw):
        self.folder = folder
        self.master_header = None
        self.environ = None
        self.roidata = None
        self.scanconf = None
        self.last_row = 0
        self.buff = []

        self.user_titles = []
        self.scan_regions= []


        self.env_desc    = []
        self.env_addr    = []
        self.env_val     = []

        self.pos         = []
        self.det         = []
        self.det_corr = []
        self.realtime = []
        self.livetime = []

        self.pos_desc    = []
        self.pos_addr    = []
        self.det_desc    = []
        self.det_addr    = []

        self.sums       = []
        self.sums_corr  = []
        self.sums_names = []
        self.sums_list  = []
        self.dt_factor  = []

        self.xrf_data     = []
        self.xrf_corr     = []

        self.xrf_sum      = []
        self.xrf_energies = []
        self.xrf_header = ''
        self.xrf_dict   = {}
        self.merge  = None
        self.merge_corr  = None
        self.roi_desc  = []
        self.roi_addr  = []
        self.roi_llim  = []
        self.roi_hlim  = []
        self.xvals  = []
        self.yvals = []

    def write_h5file(self, h5name=None):
        if h5name is None:
            h5name = self.filename + '.h5'

        try:
            fh = h5py.File(h5name, 'w')
            print 'saving hdf5 file %s' % h5name
        except:
            print 'write_h5file error??? ', h5name

        def add_group(group,name,dat=None,attrs=None):
            g = group.create_group(name)
            if isinstance(dat,dict):
                for key,val in dat.items():
                    g[key] = val
            if isinstance(attrs,dict):
                for key,val in attrs.items():
                    g.attrs[key] = val
            return g

        def add_data(group, name, data, attrs=None, **kws):
            # print 'create group in HDF file ', name
            kwargs = {'compression':4}
            kwargs.update(kws)
            # print '   add data ', name, group
            d = group.create_dataset(name,data=data, **kwargs)
            if isinstance(attrs,dict):
                for key,val in attrs.items():
                    d.attrs[key] = val

            return d


        mainattrs = copy.deepcopy(self.h5_attrs)
        mainattrs.update({'Collection Time': self.start_time})

        maingroup = add_group(fh,'data', attrs=mainattrs)

        g = add_group(maingroup,'environ')
        add_data(g,'desc',  self.env_desc)
        add_data(g,'addr',  self.env_addr)
        add_data(g,'value', self.env_val)

        roigroup = add_group(maingroup,'rois')
        add_data(roigroup, 'roi_labels',  self.roi_desc)
        add_data(roigroup, 'roi_lo_limit',self.roi_llim)
        add_data(roigroup, 'roi_hi_limit',self.roi_hlim)


        scan_attrs = {'dimension':self.dimension,
                      'stop_time':self.stop_time,
                      'start_time':self.start_time,
                      'scan_prefix': 'FAST',
                      'correct_deadtime': 'True'}

        scangroup = add_group(maingroup,'roi_scan', attrs=scan_attrs)

        add_data(scangroup,'det',            self.det)
        add_data(scangroup,'det_corrected',  self.det_corr)
        add_data(scangroup,'det_desc',       self.det_desc)
        add_data(scangroup,'det_addr',       self.det_addr)

        add_data(scangroup,'sums',           self.sums)
        add_data(scangroup,'sums_corrected', self.sums_corr)
        add_data(scangroup,'sums_desc',      self.sums_desc)

        add_data(scangroup,'x', self.xvals,     attrs={'desc':self.xdesc, 'addr':self.xaddr})

        if self.dimension  > 1:
            add_data(scangroup,'y', self.yvals, attrs={'desc':self.ydesc, 'addr':self.yaddr})

        add_data(scangroup,'user_titles', self.user_titles)

        en_attrs = {'units':'keV'}

        xrf_shape = self.xrf_data.shape
        gattrs = {'dimension':self.dimension,'nmca':xrf_shape[-1]}


        gattrs.update({'ndetectors':xrf_shape[-2]})
        gxrf = add_group(maingroup,'full_xrf',attrs=gattrs)
        add_data(gxrf,'dt_factor', self.dt_factor)
        add_data(gxrf,'realtime', self.realtime)
        add_data(gxrf,'livetime', self.livetime)

        #add_data(g, 'header', self.xrf_header)
        add_data(gxrf, 'data',   self.xrf_data)
        #add_data(gxrf, 'data_corrected',   self.xrf_corr)
        add_data(gxrf, 'energies', self.xrf_energies, attrs=en_attrs)

        fh.close()
        return None

        #g = add_group(maingroup,'merged_xrf',attrs=gattrs)
        #add_data(g, 'data', self.merge)
        #add_data(g, 'data_corrected', self.merge_corr)
        #add_data(g, 'energies',  self.xrf_energies[0,:], attrs=en_attrs)

        #print self.merge.shape, self.merge.dtype
        #print self.merge_corr.shape, self.merge_corr.dtype

        #print self.xrf_data.shape, self.xrf_data.dtype
        #print self.xrf_corr.shape, self.xrf_corr.dtype


    def ReadMaster(self):
        self.rowdata = None
        self.master_header = None

        if self.folder is not None:
            fname = os.path.join(nativepath(self.folder), self.MasterFile)
            # print  'H5Writer Read Scan file ', fname
            self.stop_time = os.stat(fname).st_mtime

            if os.path.exists(fname):
                try:
                    header, rows = readMasterFile(fname)
                except:
                    print 'Cannot read Scan folder'
                    return
                self.master_header = header
                self.rowdata = rows
                self.starttime = self.master_header[0][6:]
        if self.environ is None:
            self.environ = readEnvironFile(os.path.join(self.folder, self.EnvFile))
            self.env_val, self.env_addr, self.env_desc = [], [], []
            for eline in self.environ:
                eline = eline.replace('\t',' ').strip()
                desc, val = eline[1:].split('=')
                val = val.strip()
                addr = ''
                desc = desc.strip()
                if '(' in desc:
                    n = desc.rfind('(')
                    addr = desc[n+1:-1]
                    if addr.endswith(')'): addr = addr[:-1]
                    desc = desc[:n].rstrip()
                self.env_val.append(val)
                self.env_desc.append(desc)
                self.env_addr.append(addr)


        if self.scanconf is None:
            fastmap = FastMapConfig()
            self.slow_positioners = fastmap.config['slow_positioners']
            self.fast_positioners = fastmap.config['fast_positioners']

            self.scanconf, self.generalconf, self.start_time = readScanConfig(self.folder)
            scan = self.scanconf
            self.mca_prefix = self.generalconf['xmap']

            self.dimension = self.scanconf['dimension']
            self.user_titles = self.scanconf['comments'].split('\n')
            # print 'USER TITLES ', self.user_titles, type(self.user_titles)

            self.filename = self.scanconf['filename']

            self.xaddr = self.scanconf['pos1']
            self.xdesc = self.slow_positioners[self.xaddr]

            self.ixaddr = -1
            for i, posname in enumerate(self.fast_positioners):
                if posname == self.xaddr:
                    self.ixaddr = i

            if self.dimension > 1:
                self.yaddr =self.scanconf['pos2']
                self.ydesc = self.slow_positioners[self.yaddr]

        if self.roidata is None:
            # print 'Read ROI data from ', os.path.join(self.folder,self.ROIFile)
            self.roidata, self.calib = readROIFile(os.path.join(self.folder,self.ROIFile))
            for iroi,label,roidat in self.roidata:
                # print 'ROI ', iroi, label, self.mca_prefix
                # print "%smca1.R%i" % (self.mca_prefix, iroi)
                self.roi_desc.append(label)
                self.roi_addr.append("%smca%%i.R%i" % (self.mca_prefix, iroi))
                self.roi_llim.append([roidat[i][0] for i in range(4)])
                self.roi_hlim.append([roidat[i][1] for i in range(4)])

            self.roi_llim = numpy.array(self.roi_llim)
            self.roi_hlim = numpy.array(self.roi_hlim)

    def make_header(self):
        def add(x):
            self.buff.append(x)
        yval0 = self.rowdata[0][0]

        add('; Epics Scan %s dimensional scan' % self.dimension)
        if int(self.dimension) == 2:
            add(';2D %s: %s' % (self.yaddr, yval0))
        add('; current scan = 1')
        add('; scan dimension = %s' % self.dimension)
        add('; scan prefix = FAST')
        add('; User Titles:')
        for i in self.user_titles:
            add(';   %s' % i)
        #         add('; PV list:')
        #         for t in self.environ:  add("%s"% t)

        if self.scanconf is not None:
            add('; Scan Regions: Motor scan with        1 regions')
            add(';       Start       Stop       Step    Time')
            add(';     %(start1)s      %(stop1)s      %(step1)s     %(time1)s' % self.scanconf)

        add('; scan %s'  % self.master_header[0][6:])
        add(';====================================')


    def process(self, maxrow=None):
        print '=== HDF5 Writer: ', self.folder
        self.ReadMaster()

        if self.last_row == 0 and len(self.rowdata)>0:
            self.make_header()

        if maxrow is None:
            maxrow = len(self.rowdata)

        while self.last_row <  maxrow:
            irow = self.last_row
            self.last_row += 1
            print '>H5Writer.process row %i of %i' % (self.last_row, len(self.rowdata))
            yval, xmapfile, struckfile, gatherfile, dtime = self.rowdata[irow]

            self.yvals.append(yval)

            shead,sdata = readASCII(os.path.join(self.folder,struckfile))
            ghead,gdata = readASCII(os.path.join(self.folder,gatherfile))
            t0 = time.time()
            atime = -1
            while atime < 0 and time.time()-t0 < 10:
                try:
                    atime = time.ctime(os.stat(os.path.join(self.folder,
                                                            xmapfile)).st_ctime)
                    xmfile = os.path.join(self.folder,xmapfile)
                    xmapdat     = read_xmap_netcdf(xmfile, verbose=False)
                except:
                    print 'xmap data failed to read'
                    sys.exit()
                    # self.clear()
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
            npts = min(snpts,gnpts,xnpts)
            npts = gnpts-1

            # print gdata.shape, sdata.shape, xmdat.shape, npts

            points = range(1, npts+1)

            if irow % 2 != 0:
                points.reverse()
                xm_tr = xm_tr[::-1]
                xm_tl = xm_tl[::-1]
                xmicr = xmicr[::-1]
                xmocr = xmocr[::-1]
                xmdat = xmdat[::-1]

            if irow == 0:
                # print 'Row 0: ', self.xaddr, self.slow_positioners[self.xaddr]
                #if self.dimension > 1:
                #    print 'Row 0: ', self.yaddr, self.slow_positioners[self.yaddr]
                self.xvals = [(gdata[ipt, self.ixaddr] + gdata[ipt-1, self.ixaddr])/2.0
                               for ipt in points]

                self.det_addr = [i.strip() for i in shead[-2][1:].split('|')]
                self.det_desc = [i.strip() for i in shead[-1][1:].split('|')]
                self.sums_desc = self.det_desc[:]

                off, slope = self.calib['offset'], self.calib['slope']
                nchan = len(self.calib['offset'])


                xnpts, nchan, nelem = xmdat.shape

                enx = [(off[i] + slope[i]*numpy.arange(nelem)) for i in range(nchan)]
                self.xrf_energies = numpy.array(enx)

                for addr in self.roi_addr:
                    self.det_addr.extend([addr % (i+1) for i in range(nchan)])
                for desc in self.roi_desc:
                    self.det_desc.extend(["%s (mca%i)" % (desc, i+1) for i in range(nchan)])
                    self.sums_desc.append(desc)

            these_x = [(gdata[ipt, self.ixaddr] + gdata[ipt-1, self.ixaddr])/2.0
                       for ipt in points]

            row_det, row_detc, row_sum, row_sumc = [],[],[],[]
            for ipt in points:
                spt = ipt-1
                if spt >= sdata.shape[0]:
                    spt = sdata.shape[0]-1
                rdat = [ixs for ixs in sdata[spt,:]]
                icr_corr = xmicr[ipt,:] /  (1.e-10 + 1.0*xmocr[ipt,:])
                raw, cor, sum, sumcor = rdat, rdat[:], rdat[:], rdat[:]
                for iroi, lab, rb in self.roidata:
                    iraw = [xmdat[ipt, i, rb[i][0]:rb[i][1]].sum()  for i in range(4)]
                    icor = [iraw[i] * icr_corr[i] for i in range(4)]
                    raw.extend(iraw)
                    cor.extend(icor)
                    sum.append(numpy.array(iraw).sum())
                    sumcor.append(numpy.array(icor).sum())

                row_det.append(raw)
                row_detc.append(cor)
                row_sum.append(sum)
                row_sumc.append(sumcor)

            self.det.append(row_det)
            self.det_corr.append(row_detc)
            self.sums.append(row_sum)
            self.sums_corr.append(row_sumc)

            self.realtime.append(xm_tr)
            self.livetime.append(xm_tr)
            self.dt_factor.append(xmicr*1.0/xmocr)
            self.xrf_data.append(xmdat)
            corr = ((xmicr*1.0/xmocr)*xmdat.transpose((2,0,1))).transpose(1,2,0)
            self.xrf_corr.append(corr)

        print 'Done reading.. ', len(self.xrf_data)
        t0 = time.time()
        self.xrf_data = numpy.array(self.xrf_data)
        print ' .. converted to numpy array '
        self.xrf_corr = numpy.array(self.xrf_corr)
        self.realtime = numpy.array(self.realtime)
        self.livetime = numpy.array(self.livetime)
        self.dt_factor = numpy.array(self.dt_factor)

        self.det = numpy.array(self.det)
        self.sums = numpy.array(self.sums)
        self.det_corr = numpy.array(self.det_corr)
        self.sums_corr = numpy.array(self.sums_corr)

        #print 'FULL ', self.xrf_data.shape
        #self.merge      = self.xrf_data[:,:,0,:]*1.0
        #self.merge_corr = self.xrf_corr[:,:,0,:]*1.0
        #en_merge = self.xrf_energies[0,:]
        #n1, n2, nchan =  self.merge.shape
        #print ' Merge ', n1, n2, nchan
#         for ix in range(n1):
#             for iy in range(n2):
#                 sumr = self.merge[ix, iy, :]
#                 sumc = self.merge_corr[ix, iy, :]
#                 for ic in range(1,4):
#                     en = self.xrf_energies[ic,:]
#                     sumr += numpy.interp(en_merge, en, self.xrf_data[ix,iy,ic,:])
#                     sumc += numpy.interp(en_merge, en, self.xrf_corr[ix,iy,ic,:])
#                 self.merge[ix, iy, :] = sumr
#                 self.merge_corr[ix, iy, :] = sumc
# #                 # print 'ix,iy merge=', ix, iy, sumr.shape, sumc.shape, sumr.sum(), sumc.sum()

if __name__ == '__main__':
    dirname = '../SRM1833_map_001'
    ms = H5Writer(folder=dirname)
    ms.process()

