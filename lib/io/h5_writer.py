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
from mapfolder import (readASCII, readMasterFile,
                       readEnvironFile, parseEnviron,
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
                'Beamline': 'GSECARS, 13-IDE / APS',
                'Scan_Type': 'Fast Map',
                'Correct_Deadtime': 'True'}

    def __init__(self, folder=None, **kw):
        self.folder = folder
        self.master_header = None

        self.h5file = None

        self.last_row = 0

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

        if self.folder is None:
            return
        fname = os.path.join(nativepath(self.folder), self.MasterFile)
        self.stop_time = os.stat(fname).st_mtime
        if os.path.exists(fname):
            try:
                header, rows = readMasterFile(fname)
            except:
                print 'Cannot read Scan folder'
                return
            self.master_header = header
            self.rowdata = rows
            self.start_time = self.master_header[0][6:]

    def add_group(self, group,name,dat=None,attrs=None):
        g = group.create_group(name)
        if isinstance(dat,dict):
            for key,val in dat.items():
                g[key] = val
        if isinstance(attrs,dict):
            for key,val in attrs.items():
                g.attrs[key] = val
        return g

    def add_data(self, group, name, data, attrs=None, **kws):
        # print 'create group in HDF file ', name
        kwargs = {'compression':4}
        kwargs.update(kws)
        # print '   add data ', name, group
        d = group.create_dataset(name,data=data, **kwargs)
        if isinstance(attrs,dict):
            for key,val in attrs.items():
                d.attrs[key] = val
        return d

    def add_environ(self, group):
        "add environmental data"
        envdat = readEnvironFile(os.path.join(self.folder, self.EnvFile))
        env_desc, env_addr, env_val = parseEnviron(envdat)
        grp = self.add_group(group, 'environ')
        self.add_data(grp, 'desc',  env_desc)
        self.add_data(grp, 'addr',  env_addr)
        self.add_data(grp, 'value', env_val)

    def add_rois(self, group, mca_prefix):
        "add ROI data"
        roidata, calib = readROIFile(os.path.join(self.folder,self.ROIFile))
        for iroi,label,roidat in roidata:
            oi_desc.append(label)
            roi_addr.append("%smca%%i.R%i" % (mca_prefix, iroi))
            roi_llim.append([roidat[i][0] for i in range(4)])
            roi_hlim.append([roidat[i][1] for i in range(4)])

        roi_llim = numpy.array(roi_llim)
        roi_hlim = numpy.array(roi_hlim)

        grp = self.add_group(group,'rois')
        self.add_data(grp, 'roi_labels',  roi_desc)
        self.add_data(grp, 'roi_lo_limit', roi_llim)
        self.add_data(grp, 'roi_hi_limit', roi_hlim)


    def begin_h5file(self):
        """open and start writing to h5file:
        important: only run this once!"""
        if self.h5file is not None or self.folder is None:
            return

        fastmap = FastMapConfig()
        slow_pos = fastmap.config['slow_positioners']
        fast_pos = fastmap.config['fast_positioners']

        scanconf, generalconf, start_time = readScanConfig(self.folder)

        dimension = scanconf['dimension']
        user_titles = scanconf['comments'].split('\n')
        filename = scanconf['filename']

        pos1 = scanconf['pos1']
        pos_addr = [pos1]
        pos_desc = [slow_positioners[pos1]]
        self.ixaddr = -1
        for i, posname in enumerate(fast_positioners):
            if posname == pos1:
                self.ixaddr = i
        if dimension > 1:
            pos_addr.append(self.scanconf['pos2'])
            pos_desc.append(slow_positioners[yaddr])

        #
        h5name = self.filename + '.h5'
        fh = self.h5file = h5py.File(h5name, 'w')
        print 'saving hdf5 file %s' % h5name

        attrs = {'Dimension':dimension,
                 'Stop_Time':self.stop_time,
                 'Start_Time':self.start_time}
        attrs.update(self.h5_attrs)

        h5root = self.add_group(fh, 'data', attrs=attrs)
        self.add_data(h5root, 'user_titles', user_titles)

        self.add_environ(self, h5root)
        self.add_rois(self, h5root, generalconf['xmap'])

        pos = self.add_group(h5root, 'positioners',
                             attr = {'Dimension': dimension})

        self.add_data(pos, 'names', pos_desc)
        self.add_data(pos, 'addresses', pos_addr)

        # self.add_data(pos, 'positions', self.yvals)

        roiscan = self.add_group(h5root, 'roi_scan')
        #         add_data(roiscan,'det',            self.det)
        #         add_data(roiscan,'det_corrected',  self.det_corr)
        #         add_data(roiscan,'det_desc',       self.det_desc)
        #         add_data(roiscan,'det_addr',       self.det_addr)
        #
        #         add_data(roiscan,'sums',           self.sums)
        #         add_data(roiscan,'sums_corrected', self.sums_corr)
        #         add_data(roiscan,'sums_desc',      self.sums_desc)


        #en_attrs = {'units':'keV'}
        #xrf_shape = self.xrf_data.shape
        #gattrs = {'dimension':self.dimension,'nmca':xrf_shape[-1]}
        #gattrs.update({'ndetectors':xrf_shape[-2]})

        gxrf = self.add_group(h5root, 'xrf_spectra') # , attrs=gattrs)
        #add_data(gxrf,'dt_factor', self.dt_factor)
        #add_data(gxrf,'realtime', self.realtime)
        #add_data(gxrf,'livetime', self.livetime)

        #add_data(gxrf, 'data',   self.xrf_data)
        #add_data(gxrf, 'energies', self.xrf_energies, attrs=en_attrs)


    def process(self, maxrow=None):
        print '=== HDF5 Writer: ', self.folder
        self.ReadMaster()

        if self.last_row == 0 and len(self.rowdata)>0:
            self.begin_h5file()

        if maxrow is None:
            maxrow = len(self.rowdata)

        while self.last_row <  maxrow:
            irow = self.last_row
            self.last_row += 1
            print '>H5Writer.process row %i of %i, %s' % (self.last_row,
                                                          len(self.rowdata),
                                                          time.ctime())
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
            self.dt_factor.append(xmicr*1.0/(1.e-10+xmocr))
            self.xrf_data.append(xmdat)
            corr = ((xmicr*1.0/(1.e-10+xmocr))*xmdat.transpose((2,0,1))).transpose(1,2,0)
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

