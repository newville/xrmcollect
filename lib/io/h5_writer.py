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

        self.pos      = []
        self.det      = []
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

        self.xrf_energies = []
        self.xrf_header = ''
        self.xrf_dict   = {}

        self.roi_desc  = []
        self.roi_addr  = []
        self.roi_llim  = []
        self.roi_hlim  = []
        self.xvals  = []
        self.yvals = []

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

    def add_group(self, group, name, dat=None, attrs=None):
        """ add an hdf5 group"""
        g = group.create_group(name)
        if isinstance(dat, dict):
            for key, val in dat.items():
                g[key] = val
        if isinstance(attrs, dict):
            for key, val in attrs.items():
                g.attrs[key] = val
        return g

    def add_data(self, group, name, data, attrs=None, **kws):
        """ creata an hdf5 dataset"""
        kwargs = {'compression':4}
        kwargs.update(kws)
        d = group.create_dataset(name, data=data, **kwargs)
        if isinstance(attrs, dict):
            for key, val in attrs.items():
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
        roidata, calib, dxp = readROIFile(os.path.join(self.folder, self.ROIFile))
        roi_desc, roi_addr, roi_llim, roi_hlim = [], [], [], []
        roi_slices = []
        for iroi, label, roidat in roidata:
            roi_desc.append(label)
            roi_addr.append("%smca%%i.R%i" % (mca_prefix, iroi))
            roi_llim.append([roidat[i][0] for i in range(4)])
            roi_hlim.append([roidat[i][1] for i in range(4)])
            roi_slices.append([slice(roidat[i][0], roidat[i][1]) for i in range(4)])
        roi_llim = numpy.array(roi_llim)
        roi_hlim = numpy.array(roi_hlim)

        grp = self.add_group(group, 'rois')

        self.add_data(grp, 'labels',  roi_desc)
        self.add_data(grp, 'addrs',  roi_addr)
        self.add_data(grp, 'lo_limit', roi_llim)
        self.add_data(grp, 'hi_limit', roi_hlim)
        grp = self.add_group(group, 'calibration')
        for key, val in calib.items():
            self.add_data(grp, key, val)

        grp = self.add_group(group, 'dxp_settings')
        for key, val in dxp.items():
            self.add_data(grp, key, val)

        self.roi_desc = roi_desc
        self.roi_addr = roi_addr
        self.roi_slices = roi_slices
        self.calib = calib

        sys.exit()

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
        pos_desc = [slow_pos[pos1]]
        self.ixaddr = -1
        for i, posname in enumerate(fast_pos):
            if posname == pos1:
                self.ixaddr = i
        if dimension > 1:
            yaddr = scanconf['pos2']
            pos_addr.append(yaddr)
            pos_desc.append(slow_pos[yaddr])

        #
        h5name = filename + '.h5'
        fh = self.h5file = h5py.File(h5name, 'w')
        print 'saving hdf5 file %s' % h5name

        attrs = {'Dimension':dimension,
                 'Stop_Time':self.stop_time,
                 'Start_Time':self.start_time}
        attrs.update(self.h5_attrs)

        h5root = self.h5root = self.add_group(fh, 'xrf_map', attrs=attrs)
        self.add_data(h5root, 'user_titles', user_titles)

        self.add_environ(h5root)
        self.add_rois(h5root, generalconf['xmap'])

        pos = self.add_group(h5root, 'positioners',
                             attrs = {'Dimension': dimension})

        self.add_data(pos, 'names', pos_desc)
        self.add_data(pos, 'addrs', pos_addr)

        # self.add_data(pos, 'positions', self.yvals)

        roiscan = self.add_group(h5root, 'roi_scan')
        gxrf = self.add_group(h5root, 'xrf_spectra')
        # , attrs=gattrs)

        #         add_data(roiscan,'det',       self.det)
        #         add_data(roiscan,'det_corr',  self.det_corr)
        #         add_data(roiscan,'det_desc',  self.det_desc)
        #         add_data(roiscan,'det_addr',  self.det_addr)
        #
        #         add_data(roiscan,'sums',       self.sums)
        #         add_data(roiscan,'sums_corr',  self.sums_corr)
        #         add_data(roiscan,'sums_desc',  self.sums_desc)


        #en_attrs = {'units':'keV'}
        #xrf_shape = self.xrf_data.shape
        #gattrs = {'dimension':self.dimension,'nmca':xrf_shape[-1]}
        #gattrs.update({'ndetectors':xrf_shape[-2]})


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

        roiscan = self.h5root['roi_scan']
        while self.last_row <  maxrow:
            irow = self.last_row
            dt = debugtime()
            self.last_row += 1
            print '>H5Writer.process row %i of %i, %s' % (self.last_row,
                                                          len(self.rowdata),
                                                          time.ctime())
            yval, xmapfile, struckfile, gatherfile, dtime = self.rowdata[irow]

            self.yvals.append(yval)
            shead,sdata = readASCII(os.path.join(self.folder,struckfile))
            ghead,gdata = readASCII(os.path.join(self.folder,gatherfile))
            dt.add(' xps, struck, row data')
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
            dt.add(' xmap data')
            xmdat = xmapdat.data[:]
            xm_ic = xmapdat.inputCounts[:]/(1.e-12+xmapdat.outputCounts[:])
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
                xm_ic = xm_ic[::-1]
                xmdat = xmdat[::-1]
                dt.add('reversed data ')

            xvals = [(gdata[i, self.ixaddr] + gdata[i-1, self.ixaddr])/2.0 for i in points]

            roiscan = self.h5root['roi_scan']
            pos     = self.h5root['positioners']
            xrf     = self.h5root['xrf_spectra']

            if irow == 0:
                det_addr = [i.strip() for i in shead[-2][1:].split('|')]
                det_desc = [i.strip() for i in shead[-1][1:].split('|')]
                sums_desc = self.det_desc[:]

                off, slope = self.calib['offset'], self.calib['slope']
                xnpts, nchan, nelem = xmdat.shape

                print 'Row 0 : ',  off, slope, snpts, nscalers, xnpts, nchan, nelem

                enx = [(off[i] + slope[i]*numpy.arange(nelem)) for i in range(nchan)]
                self.xrf_energies = numpy.array(enx, dtype=numpy.float32)

                for addr in self.roi_addr:
                    det_addr.extend([addr % (i+1) for i in range(nchan)])
                for desc in self.roi_desc:
                    det_desc.extend(["%s (mca%i)" % (desc, i+1) for i in range(nchan)])
                    sums_desc.append(desc)

                self.add_data(roiscan, 'det_addr',   det_addr)
                self.add_data(roiscan, 'det_desc',   det_desc)
                self.add_data(roiscan, 'sums_desc',  sums_desc)

                xrf_energies = xrf.create_dataset('energies', (nchan, nelem), numpy.float32,
                                                  compression=2)
                rtime = xrf.create_dataset('realtime', (2, xnpts, nchan), numpy.float32,
                                           maxshape=(None, xnpts, nchan), compression=2)
                ltime = xrf.create_dataset('livetime', (2, xnpts, nchan), numpy.float32,
                                           maxshape=(None, xnpts, nchan), compression=2)
                dtcorr = xrf.create_dataset('dt_factor', (2, xnpts, nchan), numpy.float32,
                                           maxshape=(None, xnpts, nchan), compression=2)
                xdata = xrf.create_dataset('data', (2, xnpts, nchan, nelem), xmdat.dtype,
                                           compression=2)
                dt.add('add row 0 ')
            else:
                rtime = xrf['realtime']
                if rtime.shape[0] <= irow:
                    ltime = xrf['livetime']
                    dtcorr = xrf['dt_factor']
                    xdata  = xrf['data']
                    d, xnpts, nchan, nelem = xdata.shape
                    rtime.resize((8*(1+irow/8), xnpts, nchan))
                    ltime.resize((8*(1+irow/8), xnpts, nchan))
                    dtcorr.resize((8*(1+irow/8), xnpts, nchan))
                    xdata.resize((8*(1+irow/8), xnpts, nchan, nelem))
            rtime[irow,:,:] = (xm_tr).astype('float32')
            ltime[irow,:,:] = (xm_tl).astype('float32')

            dtcorr[irow,:,:] = xm_ic.astype('float32')
            dt.add('add rtime, ltime, corr')
            xdata[irow,:,:,:] = xmdat
            dt.add('add data')
            dt.show()
            print sdata.shape
            rdat = list(sdata.transpose())
            raw, cor, sraw, scor = rdat, rdat[:], rdat[:], rdat[:]
            for slices in self.roi_slices:
                iraw = [xmdat[:, i, slices[i]].sum(axis=1)  for i in range(4)]
                icor = [xmdat[:, i, slices[i]].sum(axis=1) * xm_ic[:, i]
                        for i in range(4)]

                raw.extend(iraw)
                cor.extend(icor)
                sraw.extend(sum(iraw))
                scor.extend(sum(icor))

        # self.xrf_data = numpy.array(self.xrf_data)

        # self.det = numpy.array(self.det)
        #self.sums = numpy.array(self.sums)

        #self.det_corr = numpy.array(self.det_corr)
        #self.sums_corr = numpy.array(self.sums_corr)

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

