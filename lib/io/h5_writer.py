#!/usr/bin/env python
"""
HDF5 File writer
"""
import os
from os import path
import sys
import time
import numpy as np
import h5py

from xmap_nc import read_xmap_netcdf
from ..utils import debugtime
from ..config import FastMapConfig

from file_utils import nativepath
from mapfolder import (readASCII, readMasterFile,
                       readEnvironFile, parseEnviron,
                       readROIFile)

class H5Writer(object):
    """ Write HDF5 file from raw XRF map"""

    ScanFile   = 'Scan.ini'
    EnvFile    = 'Environ.dat'
    ROIFile    = 'ROI.dat'
    MasterFile = 'Master.dat'

    h5_attrs = {'Version': '1.2.0',
                'Title': 'Epics Scan Data',
                'Beamline': 'GSECARS, 13-IDE / APS',
                'Scan_Type': 'Fast Map',
                'Correct_Deadtime': 'True'}

    def __init__(self, folder=None, **kw):
        self.folder = folder
        self.master_header = None

        self.h5file = None
        self.h5root = None
        self.pos_addr = None
        self.ixaddr = 0
        self.last_row = 0
        self.start_time = 0
        self.stop_time = 0
        self.calib = None
        self.rowdata = None
        self.pos_desc = None
        self.roi_slices = []
        self.roi_desc = []
        self.roi_addr = []
        self.npts = -1

    def read_master(self):
        "reads master file for toplevel scan info"
        self.rowdata = None
        self.master_header = None

        if self.folder is None:
            return
        fname = path.join(nativepath(self.folder), self.MasterFile)
        self.stop_time = time.ctime(os.stat(fname).st_mtime)
        if path.exists(fname):
            try:
                header, rows = readMasterFile(fname)
            except IOError:
                print 'Cannot read Scan folder'
                return
            self.master_header = header
            self.rowdata = rows
            stime = self.master_header[0][6:]

            self.start_time = stime.replace('started at','').strip()

        cfile = FastMapConfig()
        cfile.Read(path.join(self.folder, 'Scan.ini'))
        self.mapconf = cfile.config

        self.filename = self.mapconf['scan']['filename']

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

    def add_config(self, root, config):
        "add ROI, DXP Settings, and Config data"
        group = self.add_group(root, 'config')

        for name, sect in (('scan', 'scan'),
                           ('general', 'general'),
                           ('positioners', 'slow_positioners'),
                           ('motor_controller', 'xps')):
            grp = self.add_group(group, name)

            for key, val in config[sect].items():
                grp.create_dataset(key, data=val)

        roidat, calib, dxp = readROIFile(path.join(self.folder, self.ROIFile))
        roi_desc, roi_addr, roi_lim = [], [], []
        roi_slices = []
        for iroi, label, lims in roidat:
            roi_desc.append(label)
            roi_addr.append("%smca%%i.R%i" % (config['general']['xmap'], iroi))
            roi_lim.append([lims[i] for i in range(4)])
            roi_slices.append([slice(lims[i][0], lims[i][1]) for i in range(4)])
        roi_lim = np.array(roi_lim)

        grp = self.add_group(group, 'rois')

        self.add_data(grp, 'name',     roi_desc)
        self.add_data(grp, 'address',  roi_addr)
        self.add_data(grp, 'limits',   roi_lim)

        grp = self.add_group(group, 'mca_calib')
        for key, val in calib.items():
            self.add_data(grp, key, val)

        grp = self.add_group(group, 'mca_settings')
        for key, val in dxp.items():
            self.add_data(grp, key, val)

        self.roi_desc = roi_desc
        self.roi_addr = roi_addr
        self.roi_slices = roi_slices
        self.calib = calib

        # add env data
        envdat = readEnvironFile(path.join(self.folder, self.EnvFile))
        env_desc, env_addr, env_val = parseEnviron(envdat)
        grp = self.add_group(group, 'environ')
        self.add_data(grp, 'name',     env_desc)
        self.add_data(grp, 'address',  env_addr)
        self.add_data(grp, 'value',     env_val)

    def begin_h5file(self):
        """open and start writing to h5file:
        important: only run this once!"""
        if self.h5file is not None or self.folder is None:
            return

        mapconf = self.mapconf
        slow_pos = mapconf['slow_positioners']
        fast_pos = mapconf['fast_positioners']

        scanconf = mapconf['scan']
        dimension = scanconf['dimension']
        filename = scanconf['filename']

        pos1 = scanconf['pos1']
        self.pos_addr = [pos1]
        self.pos_desc = [slow_pos[pos1]]
        self.ixaddr = -1
        for i, posname in enumerate(fast_pos):
            if posname == pos1:
                self.ixaddr = i
        if dimension > 1:
            yaddr = scanconf['pos2']
            self.pos_addr.append(yaddr)
            self.pos_desc.append(slow_pos[yaddr])

        self.dimension = dimension
        #
        self.h5file = h5py.File(filename+'.h5', 'w')

        attrs = {'Dimension':dimension,
                 'Stop_Time':self.stop_time,
                 'Start_Time':self.start_time}
        attrs.update(self.h5_attrs)

        self.h5root = self.add_group(self.h5file,
                                     'xrf_map', attrs=attrs)

        self.add_config(self.h5root, mapconf)
        self.add_group(self.h5root, 'scan')
        self.xrf_dets = []
        print ' begin h5 file done'

    def process(self, maxrow=None):
        print '=== HDF5 Writer: ', self.folder
        self.read_master()
        if len(self.rowdata) < 1:
            print ' === scan directory empty!'
            return
        if self.last_row == 0 and len(self.rowdata)>0:
            self.begin_h5file()

        if maxrow is None:
            maxrow = len(self.rowdata)
        print 'process maxrow = ', maxrow
        roiscan = self.h5root['scan']
        dt = debugtime()
        while self.last_row <  maxrow:
            irow = self.last_row
            self.last_row += 1
            print '>H5Writer.process row %i of %i, %s' % (self.last_row,
                                                          len(self.rowdata),
                                                          time.ctime())
            yval, xmapfile, struckfile, gatherfile, etime = self.rowdata[irow]

            shead, sdata = readASCII(path.join(self.folder, struckfile))
            ghead, gdata = readASCII(path.join(self.folder, gatherfile))
            dt.add('read xps, struck, row data')
            t0 = time.time()
            atime = -1
            xmapdat = None
            while atime < 0 and time.time()-t0 < 10:
                try:
                    atime = time.ctime(os.stat(path.join(self.folder,
                                                         xmapfile)).st_ctime)
                    xmfile = path.join(self.folder, xmapfile)
                    xmapdat = read_xmap_netcdf(xmfile, verbose=False)
                except (IOError, IndexError):
                    time.sleep(0.10)
            if atime < 0 or xmapdat is None:
                print 'Failed to read xmap data for row ', self.last_row
                return
            #
            dt.add('read xmap data')
            xmdat = xmapdat.data[1:]
            xm_ic = xmapdat.inputCounts[1:]/(1.e-12+xmapdat.outputCounts[1:])
            # times as integer microseconds
            xm_tl = (1.e6*xmapdat.liveTime[1:]).astype('int')
            xm_tr = (1.e6*xmapdat.realTime[1:]).astype('int')

            gnpts = gdata.shape[0]
            snpts = sdata.shape[0]
            xnpts = xmdat.shape[0]
            npts = min(snpts, gnpts, xnpts)
            # ok this is a hack -- try to recover from missing
            # struck data.
            if irow  > 0 and npts != self.npts:
                if (snpts > self.npts/2.) and (snpts < self.npts):
                    sdata = list(sdata)
                    for i in range(self.npts+1-snpts):
                        sdata.append(sdata[snpts-1])
                    sdata = np.array(sdata)
                snpts = sdata.shape[0]
                npts = min(snpts, gnpts, xnpts)
                if npts != self.npts:
                    print 'not enough data at row ', irow
                    break
            if npts < 2:
                print 'not enough data at row ', irow
                break

            if xnpts != npts:
                xm_tr = xm_tr[:npts]
                xm_tl = xm_tl[:npts]
                xm_ic = xm_ic[:npts]
                xmdat = xmdat[:npts]

            points = range(1, npts+1)
            if irow % 2 != 0:
                points.reverse()
                xm_tr = xm_tr[::-1]
                xm_tl = xm_tl[::-1]
                xm_ic = xm_ic[::-1]
                xmdat = xmdat[::-1]
                #dt.add('reversed data ')
            ix = self.ixaddr

            xvals = [(gdata[i, ix] + gdata[i-1, ix])/2.0 for i in points]

            posvals = [np.array(xvals)]
            if self.dimension == 2:
                posvals.append(np.array([float(yval) for i in points]))

            if irow == 0:
                self.npts = npts
                n, ndet, nchans = xmdat.shape
                xnpts, nmca, nchan = xmdat.shape
                en_index = np.arange(nchan)
                off, slo = self.calib['offset'], self.calib['slope']

                for imca in range(nmca):
                    dname = 'det%i' % (imca+1)
                    self.add_group(self.h5root, dname)
                    self.xrf_dets.append(self.h5root[dname])
                    en = 1.0*off[imca] + slo[imca]*en_index
                    self.add_data(self.h5root[dname], 'energy', en)

                scan = self.h5root['scan']
                det_addr = [i.strip() for i in shead[-2][1:].split('|')]
                det_desc = [i.strip() for i in shead[-1][1:].split('|')]

                for addr in self.roi_addr:
                    det_addr.extend([addr % (i+1) for i in range(nmca)])
                for desc in self.roi_desc:
                    det_desc.extend(["%s (mca%i)" % (desc, i+1)
                                     for i in range(nmca)])

                sums_map = {}
                sums_desc = []
                nsum = 0
                for idet, addr in enumerate(det_desc):
                    if '(mca' in addr:
                        addr = addr.split('(mca')[0].strip()

                    if addr not in sums_map:
                        sums_map[addr] = []
                        sums_desc.append(addr)
                    sums_map[addr].append(idet)
                nsum = max([len(s) for s in sums_map.values()])
                sums_list = []
                for sname in sums_desc:
                    slist = sums_map[sname]
                    if len(slist) < nsum:
                        slist.extend([-1]*(nsum-len(slist)))
                    sums_list.append(slist)

                nsum = len(sums_list)
                sums_list = np.array(sums_list)
                self.add_data(scan, 'det_name',    det_desc)
                self.add_data(scan, 'det_address', det_addr)
                self.add_data(scan, 'sum_name',    sums_desc)
                self.add_data(scan, 'sum_list',    sums_list)

                nsca = len(det_desc)
                for pname in ('MCA Real Time', 'MCA Live Time'):
                    self.pos_desc.append(pname)
                    self.pos_addr.append('')
                npos = len(self.pos_desc)
                self.add_data(scan, 'pos_name',     self.pos_desc)
                self.add_data(scan, 'pos_address',  self.pos_addr)

                self.create_arrays(npts, npos, nsca, nsum, nmca, nchan)

                dt.add('add row 0 ')
            else:
                rtime = self.xrf_dets[0]['realtime']
                if rtime.shape[0] <= irow:
                    nrow = 8*(1+irow/8)
                    self.resize_arrays(nrow)
                    dt.add('resize data')

            scan = self.h5root['scan']
            pos    = scan['pos']
            det_raw = scan['det_raw']
            det_cor = scan['det_dtcorr']
            sum_raw = scan['sum_raw']
            sum_cor = scan['sum_dtcorr']

            #print 'DET 0 realtime : ', self.xrf_dets[0]['realtime']
            #print 'xmap data shape: ', xm_tr.shape, xmdat.shape
            for ixrf, xrf in enumerate(self.xrf_dets):
                xrf['realtime'][irow, :] = xm_tr[:,ixrf]
                xrf['livetime'][irow, :] = xm_tl[:,ixrf]
                xrf['dt_factor'][irow, :] = xm_ic[:,ixrf].astype('float32')
                # dt.add('add rtime, ltime, corr')
                xrf['data'][irow, :, :] = xmdat[:,ixrf,:]
            dt.add('add xrf data')
            draw = list(sdata[:npts].transpose())
            # print "======== NPTS ", sdata.shape, xmdat.shape, npts, nmca
            dcor = draw[:]
            sraw = draw[:]
            scor = draw[:]

            for slices in self.roi_slices:
                iraw = [xmdat[:, i, slices[i]].sum(axis=1)
                        for i in range(nmca)]
                icor = [xmdat[:, i, slices[i]].sum(axis=1)*xm_ic[:, i]
                        for i in range(nmca)]
                draw.extend(iraw)
                dcor.extend(icor)
                sraw.append(np.array(iraw).sum(axis=0))
                scor.append(np.array(icor).sum(axis=0))
            # dt.add('made det data')

            det_raw[irow, :, :] = np.array(draw).transpose()
            det_cor[irow, :, :] = np.array(dcor).transpose()
            sum_raw[irow, :, :] = np.array(sraw).transpose()
            sum_cor[irow, :, :] = np.array(scor).transpose()
            #dt.add('add det data')

            posvals.append(xm_tr.sum(axis=1).astype('float32') / nmca)
            posvals.append(xm_tl.sum(axis=1).astype('float32') / nmca)

            pos[irow, :, :] = np.array(posvals).transpose()
            dt.add('add positioners')
        try:
            self.resize_arrays()
            dt.add('end of final resize')
        except:
            pass
        dt.show()

    def resize_arrays(self, nrow=None):
        "resize all arrays for nrows"
        if nrow is None:
            nrow = self.last_row
        print 'RESIZE Arrays ', nrow
        # xrf  = self.h5root['xrf_spectra']

        old, npts, nchan = self.xrf_dets[0]['data'].shape
        for xrf in self.xrf_dets:
            for aname in ('livetime', 'realtime', 'dt_factor'):
                xrf[aname].resize((nrow, npts))
            xrf['data'].resize((nrow, npts, nchan))

        scan = self.h5root['scan']
        pos     = scan['pos']
        det_raw = scan['det_raw']
        det_cor = scan['det_dtcorr']
        sum_raw = scan['sum_raw']
        sum_cor = scan['sum_dtcorr']

        old, npts, npos = pos.shape
        old, npts, nsca = det_raw.shape
        old, npts, nsum = sum_raw.shape

        pos.resize((nrow, npts, npos))
        det_raw.resize((nrow, npts, nsca))
        det_cor.resize((nrow, npts, nsca))
        sum_raw.resize((nrow, npts, nsum))
        sum_cor.resize((nrow, npts, nsum))

    def create_arrays(self, npts, npos, nsca, nsum, nmca, nchan):
        scan = self.h5root['scan']
        NINIT = 4
        print 'Create Arrays  ! XRF SPECTRA ', NINIT
        scan.create_dataset('det_raw', (NINIT, npts, nsca),
                            np.int32, compression=2,
                            maxshape=(None, npts, nsca))

        scan.create_dataset('det_dtcorr', (NINIT, npts, nsca),
                            np.float32, compression=2,
                            maxshape=(None, npts, nsca))

        scan.create_dataset('sum_raw', (NINIT, npts, nsum),
                            np.int32, compression=2,
                            maxshape=(None, npts, nsum))

        scan.create_dataset('sum_dtcorr', (NINIT, npts, nsum),
                            np.float32, compression=2,
                            maxshape=(None, npts, nsum))

        scan.create_dataset('pos', (NINIT, npts, npos),
                            np.float32, compression=2,
                            maxshape=(None, npts, npos))
        for xrf in self.xrf_dets:
            xrf.create_dataset('realtime', (NINIT, npts),
                               np.int, compression=2,
                               maxshape=(None, npts))

            xrf.create_dataset('livetime', (NINIT, npts),
                               np.int, compression=2,
                               maxshape=(None, npts))

            xrf.create_dataset('dt_factor', (NINIT, npts),
                               np.float32, compression=2,
                               maxshape=(None, npts))

            xrf.create_dataset('data', (NINIT, npts, nchan),
                               np.int16, compression=2,
                               maxshape=(None, npts, nchan))




if __name__ == '__main__':
    dirname = '../SRM1833_map_001'
    ms = H5Writer(folder=dirname)
    ms.process()


