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
                       readROIFile)

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

        self.roi_desc  = []
        self.roi_addr  = []


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

        roidata, calib, dxp = readROIFile(os.path.join(self.folder, self.ROIFile))
        roi_desc, roi_addr, roi_lim = [], [], []
        roi_slices = []
        for iroi, label, roidat in roidata:
            roi_desc.append(label)
            roi_addr.append("%smca%%i.R%i" % (config['general']['xmap'], iroi))
            roi_lim.append([roidat[i] for i in range(4)])
            roi_slices.append([slice(roidat[i][0], roidat[i][1]) for i in range(4)])
        roi_lim = numpy.array(roi_lim)

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
        envdat = readEnvironFile(os.path.join(self.folder, self.EnvFile))
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

        cfile = FastMapConfig()
        cfile.Read(os.path.join(self.folder, 'Scan.ini'))

        mapconf = cfile.config
        slow_pos = mapconf['slow_positioners']
        fast_pos = mapconf['fast_positioners']
        scanconf = mapconf['scan']
        dimension = scanconf['dimension']

        user_titles = scanconf['comments'].split('\n')
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

        #
        h5name = filename + '.h5'
        fh = self.h5file = h5py.File(h5name, 'w')

        attrs = {'Dimension':dimension,
                 'Stop_Time':self.stop_time,
                 'Start_Time':self.start_time}
        attrs.update(self.h5_attrs)

        h5root = self.h5root = self.add_group(fh, 'xrf_map', attrs=attrs)
        self.add_config(h5root, mapconf)

        self.add_group(h5root, 'scan')
        self.add_group(h5root, 'xrf_spectra')


    def process(self, maxrow=None):
        print '=== HDF5 Writer: ', self.folder
        self.ReadMaster()

        if self.last_row == 0 and len(self.rowdata)>0:
            self.begin_h5file()

        if maxrow is None:
            maxrow = len(self.rowdata)

        roiscan = self.h5root['scan']
        while self.last_row <  maxrow:
            irow = self.last_row
            dt = debugtime()
            self.last_row += 1
            print '>H5Writer.process row %i of %i, %s' % (self.last_row,
                                                          len(self.rowdata),
                                                          time.ctime())
            yval, xmapfile, struckfile, gatherfile, dtime = self.rowdata[irow]

            shead,sdata = readASCII(os.path.join(self.folder,struckfile))
            ghead,gdata = readASCII(os.path.join(self.folder,gatherfile))
            dt.add('read xps, struck, row data')
            t0 = time.time()
            atime = -1
            while atime < 0 and time.time()-t0 < 10:
                try:
                    atime = time.ctime(os.stat(os.path.join(self.folder,
                                                            xmapfile)).st_ctime)
                    xmfile = os.path.join(self.folder,xmapfile)
                    xmapdat = read_xmap_netcdf(xmfile, verbose=False)
                except:
                    print 'xmap data failed to read'
                    sys.exit()
                    # self.clear()
                    atime = -1
                time.sleep(0.03)
            if atime < 0:
                return 0
            #
            dt.add('read xmap data')
            xmdat = xmapdat.data[1:]
            xm_ic = xmapdat.inputCounts[1:]/(1.e-12+xmapdat.outputCounts[1:])
            # times as integer microseconds
            xm_tl = (1.e6*xmapdat.liveTime[1:]).astype('int')
            xm_tr = (1.e6*xmapdat.realTime[1:]).astype('int')

            gnpts, ngather  = gdata.shape
            snpts, nscalers = sdata.shape

            xnpts = xmdat.shape[0]
            npts = min(snpts,gnpts,xnpts)
            npts = gnpts-1

            points = range(1, npts+1)
            if irow % 2 != 0:
                points.reverse()
                xm_tr = xm_tr[::-1]
                xm_tl = xm_tl[::-1]
                xm_ic = xm_ic[::-1]
                xmdat = xmdat[::-1]
                dt.add('reversed data ')
            ix = self.ixaddr
            posvals = [numpy.array([(gdata[i, ix] + gdata[i-1, ix])/2.0 for i in points]),
                       numpy.array([float(yval) for i in points])]

            scan = self.h5root['scan']
            xrf  = self.h5root['xrf_spectra']

            if irow == 0:
                det_addr = [i.strip() for i in shead[-2][1:].split('|')]
                det_desc = [i.strip() for i in shead[-1][1:].split('|')]

                off, slope = self.calib['offset'], self.calib['slope']
                xnpts, nchan, nelem = xmdat.shape

                # print 'Row 0 : ',  off, slope, snpts, nscalers, xnpts, nchan, nelem

                enx = [(off[i] + slope[i]*numpy.arange(nelem)) for i in range(nchan)]
                self.add_data(xrf, 'energies', numpy.array(enx, dtype=numpy.float32))
                for addr in self.roi_addr:
                    det_addr.extend([addr % (i+1) for i in range(nchan)])
                for desc in self.roi_desc:
                    det_desc.extend(["%s (mca%i)" % (desc, i+1) for i in range(nchan)])

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
                sums_list = numpy.array(sums_list)
                self.add_data(scan, 'det_name',    det_desc)
                self.add_data(scan, 'det_address', det_addr)
                self.add_data(scan, 'sum_name',    sums_desc)
                self.add_data(scan, 'sum_list',    sums_list)

                ndet = len(det_desc)
                for pname in ('MCA Real Time', 'MCA Live Time'):
                    self.pos_desc.append(pname)
                    self.pos_addr.append('')
                npos = len(self.pos_desc)
                self.add_data(scan, 'pos_name',     self.pos_desc)
                self.add_data(scan, 'pos_address',  self.pos_addr)

                det_raw = scan.create_dataset('det_raw', (2, npts, ndet), numpy.long,
                                              compression=2)

                det_cor = scan.create_dataset('det_dtcorr', (2, npts, ndet), numpy.float32,
                                              compression=2)
                sum_raw = scan.create_dataset('sum_raw', (2, npts, nsum), numpy.long,
                                              compression=2)
                sum_cor = scan.create_dataset('sum_dtcorr', (2, npts, nsum), numpy.float32,
                                              compression=2)

                pos = scan.create_dataset('pos', (2, npts, npos), numpy.float32,
                                            compression=2)

                rtime = xrf.create_dataset('realtime', (2, npts, nchan), numpy.int,
                                           maxshape=(None, npts, nchan), compression=2)
                ltime = xrf.create_dataset('livetime', (2, npts, nchan), numpy.int,
                                           maxshape=(None, npts, nchan), compression=2)
                dtcorr = xrf.create_dataset('dt_factor', (2, npts, nchan), numpy.float32,
                                           maxshape=(None, npts, nchan), compression=2)
                xdata = xrf.create_dataset('data', (2, npts, nchan, nelem), xmdat.dtype,
                                           compression=2)
                dt.add('add row 0 ')
            else:
                rtime = xrf['realtime']
                if rtime.shape[0] <= irow:
                    ltime = xrf['livetime']
                    dtcorr = xrf['dt_factor']
                    xdata  = xrf['data']
                    d, npts, nchan, nelem = xdata.shape
                    nrow = 8*(1+irow/8)
                    rtime.resize((nrow, npts, nchan))
                    ltime.resize((nrow, npts, nchan))
                    dtcorr.resize((nrow, npts, nchan))
                    xdata.resize((nrow, npts, nchan, nelem))

                    pos = scan['pos']
                    pos.resize((nrow, npts, npos))

                    det_raw = scan['det_raw']
                    ndet = det_raw.shape[2]
                    det_raw.resize((nrow, npts, ndet))
                    det_cor = scan['det_dtcorr']
                    det_cor.resize((nrow, npts, ndet))

                    sum_raw = scan['sum_raw']
                    nsum = sum_raw.shape[2]
                    sum_raw.resize((nrow, npts, nsum))
                    sum_cor = scan['sum_dtcorr']
                    sum_cor.resize((nrow, npts, nsum))


            rtime[irow,:,:] = xm_tr
            ltime[irow,:,:] = xm_tl

            dtcorr[irow,:,:] = xm_ic.astype('float32')
            dt.add('add rtime, ltime, corr')
            xdata[irow,:,:,:] = xmdat
            dt.add('add xrf data')
            draw = list(sdata.transpose())
            dcor = draw[:]
            sraw = draw[:]
            scor = draw[:]
            for slices in self.roi_slices:
                iraw = [xmdat[:, i, slices[i]].sum(axis=1) for i in range(nchan)]
                icor = [xmdat[:, i, slices[i]].sum(axis=1)*xm_ic[:, i]  for i in range(nchan)]
                draw.extend(iraw)
                dcor.extend(icor)
                sraw.append(numpy.array(iraw).sum(axis=0))
                scor.append(numpy.array(icor).sum(axis=0))

            det_raw[irow,:,:] = numpy.array(draw).transpose()
            det_cor[irow,:,:] = numpy.array(dcor).transpose()
            sum_raw[irow,:,:] = numpy.array(sraw).transpose()
            sum_cor[irow,:,:] = numpy.array(scor).transpose()
            dt.add('add det data')

            posvals.append(xm_tr.sum(axis=1).astype('float32') / nchan)
            posvals.append(xm_tl.sum(axis=1).astype('float32') / nchan)
            pos[irow,:,:] = numpy.array(posvals).transpose()
            dt.add('add positioners')
            #dt.show()


if __name__ == '__main__':
    dirname = '../SRM1833_map_001'
    ms = H5Writer(folder=dirname)
    ms.process()

