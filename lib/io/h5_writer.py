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

def isH5XRM(fname):
    "return whether fname is a valid HDF5 Map file"
    valid_h5xrm = False
    try:
        fh = h5py.File(fname)
        xrfmap = fh['/xrf_map']
        tmp = xrfmap.attrs['Version'], xrfmap.attrs['Beamline']
        tmp = xrfmap['config'], xrfmap['scan']
        valid_h5xrm = True
    except:
        pass
    finally:
        fh.close()
    return valid_h5xrm

def hasData(h5file, groupname='det1'):
    "return whether an XRM HDF5 filehandle has the named data group"
    return groupname in h5file['/xrf_map']

class H5Writer(object):
    """ Write HDF5 file from raw XRF map"""

    ScanFile   = 'Scan.ini'
    EnvFile    = 'Environ.dat'
    ROIFile    = 'ROI.dat'
    MasterFile = 'Master.dat'

    h5xrm_attrs = {'Version': '1.2.0',
                   'Title': 'Epics Scan Data',
                   'Beamline': 'GSECARS, 13-IDE / APS',
                   'Scan_Type': 'FastMap',
                   'Correct_Deadtime': 'True'}

    def __init__(self, folder=None, **kw):
        self.folder = folder
        self.master_header = None
        self.filename = None
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
        print 'ReadMaster!'
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
        cfile.Read(path.join(self.folder, self.ScanFile))
        self.mapconf = cfile.config

        self.filename = filename = self.mapconf['scan']['filename']

        mapconf = self.mapconf
        slow_pos = mapconf['slow_positioners']
        fast_pos = mapconf['fast_positioners']

        scanconf = mapconf['scan']
        self.dimension = scanconf['dimension']
   
        pos1 = scanconf['pos1']
        self.pos_addr = [pos1]
        self.pos_desc = [slow_pos[pos1]]
        self.ixaddr = -1
        for i, posname in enumerate(fast_pos):
            if posname == pos1:
                self.ixaddr = i
        if self.dimension > 1:
            yaddr = scanconf['pos2']
            self.pos_addr.append(yaddr)
            self.pos_desc.append(slow_pos[yaddr])
        print 'ReadMaster Done'
            
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
        kwargs = {'compression': 4}
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
        group = root['config']

        scantext = open(path.join(self.folder, self.ScanFile), 'r').read()
        group.create_dataset('scanfile', data=scantext)

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
        print 'This is begin h5 ', self.h5file, self.folder
        if self.h5file is None or self.folder is None:
            return
   
        attrs = {'Dimension':self.dimension,
                 'Stop_Time':self.stop_time,
                 'Start_Time':self.start_time,
                 'Map_Folder': self.folder,
                 'Process_Machine': '',
                 'Process_ID': 0}
        attrs.update(self.h5xrm_attrs)
        print '==begin: h5file =  ', self.h5file
        
        # root = self.add_group(self.h5file, 'xrf_map', attrs=attrs)
        root = self.h5file.create_group('xrf_map')
        for key, val in attrs.items():
            root.attrs[key] = val
        print '==begin: root = ', root
        self.add_group(root, 'scan')
        self.add_group(root, 'config')
        self.add_config(root, self.mapconf)          

        
    def open(self):
        if self.filename is None:
            self.read_master()
        print 'Open: Self.filename ', self.filename
        
        self.h5file = h5py.File("%s.h5" % self.filename, mode='w')
        print 'H5File = ', self.h5file
        
        if '/xrf_map' not in self.h5file:
            self.begin_h5file()
        
        self.h5root = self.h5file['/xrf_map']
        self.xrf_dets = [] 
        
    def close(self):
        self.h5file.close()

    def process(self, maxrow=None):
        print '=== HDF5 Writer: ', self.folder
        self.read_master()
        if self.h5file is None:
            print 'Process: Need to Open File: .....'
            self.open()
            print 'Process: File Opened'
        print 'PROCESS XXX '
        if len(self.rowdata) < 1:
            print ' === scan directory empty!'
            return

        if maxrow is None:
            maxrow = len(self.rowdata)
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
                    time.sleep(0.010)

            if atime < 0 or xmapdat is None:
                print 'Failed to read xmap data for row ', self.last_row
                return
            #
            dt.add('read xmap data')
            xmdat = xmapdat.data[:]
            xm_outcts = xmapdat.outputCounts[:]
            xm_dtfact = xmapdat.inputCounts[:]/(1.e-12+xmapdat.outputCounts[:])
            # times as integer microseconds
            xm_tlive = (1.e6*xmapdat.liveTime[:]).astype('int')
            xm_treal = (1.e6*xmapdat.realTime[:]).astype('int')

            gnpts, ngather  = gdata.shape
            snpts, nscalers = sdata.shape
            xnpts = xmdat.shape[0]
            npts = min(gnpts, xnpts)

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
            pform = "=Write Scan Data row=%i, npts=%i, folder=%s npts(xps, sis, xmap) =(%i, %i, %i)"
            #print pform % (irow, npts, self.folder,
            #               gdata.shape[0], sdata.shape[0], xmdat.shape[0])

            if xnpts != npts:
                xm_treal = xm_treal[:npts]
                xm_tlive = xm_tlive[:npts]
                xm_outcts = xm_outcts[:npts]
                xm_dtfact = xm_dtfact[:npts]
                xmdat = xmdat[:npts]

            points = range(1, npts+1)
            if irow % 2 != 0:
                points.reverse()
                xm_treal = xm_treal[::-1]
                xm_tlive = xm_tlive[::-1]
                xm_outcts = xm_outcts[::-1]
                xm_dtfact = xm_dtfact[::-1]
                xmdat = xmdat[::-1]
                dt.add('reversed data ')
            ix = self.ixaddr

            xvals = [(gdata[i, ix] + gdata[i-1, ix])/2.0 for i in points]

            posvals = [np.array(xvals)]
            if self.dimension == 2:
                posvals.append(np.array([float(yval) for i in points]))

            if irow == 0:
                self.npts = npts
                xnpts, nmca, nchan = xmdat.shape
                en_index = np.arange(nchan)
                off, slo, quad = (self.calib['offset'], self.calib['slope'],
                                  self.calib['quad'])
                roi_names = self.h5root['config/rois/name'][:]
                roi_addrs = self.h5root['config/rois/address'][:]
                roi_limits = self.h5root['config/rois/limits']
                for imca in range(nmca):
                    dname = 'det%i' % (imca+1)
                    self.add_group(self.h5root, dname)
                    self.xrf_dets.append(self.h5root[dname])
                    en = 1.0*off[imca] + slo[imca]*en_index
                    self.add_data(self.h5root[dname], 'energy', en,
                                  attrs={'cal_offset':off[imca],
                                         'cal_slope': slo[imca],
                                         'cal_quad': quad[imca]})
                    self.add_data(self.h5root[dname], 'roi_names', roi_names)
                    self.add_data(self.h5root[dname], 'roi_addrs', [s % (imca+1) for s in roi_addrs])
                    self.add_data(self.h5root[dname], 'roi_limits', roi_limits[:,imca,:])

                # 'virtual detector' for corrected sum:
                dname = 'detsum_corr'
                self.add_group(self.h5root, dname)
                en = 1.0*off[0] + slo[0]*en_index
                self.add_data(self.h5root[dname], 'energy', en,
                              attrs={'cal_offset':off[0],
                                     'cal_slope': slo[0],
                                     'cal_quad': quad[0]})
                self.add_data(self.h5root[dname], 'roi_names', roi_names)
                self.add_data(self.h5root[dname], 'roi_addrs', [s % 1 for s in roi_addrs])
                self.add_data(self.h5root[dname], 'roi_limits', roi_limits[:,0,:])

                # scan
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
            else: # Not Row 0
                rtime = self.xrf_dets[0]['realtime']
                if rtime.shape[0] <= irow:
                    nrow = 64*(1+irow/64)
                    self.resize_arrays(nrow)
                    dt.add('resize data')

            scan = self.h5root['scan']
            pos    = scan['pos']
            det_raw = scan['det_raw']
            det_cor = scan['det_dtcorr']
            sum_raw = scan['sum_raw']
            sum_cor = scan['sum_dtcorr']

            #print 'xmap data shape: ', xm_treal.shape, xmdat.shape
            total = None
            for ixrf, xrf in enumerate(self.xrf_dets):
                dtcorr = xm_dtfact[:,ixrf].astype('float32')
                corr = dtcorr.reshape((dtcorr.shape[0], 1))
                xrf['dtfactor'][irow, :]  = dtcorr
                xrf['realtime'][irow, :]  = xm_treal[:,ixrf]
                xrf['livetime'][irow, :]  = xm_tlive[:,ixrf]
                xrf['outcounts'][irow, :] = xm_outcts[:, ixrf]
                xrf['data'][irow, :, :]   = xmdat[:,ixrf,:]
                if total is None:
                    total = xmdat[:,ixrf,:] * corr
                else:
                    total = total + xmdat[:,ixrf,:] * corr

            # here, we add the total dead-time-corrected data to detsum.
            self.h5root['detsum_corr']['data'][irow, :] = total.astype('int')

            dt.add('add xrf data')
            draw = list(sdata[:npts].transpose())
            # print "======== NPTS ", sdata.shape, xmdat.shape, npts, nmca
            dcor = draw[:]
            sraw = draw[:]
            scor = draw[:]

            for slices in self.roi_slices:
                iraw = [xmdat[:, i, slices[i]].sum(axis=1)
                        for i in range(nmca)]
                icor = [xmdat[:, i, slices[i]].sum(axis=1)*xm_dtfact[:, i]
                        for i in range(nmca)]
                draw.extend(iraw)
                dcor.extend(icor)
                sraw.append(np.array(iraw).sum(axis=0))
                scor.append(np.array(icor).sum(axis=0))
            det_raw[irow, :, :] = np.array(draw).transpose()
            det_cor[irow, :, :] = np.array(dcor).transpose()
            sum_raw[irow, :, :] = np.array(sraw).transpose()
            sum_cor[irow, :, :] = np.array(scor).transpose()
            posvals.append(xm_treal.sum(axis=1).astype('float32') / nmca)
            posvals.append(xm_tlive.sum(axis=1).astype('float32') / nmca)

            pos[irow, :, :] = np.array(posvals).transpose()
            dt.add('add det/pos data')
            #dt.show()
        try:
            self.resize_arrays()
            dt.add('end of final resize')
        except:
            pass
        #dt.show()

    def resize_arrays(self, nrow=None):
        "resize all arrays for nrows"
        if nrow is None:
            nrow = self.last_row
        # xrf  = self.h5root['xrf_spectra']

        old, npts, nchan = self.xrf_dets[0]['data'].shape
        for xrf in self.xrf_dets:
            for aname in ('livetime', 'realtime', 'outcounts', 'dtfactor'):
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

        s   = self.h5root['detsum_corr']
        raw = s['data']
        old, nx, ny = raw.shape
        raw.resize((nrow, nx, ny))
        # print 'Resized to ', nrow

    def create_arrays(self, npts, npos, nsca, nsum, nmca, nchan):
        scan = self.h5root['scan']
        NINIT = 16
        COMP = 4
        #print 'Create Arrays  ! XRF SPECTRA ', NINIT
        scan.create_dataset('det_raw', (NINIT, npts, nsca),
                            np.int32, compression=COMP,
                            maxshape=(None, npts, nsca))

        scan.create_dataset('det_dtcorr', (NINIT, npts, nsca),
                            np.float32, compression=COMP,
                            maxshape=(None, npts, nsca))

        scan.create_dataset('sum_raw', (NINIT, npts, nsum),
                            np.int32, compression=COMP,
                            maxshape=(None, npts, nsum))

        scan.create_dataset('sum_dtcorr', (NINIT, npts, nsum),
                            np.float32, compression=COMP,
                            maxshape=(None, npts, nsum))

        scan.create_dataset('pos', (NINIT, npts, npos),
                            np.float32, compression=COMP,
                            maxshape=(None, npts, npos))

        detsum = self.h5root['detsum_corr']
        detsum.create_dataset('data', (NINIT, npts, nchan),
                              np.int16, compression=COMP,
                              maxshape=(None, npts, nchan))


        for xrf in self.xrf_dets:
            xrf.create_dataset('realtime', (NINIT, npts),
                               np.int, compression=COMP,
                               maxshape=(None, npts))

            xrf.create_dataset('livetime', (NINIT, npts),
                               np.int, compression=COMP,
                               maxshape=(None, npts))

            xrf.create_dataset('dtfactor', (NINIT, npts),
                               np.float32, compression=COMP,
                               maxshape=(None, npts))

            xrf.create_dataset('outcounts', (NINIT, npts),
                               np.float32, compression=COMP,
                               maxshape=(None, npts))

            xrf.create_dataset('data', (NINIT, npts, nchan),
                               np.int16, compression=COMP,
                               maxshape=(None, npts, nchan))




if __name__ == '__main__':
    dirname = '../SRM1833_map_001'
    ms = H5Writer(folder=dirname)
    ms.process()


