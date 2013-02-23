import os
import socket
import time
import h5py
import numpy as np

from ..utils import debugtime
from ..config import FastMapConfig

from .xmap_nc import read_xmap_netcdf
from .mapfolder import (readASCII, readMasterFile,
                        readEnvironFile, parseEnviron,
                        readROIFile)

from .file_utils import nativepath

class GSEXRM_FileStatus:
    no_xrfmap    = 'hdf5 does not have /xrfmap'     # HDF5 created, no xrfmap group
    created      = 'hdf5 has empty schema'  # HDF5 started, xrfmap exists, no data
    hasdata      = 'hdf5 has map data' # 1+ row written: array sizes known
    err_notfound = 'file not found'
    err_nothdf5  = 'file is not hdf5'

def getFileStatus(filename):
    # see if file exists:
    if (not os.path.exists(filename) or
        not os.path.isfile(filename) ):
        return GSEXRM_FileStatus.err_notfound

    # see if file is an H5 file
    try:
        fh = h5py.File(filename)
    except IOError:
        return GSEXRM_FileStatus.err_nothdf5
    if 'xrfmap' not in fh:
        return GSEXRM_FileStatus.no_xrfmap

    if 'det1' in fh['/xrfmap']:
        return GSEXRM_FileStatus.hasdata
    fh.close()
    return GSEXRM_FileStatus.created

def isGSEXRM_MapFolder(fname):
    "return whether folder a valid Scan Folder (raw data)"
    if not os.path.isdir(fname):
        return False
    flist = os.listdir(fname)
    for f in ('Master.dat', 'Environ.dat', 'Scan.ini', 'xmap.0001'):
        if f not in flist:
            return False
    return True

H5ATTRS = {'Version': '1.3.0',
           'Title': 'Epics Scan Data',
           'Beamline': 'GSECARS, 13-IDE / APS',
           'Start_Time':'',
           'Stop_Time':'',
           'Dimension': 2,
           'Process_Machine':'',
           'Process_ID': 0}

def create_xrfmap(h5root, dimension=2, folder='', start_time=None):
    """creates a skeleton '/xrfmap' group in an open HDF5 file

    This is left as a function, not method of GSEXRM_MapFile below
    because it may be called by the mapping collection program
    (ie, from collector.py) when a map is started

    This leaves a structure to be filled in by
    GSEXRM_MapFile.init_xrfmap(),
    """
    attrs = {}
    attrs.update(H5ATTRS)
    if start_time is None:
        start_time = time.ctime()
    attrs.update({'Dimension':dimension, 'Start_Time':start_time,
                  'Map_Folder': folder, 'Last_Row': -1})

    xrfmap = h5root.create_group('xrfmap')
    for key, val in attrs.items():
        xrfmap.attrs[key] = val

    g = xrfmap.create_group('roimap')
    g.attrs['type'] = 'roi maps'
    g.attrs['desc'] = 'ROI data, including summed and deadtime corrected maps'

    xrfmap.create_group('config')
    g.attrs['type'] = 'scan config'
    g.attrs['desc'] = '''scan configuration, including scan definitions,
    ROI definitions, MCA calibration, Environment Data, etc'''

    conf = xrfmap['config']
    for name in ('scan', 'general', 'environ', 'positioners',
                 'motor_controller', 'rois', 'mca_settings', 'mca_calib'):
        conf.create_group(name)
    h5root.flush()

class GSEXRM_Exception(Exception):
    """GSEXRM Exception: General Errors"""
    def __init__(self, msg):
        Exception.__init__(self)
        self.msg = msg
    def __str__(self):
        return self.msg

class GSEXRM_NotOwner(Exception):
    """GSEXRM Not Owner Host/Process ID"""
    def __init__(self, msg):
        Exception.__init__(self)
        self.msg = 'Not Owner of HDF5 file %s' % msg
    def __str__(self):
        return self.msg

class GSEXRM_MapRow:
    """
    read one row worth of data:
    """
    def __init__(self, yvalue, xmapfile, xpsfile, sisfile, folder,
                 reverse=False, ixaddr=0, dimension=2, npts=None):

        self.npts = npts
        shead, sdata = readASCII(os.path.join(folder, sisfile))
        ghead, gdata = readASCII(os.path.join(folder, xpsfile))
        self.sishead = shead

        t0 = time.time()
        atime = -1
        xmapdat = None
        xmfile = os.path.join(folder, xmapfile)
        while atime < 0 and time.time()-t0 < 10:
            try:
                atime = os.stat(xmfile).st_ctime
                xmapdat = read_xmap_netcdf(xmfile, verbose=False)
            except (IOError, IndexError):
                time.sleep(0.010)

        if atime < 0 or xmapdat is None:
            print 'Failed to read xmap data from %s' % self.xmapfile
            return
        #
        self.spectra   = xmapdat.data[:]
        self.inpcounts = xmapdat.inputCounts[:]
        self.outcounts = xmapdat.outputCounts[:]
        self.dtfactor  = xmapdat.inputCounts[:]/(1.e-12+xmapdat.outputCounts[:])
        # times are extracted from the netcdf file as floats of microseconds
        # here we truncate to nearest microsecond (clock tick is 0.32 microseconds)
        self.livetime  = (xmapdat.liveTime[:]).astype('int')
        self.realtime  = (xmapdat.realTime[:]).astype('int')

        gnpts, ngather  = gdata.shape
        snpts, nscalers = sdata.shape
        xnpts, nmca, nchan = self.spectra.shape
        npts = min(gnpts, xnpts)
        if self.npts is None:
            self.npts = npts
        if snpts < self.npts:  # extend struck data if needed
            sdata = list(sdata)
            for i in range(self.npts+1-snpts):
                sdata.append(sdata[snpts-1])
            sdata = np.array(sdata)
            snpts = self.npts
        self.sisdata = sdata

        if xnpts != npts:
            self.spectra  = self.spectra[:npts]
            self.realtime = self.realtime[:npts]
            self.livetime = self.livetime[:npts]
            self.dtfactor = self.dtfactor[:npts]
            self.inpcounts= self.inpcounts[:npts]
            self.outcounts= self.outcounts[:npts]

        points = range(1, npts+1)
        if reverse:
            points.reverse()
            self.sisdata  = self.sisdata[::-1]
            self.spectra  = self.spectra[::-1]
            self.realtime = self.realtime[::-1]
            self.livetime = self.livetime[::-1]
            self.dtfactor = self.dtfactor[::-1]
            self.inpcounts= self.inpcounts[::-1]
            self.outcounts= self.outcounts[::-1]

        xvals = [(gdata[i, ixaddr] + gdata[i-1, ixaddr])/2.0 for i in points]

        self.posvals = [np.array(xvals)]
        if dimension == 2:
            self.posvals.append(np.array([float(yvalue) for i in points]))
        self.posvals.append(self.realtime.sum(axis=1).astype('float32') / nmca)
        self.posvals.append(self.livetime.sum(axis=1).astype('float32') / nmca)

        # pform = "=Write Scan Data row=%i, npts=%i, folder=%s"
        # print pform % (irow, npts, self.folder)



class GSEXRM_MapFile:
    """
    GSECARS X-ray Microprobe Map File:

    The GSEXRM Map file is an HDF5 file built from a folder containing
    'raw' data from a set of sources
         xmap:   XRF spectra saved to NetCDF by the Epics MCA detector
         struck: a multichannel scaler, saved as ASCII column data
         xps:    stage positions, saved as ASCII file from the Newport XPS

    The object here is intended to expose an HDF5 file that:
         a) watches the corresponding folder and auto-updates when new
            data is available, as for on-line collection
         b) stores locking information (Machine Name/Process ID) in the top-level

    """
    ScanFile   = 'Scan.ini'
    EnvFile    = 'Environ.dat'
    ROIFile    = 'ROI.dat'
    MasterFile = 'Master.dat'

    def __init__(self, filename=None, folder=None):
        self.filename = filename
        self.folder   = folder
        self.status   = GSEXRM_FileStatus.err_notfound
        self.dimension = None
        self.start_time = None
        self.xrfmap   = None
        self.h5root   = None
        self.last_row = -1
        self.npts = None
        self.roi_slices = None

        # initialize from filename or folder
        if self.filename is not None:
            self.status   = getFileStatus(self.filename)

        elif isGSEXRM_MapFolder(self.folder):
            self.read_master()
            if self.filename is None:
                raise GSEXRM_Exception(
                    "'%s' is not a valid GSEXRM Map folder" % self.folder)
            self.status   = getFileStatus(self.filename)


        # for existing file, read initial settings
        if self.status in (GSEXRM_FileStatus.hasdata,
                           GSEXRM_FileStatus.created):
            self.open(self.filename, check_status=False)
            return

        # file exists but is not hdf5
        if self.status ==  GSEXRM_FileStatus.err_nothdf5:
            raise GSEXRM_Exception(
                "'%s' is not an HDF5 file" % self.filename)

        # create empty HDF5 if needed
        if (self.status == GSEXRM_FileStatus.err_notfound and
            self.folder is not None and isGSEXRM_MapFolder(self.folder)):
            self.read_master()
            self.h5root = h5py.File(self.filename)
            if self.dimension is None:
                self.read_master()
            create_xrfmap(self.h5root, dimension=self.dimension,
                          folder=self.folder, start_time=self.start_time)
            self.status = GSEXRM_FileStatus.created
            self.open(self.filename, check_status=False)
        else:
            raise GSEXRM_Exception(
                "'GSEXMAP Error: could not locate map file or folder")


    def open(self, filename, check_status=True):
        """open GSEXRM HDF5 File :

        with check_status=False, this **must** be called
        for an existing, valid GSEXRM HDF5 File!!
        """
        if check_status:
            self.status   = getFileStatus(filename)
            if self.status not in (GSEXRM_FileStatus.hasdata,
                                   GSEXRM_FileStatus.created):
                raise GSEXRM_Exception(
                    "'%s' is not a valid GSEXRM HDF5 file" % self.filename)
        self.filename = filename

        if self.h5root is None:
            self.h5root = h5py.File(self.filename)
        self.xrfmap = self.h5root['/xrfmap']
        if self.folder is None:
            self.folder = self.xrfmap.attrs['Map_Folder']
        self.is_owner = self.check_hostid()
        self.h5_modtime = os.stat(self.filename).st_mtime
        self.last_row = self.xrfmap.attrs['Last_Row']

    def close(self):
        self.xrfmap.attrs['Process_Machine'] = ''
        self.xrfmap.attrs['Process_ID'] = 0
        self.xrfmap.attrs['Last_Row'] = self.last_row
        self.resize_arrays(self.last_row)
        self.h5root.flush()
        self.h5root.close()
        self.h5root = None

    def add_data(self, group, name, data, attrs=None, **kws):
        """ creata an hdf5 dataset"""
        kwargs = {'compression': 4}
        kwargs.update(kws)
        d = group.create_dataset(name, data=data, **kwargs)
        if isinstance(attrs, dict):
            for key, val in attrs.items():
                d.attrs[key] = val
        return d

    def add_map_config(self, config):
        """add configuration from Map Folder to HDF5 file
        ROI, DXP Settings, and Config data
        """
        group = self.xrfmap['config']
        scantext = open(os.path.join(self.folder, self.ScanFile), 'r').read()
        for name, sect in (('scan', 'scan'),
                           ('general', 'general'),
                           ('positioners', 'slow_positioners'),
                           ('motor_controller', 'xps')):
            for key, val in config[sect].items():
                group[name].create_dataset(key, data=val)

        group['scan'].create_dataset('text', data=scantext)

        roidat, calib, dxp = readROIFile(os.path.join(self.folder, self.ROIFile))
        roi_desc, roi_addr, roi_lim = [], [], []
        roi_slices = []
        for iroi, label, lims in roidat:
            roi_desc.append(label)
            roi_addr.append("%smca%%i.R%i" % (config['general']['xmap'], iroi))
            roi_lim.append([lims[i] for i in range(4)])
            roi_slices.append([slice(lims[i][0], lims[i][1]) for i in range(4)])
        roi_lim = np.array(roi_lim)

        self.add_data(group['rois'], 'name',     roi_desc)
        self.add_data(group['rois'], 'address',  roi_addr)
        self.add_data(group['rois'], 'limits',   roi_lim)

        for key, val in calib.items():
            self.add_data(group['mca_calib'], key, val)

        for key, val in dxp.items():
            self.add_data(group['mca_settings'], key, val)

        self.roi_desc = roi_desc
        self.roi_addr = roi_addr
        self.roi_slices = roi_slices
        self.calib = calib
        # add env data
        envdat = readEnvironFile(os.path.join(self.folder, self.EnvFile))
        env_desc, env_addr, env_val = parseEnviron(envdat)

        self.add_data(group['environ'], 'name',     env_desc)
        self.add_data(group['environ'], 'address',  env_addr)
        self.add_data(group['environ'], 'value',     env_val)
        self.h5root.flush()

    def initialize_xrfmap(self):
        """ initialize '/xrfmap' group in HDF5 file, generally
        possible once at least 1 row of raw data is available
        in the scan folder.
        """
        if self.status == GSEXRM_FileStatus.hasdata:
            return
        if self.status != GSEXRM_FileStatus.created:
            print 'Warning, cannot initialize xrfmap yet.'
            return

        if not self.check_hostid():
            raise GSEXRM_NotOwner(self.filename)

        if self.dimension is None:
            self.read_master()
        self.npts = None
        if len(self.rowdata) < 1:
            return
        self.last_row = -1
        self.add_map_config(self.mapconf)
        row = self.read_rowdata(0)
        self.build_schema(row)
        self.add_rowdata(row)
        self.status = GSEXRM_FileStatus.hasdata


    def process(self, maxrow=None, force=False):
        "look for more data from raw folder, process if needed"
        if not self.check_hostid():
            raise GSEXRM_NotOwner(self.filename)

        if self.status == GSEXRM_FileStatus.created:
            self.initialize_xrfmap()

        nrows = len(self.rowdata)
        if maxrow is not None:
            nrows = min(nrows, maxrow)
        print 'Process:  ', nrows, force, self.folder_has_newdata()
        if force or self.folder_has_newdata():
            print 'PROCESS NEW DATA ', self.last_row, len(self.rowdata)
            irow = self.last_row
            while irow < nrows:
                irow  +=1
                row = self.read_rowdata(irow)
                if row is not None:
                    self.add_rowdata(row)
                    self.xrfmap.attrs['Last_Row'] = self.last_row

    def read_rowdata(self, irow):
        """read a row's worth of raw data from the Map Folder
        returns arrays of data
        """
        if self.dimension is None or irow > len(self.rowdata):
            self.read_master()

        if self.folder is None or irow >= len(self.rowdata):
            return

        yval, xmapf, sisf, xpsf, etime = self.rowdata[irow]
        reverse = (irow % 2 != 0)
        print 'Read ROW ', yval, xmapf, sisf, reverse
        row = GSEXRM_MapRow(yval, xmapf, xpsf, sisf, ixaddr=self.ixaddr,
                            dimension=self.dimension, npts=self.npts,
                            folder=self.folder, reverse=reverse)
        return row

    def add_rowdata(self, row):
        """adds a row worth of real data"""
        thisrow = self.last_row + 1
        print 'Add Row:: ', self.last_row, thisrow
        xnpts, nmca, nchan = row.spectra.shape
        mcas = []
        map_items = sorted(self.xrfmap.keys())
        for gname in map_items:
            g = self.xrfmap[gname]
            if g.attrs.get('type', None) == 'mca detector':
                mcas.append(g)
                nrows, npts, nchan =  g['data'].shape

        if thisrow >= nrows:
            self.resize_arrays(32*(1+nrows/32))

        total = None
        for imca, grp in enumerate(mcas):
            dtcorr = row.dtfactor[:, imca].astype('float32')
            cor   = dtcorr.reshape((dtcorr.shape[0], 1))
            grp['dtfactor'][thisrow, :]  = dtcorr
            grp['realtime'][thisrow, :]  = row.realtime[:,imca]
            grp['livetime'][thisrow, :]  = row.livetime[:,imca]
            grp['inpcounts'][thisrow, :] = row.inpcounts[:, imca]
            grp['outcounts'][thisrow, :] = row.outcounts[:, imca]
            grp['data'][thisrow, :, :]   = row.spectra[:, imca, :]
            if total is None:
                total = row.spectra[:, imca, :] * cor
            else:
                total = total + row.spectra[:, imca, :] * cor

        # here, we add the total dead-time-corrected data to detsum.
        self.xrfmap['detsum']['data'][thisrow, :] = total.astype('int16')

        # now add roi map data
        roimap = self.xrfmap['roimap']
        pos    = roimap['pos']
        pos[thisrow, :, :] = np.array(row.posvals).transpose()


        det_raw = roimap['det_raw']
        det_cor = roimap['det_cor']
        sum_raw = roimap['sum_raw']
        sum_cor = roimap['sum_cor']

        draw = list(row.sisdata[:npts].transpose())
        # print "======== NPTS ", sdata.shape, xmdat.shape, npts, nmca
        dcor = draw[:]
        sraw = draw[:]
        scor = draw[:]

        if self.roi_slices is None:
            lims = self.xrfmap['config/rois/limits'].value
            nrois, nmca, nx = lims.shape
            for iroi in range(nrois):
                x = [slice(lims[iroi, i, 0],
                           lims[iroi, i, 1]) for i in range(nmca)]
                self.roi_slices.append(x)

        for slices in self.roi_slices:
            iraw = [row.spectra[:, i, slices[i]].sum(axis=1)
                    for i in range(nmca)]
            icor = [row.spectra[:, i, slices[i]].sum(axis=1)*row.dtfactor[:, i]
                    for i in range(nmca)]
            draw.extend(iraw)
            dcor.extend(icor)
            sraw.append(np.array(iraw).sum(axis=0))
            scor.append(np.array(icor).sum(axis=0))
        det_raw[thisrow, :, :] = np.array(draw).transpose()
        det_cor[thisrow, :, :] = np.array(dcor).transpose()
        sum_raw[thisrow, :, :] = np.array(sraw).transpose()
        sum_cor[thisrow, :, :] = np.array(scor).transpose()

        self.last_row = thisrow
        self.xrfmap.attrs['Last_Row'] = thisrow
        print ' Last row is now ', thisrow
        self.h5root.flush()

    def build_schema(self, row):
        """build schema for detector and scan data"""
        NINIT = 16
        COMP = 4 # compression level

        if self.npts is None:
            self.npts = row.npts
        npts = self.npts
        xnpts, nmca, nchan = row.spectra.shape
        en_index = np.arange(nchan)

        xrfmap = self.xrfmap
        conf   = self.xrfmap['config']

        offset = conf['mca_calib/offset'].value
        slope  = conf['mca_calib/slope'].value
        quad   = conf['mca_calib/quad'].value

        roi_names = list(conf['rois/name'])
        roi_addrs = list(conf['rois/address'])
        roi_limits = conf['rois/limits'].value
        for imca in range(nmca):
            dname = 'det%i' % (imca+1)
            dgrp = xrfmap.create_group(dname)
            dgrp.attrs['type'] = 'mca detector'
            dgrp.attrs['desc'] = 'mca%i' % (imca+1)
            en   = 1.0*offset[imca] + slope[imca]*1.0*en_index
            self.add_data(dgrp, 'energy', en, attrs={'cal_offset':offset[imca],
                                                     'cal_slope': slope[imca]})

            self.add_data(dgrp, 'roi_names', roi_names)
            self.add_data(dgrp, 'roi_addrs', [s % (imca+1) for s in roi_addrs])
            self.add_data(dgrp, 'roi_limits', roi_limits[:,imca,:])

            dgrp.create_dataset('data', (NINIT, npts, nchan), np.int16,
                                compression=COMP, maxshape=(None, npts, nchan))
            for name, dtype in (('realtime', np.int),  ('livetime', np.int),
                                ('dtfactor', np.float32),
                                ('inpcounts', np.float32),
                                ('outcounts', np.float32)):
                dgrp.create_dataset(name, (NINIT, npts), dtype,
                                    compression=COMP, maxshape=(None, npts))

        # add 'virtual detector' for corrected sum:
        dgrp = xrfmap.create_group('detsum')
        dgrp.attrs['type'] = 'mca virtual detector'
        dgrp.attrs['desc'] = 'deadtime corrected sum of detectors'
        en = 1.0*offset[0] + slope[0]*1.0*en_index
        self.add_data(dgrp, 'energy', en, attrs={'cal_offset':offset[0],
                                                 'cal_slope': slope[0]})
        self.add_data(dgrp, 'roi_names', roi_names)
        self.add_data(dgrp, 'roi_addrs', [s % 1 for s in roi_addrs])
        self.add_data(dgrp, 'roi_limits', roi_limits[: ,0, :])
        dgrp.create_dataset('data', (NINIT, npts, nchan), np.int16,
                            compression=COMP, maxshape=(None, npts, nchan))

        # roi map data
        scan = xrfmap['roimap']
        det_addr = [i.strip() for i in row.sishead[-2][1:].split('|')]
        det_desc = [i.strip() for i in row.sishead[-1][1:].split('|')]
        for addr in roi_addrs:
            det_addr.extend([addr % (i+1) for i in range(nmca)])

        for desc in roi_names:
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
        for pname in ('mca realtime', 'mca livetime'):
            self.pos_desc.append(pname)
            self.pos_addr.append(pname)
        npos = len(self.pos_desc)
        self.add_data(scan, 'pos_name',     self.pos_desc)
        self.add_data(scan, 'pos_address',  self.pos_addr)

        for name, nx, dtype in (('det_raw', nsca, np.int32),
                                ('det_cor', nsca, np.float32),
                                ('sum_raw', nsum, np.int32),
                                ('sum_cor', nsum, np.float32),
                                ('pos',     npos, np.float32)):
            scan.create_dataset(name, (NINIT, npts, nx), dtype,
                                compression=COMP, maxshape=(None, npts, nx))

    def resize_arrays(self, nrow):
        "resize all arrays for new nrow size"
        mca_groups  = []
        for g in self.xrfmap.value():
            # include both real and virtual mca detectors!
            if g.attrs.get('type', None).startswith('mca'):
                mca_groups.append(g)
        oldnrow, npts, nchan =  mca_groups[0]['data'].shape
        for g in mca_groups:
            g['data'].resize((nrow, npts, nchan))
            for aname in ('livetime', 'realtime',
                          'inpcounts', 'outcounts', 'dtfactor'):
                g[aname].resize((nrow, npts))

        for aname in ('pos', 'det_raw', 'det_cor', 'sum_raw', 'sum_cor'):
            g = self.xrfmap['roimap'][aname]
            old, npts, nx = g.shape
            g.resize((nrow, npts, nx))


    def check_hostid(self):
        """checks host and id of file:
        returns True if this process the owner?
        """
        if self.xrfmap is None:
            return

        attrs = self.xrfmap.attrs
        file_mach = attrs['Process_Machine']
        file_pid  = attrs['Process_ID']
        thisname  = socket.gethostname()
        thispid   = os.getpid()
        if len(file_mach) < 1 or file_pid < 1:
            file_mach, file_pid = thisname, thispid

        self.folder = attrs['Map_Folder']
        return (file_mach == thisname and file_pid == thispid)

    def folder_has_newdata(self):
        if self.folder is not None:
            self.read_master()
        return (self.last_row < len(self.rowdata))

    def read_master(self):
        "reads master file for toplevel scan info"
        if self.folder is None:
            return
        self.masterfile = os.path.join(nativepath(self.folder),
                                       self.MasterFile)
        try:
            header, rows = readMasterFile(self.masterfile)
        except IOError:
            raise GSEXRM_Exception(
                "cannot read Master file from '%s'" % self.masterfile)

        self.master_header = header
        self.rowdata = rows
        stime = self.master_header[0][6:]
        self.start_time = stime.replace('started at','').strip()

        self.folder_modtime = os.stat(self.masterfile).st_mtime
        self.stop_time = time.ctime(self.folder_modtime)

        cfile = FastMapConfig()
        cfile.Read(os.path.join(self.folder, self.ScanFile))
        self.mapconf = cfile.config

        if self.filename is None:
            self.filename = self.mapconf['scan']['filename']

        mapconf = self.mapconf
        slow_pos = mapconf['slow_positioners']
        fast_pos = mapconf['fast_positioners']

        scanconf = mapconf['scan']
        self.dimension = scanconf['dimension']
        start = mapconf['scan']['start1']
        stop  = mapconf['scan']['stop1']
        step  = mapconf['scan']['step1']
        span = abs(stop-start)
        self.npts = int(abs(step*1.01 + span)/step)

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


