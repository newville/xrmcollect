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
    no_xrfmap   = 0  # HDF5 created, no xrf_map group
    created     = 1  # HDF5 started, xrf_map exists
    initialized = 2  # 1+ row written: array sizes known

    err_notfound = 101  # file does not exist 
    err_nothdf5  = 102  


def getFileStatus(filename):
    # see if file exists:
    if (not os.path.exists(filename) or
        not os.path.isfile(filename) ):
        return GSEXRM_FileStatus.err_notfound
    
    # see if file is an H5 file
    try:
        fh = h5py.File(filename, 'r')
    except IOError:
        return GSEXRM_FileStatus.err_nothdf5

    if 'xrf_map' not in fh:
        return GSEXRM_FileStatus.no_xrfmap

    if 'det1' in fh['/xrf_map']:
        return GSEXRM_FileStatus.initialized
    
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


class GSEXRM_Exception(Exception):
    """GSEXRM Exception: General Errors"""
    def __init__(self, msg):
        Exception.__init__(self)
        self.msg = msg
    def __str__(self):
        return self.msg


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

    H5_attrs = {'Version': '1.3.0',
                'Title': 'Epics Scan Data',
                'Beamline': 'GSECARS, 13-IDE / APS',
                'Scan_Type': 'FastMap',
                'Correct_Deadtime': 'True'}

    
    def __init__(self, filename=None, folder=None):
        self.filename = filename
        self.folder   = folder
        self.status   = GSEXRM_FileStatus.err_notfound
        self.dimension = None
        self.xrfmap   = None
        self.h5root   = None
        self.__initialize_from_filefolder()

    def __initialize_from_filefolder(self):
        """initialize from filename or folder"""

        if self.filename is not None:
            self.status   = getFileStatus(self.filename)

        print '__INIT__  ', self.filename, self.folder, self.status

        # for existing file, read initial settings
        if self.status in (GSEXRM_FileStatus.initialized,
                           GSEXRM_FileStatus.created):
            self.open(self.filename, check_status=False)
            return

        # file exists but is not hdf5
        if self.status ==  GSEXRM_FileStatus.err_nothdf5:
            raise GSEXRM_Exception(
                "'%s' is not an HDF5 file" % self.filename)

        # create empty HDF5 if needed
        if (self.status == GSEXRM_FileStatus.err_notfound and
            self.folder is not None and
            isGSEXRM_MapFolder(self.folder)):

            self.read_master()
            if self.filename is None:
                raise GSEXRM_Exception(
                    "'%s' is not a valid GSEXRM Map folder" % self.folder)

            if not self.filename.endswith('.h5'):
                self.filename = "%s.h5" % self.filename
            if not os.path.exists(self.filename):
                self.h5root = h5py.File(self.filename, 'w')
            # re-call this routine now that an empty file exists
            return self.__initialize_from_filefolder()
                
        # initialize xrf_map group to HDF5 if needed
        if (self.status ==  GSEXRM_FileStatus.no_xrfmap and
            self.folder is not None):
            if not isGSEXRM_MapFolder(self.folder):
                raise GSEXRM_Exception(
                    "'%s' is not a valid GSEXRM Map folder" % self.folder)
            self.create_xrfmap()
            self.status = GSEXRM_FileStatus.created                

    def open(self, filename, check_status=True):
        """open GSEXRM HDF5 File :

        with check_status=False, this **must** be called
        for an existing, valid GSEXRM HDF5 File!!
        """
        if check_status:
            self.status   = getFileStatus(filename)
            if self.status not in (GSEXRM_FileStatus.initialized,
                                   GSEXRM_FileStatus.created):
                raise GSEXRM_Exception(
                    "'%s' is not a valid GSEXRM HDF5 file" % self.filename)
                
        
        self.h5root = h5py.File(self.filename, 'a')
        self.xrfmap = self.h5root['/xrf_map']
        if self.folder is None:
            self.folder = xrfmap.attrs['Map_Folder']
        self.is_owner = self.check_hostid()
        self.h5_modtime = os.stat(self.filename).st_mtime            

    def close(self):
        self.xrfmap.attrs['Process_Machine'] = ''
        self.xrfmap.attrs['Process_ID'] = 0
        self.h5root.close()
        self.h5root = None


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

    def create_xrfmap(self):
        """ create skeleton '/xrf_map' group in HDF5 file
        This leaves a structure to be filled in byt the init_xrfmap(),
        """
        print 'Create XRF MAP '
        if self.dimension is None:
            self.read_master()

        if self.h5root is None:
            self.h5root = h5py.File(self.filename, 'w')
   
        attrs = {'Dimension':self.dimension,
                 'Stop_Time':self.stop_time,
                 'Start_Time':self.start_time,
                 'Map_Folder': self.folder,
                 'Process_Machine': '',
                 'Process_ID': 0}
        attrs.update(self.H5_attrs)

        self.xrfmap = self.h5root.create_group('xrf_map')
        for key, val in attrs.items():
            self.xrfmap.attrs[key] = val
            
        self.xrfmap.create_group('scan')
        self.xrfmap.create_group('config')
        # self.add_map_config(self.xrfmap, self.mapconf)
        

    def add_map_config(self, root, config):
        """add configuration from Map Folder to HDF5 file
        ROI, DXP Settings, and Config data
        """

        group = self.xrfmap['config']

        scantext = open(path.join(self.folder, self.ScanFile), 'r').read()

        for name, sect in (('scan', 'scan'),
                           ('general', 'general'),
                           ('positioners', 'slow_positioners'),
                           ('motor_controller', 'xps')):
            grp = self.add_group(group, name)

            for key, val in config[sect].items():
                grp.create_dataset(key, data=val)

        group['scan'].create_dataset('text', data=scantext)

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


    def init_xrfmap(self):
        """ initialize '/xrf_map' group in HDF5 file, generally
        possible once at least 1 row of raw data is available
        in the scan folder.
        """
        
        self.add_map_config(self.xrfmap, self.mapconf)
        pass

    def read_raw_row(self, row):
        """read a row's worth of raw data from the Map Folder"""
        if self.folder is None:
            return
        
    def check_hostid(self):
        """checks host and id of file: returns True if this process the owner?"""
        if self.xrfmap is None:
            return
        
        attrs = self.xrfmap.attrs
        file_mach = attrs['Process_Machine']
        file_pid  = attrs['Process_ID']
        thisname  = socket.gethostname()
        thispid   = os.getpid()
        if len(file_mach) == 0 or file_pid < 1:
            attrs['Process_Machine'], attrs['Process_ID'] = thisname, thispid

        self.folder = attrs['Map_Folder']
        return (file_mach == thisname and file_pid == thispid)

    def folder_has_newdata(self):
        if self.folder is not None:
            self.masterfile = os.path.join(nativepath(self.folder),
                                           self.Mastefile)
            if os.path.exists(self.masterfile):
                self.folder_modtime = os.stat(self.masterfile).st_mtime
        return (self.h5_modtime < self.folder_modtime)


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
        print 'ReadMaster!!'
    

    def process(self, maxrow=None, force=False):
        "look for more data from raw folder, process if needed"
        if self.check_hostid() and (self.folder_has_newdata() or force):
            self.close()
            if self.h5writer is None:
                self.h5writer = H5Writer(folder=self.folder)
            self.h5writer.open()
            self.h5writer.process(maxrow=maxrow)
            self.h5writer.close()
            self.open(self.filename)
            self.check_hostid()


