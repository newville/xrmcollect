import os
import socket
import h5py
import numpy as np
from ..io.h5_writer import H5Writer

def randname(n=6):
    "return random string of n (default 6) lowercase letters"
    return ''.join([chr(randrange(26)+97) for i in range(n)])

def isGSEXRM_MapFile(fname):
    "return whether fname is a valid HDF5 file for a GSE XRM Map"
    valid = False
    try:
        fh = h5py.File(fname)
        xrfmap = fh['/xrf_map']
        tmp = xrfmap.attrs['Version'], xrfmap.attrs['Beamline']
        tmp = xrfmap['config'], xrfmap['scan']
        tmp = xrfmap['det1/data'], xrfmap['det1/energy'], xrfmap['det1/roi_limits']
        valid = True
    except:
        pass
    finally:
        fh.close()
    return valid

def isGSEXRM_MapFolder(fname):
    "return whether folder a valid Scan Folder (raw data)"
    if not os.path.isdir(fname):
        return False
    flist = os.listdir(fname)
    for f in ('Master.dat', 'Environ.dat', 'Scan.ini', 'xmap.0001'):
        if f not in flist:
            return False
    return True

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
    def __init__(self, filename):
        self.filename = filename
        self.valid = self.open(filename)
        if self.valid:
            self.owner = self.check_hostid()


    def open(self, filename):
        try:
            self.h5file_modtime = os.stat(filename).st_mtime
            self.root = h5py.File(filename, 'a')
            self.parent, self.filename = os.path.split(filename)
            self.xrfmap = self.root['/xrf_map']
            self.folder = attrs['Map_Folder']
            tmp = xrfmap.attrs['Version'], xrfmap.attrs['Beamline']
            tmp = xrfmap['config'], xrfmap['scan']
            tmp = xrfmap['det1/data'], xrfmap['det1/energy'], xrfmap['det1/roi_limits']
            return True
        except:
            return False

    def close(self):
        self.xrfmap.attrs['Process_Machine'] = ''
        self.xrfmap.attrs['Process_ID'] = 0
        self.root.close()
        self.root = None

    def check_hostid(self):
        """checks host and id of file: returns True if this process the owner?"""
        file_mach = self.xrfmap.attrs['Process_Machine']
        file_pid  = self.xrfmap.attrs['Process_ID']
        thisname  = socket.gethostname()
        thispid   = os.getpid()
        if len(file_mach) == 0 or file_pid < 1:
            attrs['Process_Machine'], attrs['Process_ID'] = thisname, thispid

        self.folder = self.xrfmap.attrs['Map_Folder']
        return (file_mach == thisname and file_pid == thispid)

    def folder_has_newdata(self):
        self.folder_modtime = 0
        if self.folder != '':
            self.masterfile = os.path.join(self.parent, self.folder, 'Master.dat')
            if os.path.exists(self.masterfile):
                self.folder_modtime = os.stat(self.masterfile).st_mtime
        return (self.h5file_modtime < self.folder_modtime)

    def process(self):
        "look for more data from raw folder, process if needed"
        if self.check_hostid() and self.folder_has_newdata():
            self.close()
            h5w = H5Writer(folder=self.folder)
            h5w.process()
            h5w.close()
            self.open()
            self.check_hostid()

