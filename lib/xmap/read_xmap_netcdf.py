#!/usr/bin/python

import numpy as np
import time
import sys
import os

try:
    import scipy.io.netcdf
    netcdf_open = scipy.io.netcdf.netcdf_file
except ImportError:
    import netCDF4
    netcdf_open = netCDF4.Dataset
except ImportError:
    raise ImportError('cannot find a netcdf module')

def aslong(d):
    """unravels and converts array of int16 (int) to int32 (long)"""
    # need to unravel the array!!!
    d = d.astype(np.int16).ravel()
    d.dtype = np.int32
    return d

class xMAPBufferHeader(object):
    def __init__(self,buff):
        self.tag0          = buff[0]  # Tag word 0
        self.tag1          = buff[1]  # Tag word 1
        self.headerSize    = buff[2]  #  Buffer header size
        #  Mapping mode (1=Full spectrum, 2=Multiple ROI, 3=List mode)
        self.mappingMode   = buff[3]
        self.runNumber     = buff[4]  # Run number
        # Sequential buffer number, low word first
        self.bufferNumber  = aslong(buff[5:7])[0]
        self.bufferID      = buff[7]  # 0=A, 1=B
        self.numPixels     = buff[8]  # Number of pixels in buffer
        # Starting pixel number, low word first
        self.startingPixel = aslong(buff[9:11])[0]
        self.moduleNumber  = buff[11]
        self.channelID     = np.array(buff[12:20]).reshape((4,2))
        self.channelSize   = buff[20:24]
        self.bufferErrors  = buff[24]
        self.userDefined   = buff[32:64]

class xMAPMCAPixelHeader(object):
    def __init__(self,buff):
        self.tag0        = buff[0]
        self.tag1        = buff[1]
        self.headerSize  = buff[2]
        # Mapping mode (1=Full spectrum, 2=Multiple ROI, 3=List mode)
        self.mappingMode = buff[3]
        self.pixelNumber = aslong(buff[4:6])[0]
        self.blockSize   = aslong(buff[6:8])[0]

class xMAPData(object):
    def __init__(self,npix,nmod,nchan):
        ndet = 4 * nmod
        self.firstPixel   = 0
        self.numPixels    = 0
        self.data         = np.zeros((npix, ndet, nchan), dtype='i2')
        self.realTime     = np.zeros((npix, ndet), dtype='i8')
        self.liveTime     = np.zeros((npix, ndet), dtype='i8')
        self.inputCounts  = np.zeros((npix, ndet), dtype='i4')
        self.outputCounts = np.zeros((npix, ndet), dtype='i4')

def read_xmap_netcdf(fname, verbose=False):
    # Reads a netCDF file created with the DXP xMAP driver
    # with the netCDF plugin buffers

    if verbose: print ' reading ', fname

    t0 = time.time()
    # read data from array_data variable of netcdf file
    fh = netcdf_open(fname,'r')
    array_data = fh.variables['array_data'][:]
    fh.close()
    t1 = time.time()

    # array_data will normally be 3d:
    #  shape = (narrays, nmodules, buffersize)
    # but nmodules and narrays could be 1, so that
    # array_data could be 1d or 2d.
    #
    # here we force the data to be 3d
    shape = array_data.shape
    if len(shape) == 1:
        array_data.shape = (1,1,shape[0])
    elif len(shape) == 2:
        array_data.shape = (1,shape[0],shape[1])

    narrays,nmodules,buffersize = array_data.shape
    modpixs    = array_data[0,0,8]
    if modpixs < 124: modpixs = 124
    npix_total = 0
    clocktick  = 320.e-9
    for array in range(narrays):
        for module in range(nmodules):
            d   = array_data[array,module,:]
            bh  = xMAPBufferHeader(d)
            #if verbose and array==0:
            #print  ' nc data shape: ', d.shape, d.size
            # print modpixs, (d.size-256), (d.size-256)/modpixs
            # print modpixs*(d.size-256)/(1.0*modpixs)
            dat = d[256:].reshape(modpixs, (d.size-256)/modpixs )

            npix = bh.numPixels
            if module == 0:
                npix_total += npix
                if array == 0:
                    # first time through, (array,module)=(0,0) we
                    # read mapping mode, set up how to slice the
                    # data, and build data arrays in xmapdat
                    mapmode = dat[0,3]
                    if mapmode == 1:  # mapping, full spectra
                        nchans = d[20]
                        data_slice = slice(256,8448)
                    elif mapmode == 2:  # ROI mode
                        # Note:  nchans = number of ROIS !!
                        nchans     = max(d[264:268])
                        data_slice = slice(64,64+8*nchans)
                    xmapdat = xMAPData(narrays*modpixs, nmodules, nchans)
                    xmapdat.firstPixel = bh.startingPixel

            # acquistion times and i/o counts data are stored
            # as longs in locations 32:64
            t_times = aslong(dat[:npix,32:64]).reshape(npix,4,4)
            p1 = npix_total - npix
            p2 = npix_total
            xmapdat.realTime[p1:p2,:]     = t_times[:,:,0] * clocktick
            xmapdat.liveTime[p1:p2,:]     = t_times[:,:,1] * clocktick
            xmapdat.inputCounts[p1:p2,:]  = t_times[:,:,2]
            xmapdat.outputCounts[p1:p2,:] = t_times[:,:,3]

            # the data, extracted as per data_slice and mapmode
            t_data = dat[:npix,data_slice]
            if mapmode == 2:
                t_data = aslong(t_data)
            xmapdat.data[p1:p2,:,:] = t_data.reshape(npix,4,nchans)

    t2 = time.time()
    xmapdat.numPixels = npix_total
    xmapdat.data = xmapdat.data[:npix_total]
    xmapdat.realTime = xmapdat.realTime[:npix_total]
    xmapdat.liveTime = xmapdat.liveTime[:npix_total]
    xmapdat.inputCounts  = xmapdat.inputCounts[:npix_total]
    xmapdat.outputCounts = xmapdat.outputCounts[:npix_total]
    if verbose:
        print '   time to read file    = %5.1f ms' % ((t1-t0)*1000)
        print '   time to extract data = %5.1f ms' % ((t2-t1)*1000)
        print '   read %i pixels ' %  npix_total
        print '   data shape:    ' ,  xmapdat.data.shape
    return xmapdat

if __name__ == '__main__':
    import sys, os
    fname = sys.argv[1]

    print fname,  os.stat(fname)
    fd = read_xmap_netcdf(fname, verbose=True)
    print fd.data.shape
