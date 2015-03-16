import time
import os
import sys
import numpy
import epics
from threading import Thread

from epics import caput
from epics.devices.struck import Struck

from .utils import debugtime
from .io.file_utils import (nativepath, winpath, fix_filename,
                            increment_filename, basepath)
from .io.escan_writer import EscanWriter

from .xps.xps_trajectory import XPSTrajectory
from .xrd_ad import PerkinElmer_AD
from .xmap import MultiXMAP
from .xmap.xsp3 import XSP3

from mapper import mapper
from config import FastMapConfig
from set_mono_tilt import set_mono_tilt

# USE_MONO_CONTROL = True

USE_MONO_CONTROL = True
SCAN_VERSION = '1.2'
ROW_MSG = 'Row %i complete, npts (XPS, SIS, XMAP) = (%i, %i, %i)'
ROW_MSG = '(%i, %i/%i/%i)'

POSITIONER_OFFSETS = {'X':1, 'Y':0, 'THETA':0}

def fix_range(start=0,stop=1,step=0.1, addstep=False):
    """returns (npoints,start,stop,step) for a trajectory
    so that the start and stop points are on the trajectory
    boundaries and will be included in the scan.
    """
    if stop < start:
        start, stop= stop, start
    step = abs(step)
    if addstep:
        start= start - step/2.0
        stop = stop  + step/2.0
    span = abs(stop-start)
    if abs(span) < 1.e-12:
        return (1, start, stop, 0)
    npts = int(abs(step*1.01 + span)/step)
    stop = start + step * (npts-1)
    return (npts,start,stop,step)

class TrajectoryScan(object):
    def __init__(self, xrf_prefix='13SDD1:', configfile=None):
        self._pvs = {}
        self.state = 'idle'
        self.xmap = None
        self.xsp3 = None
        self.xrdcam = None

        conf = self.mapconf = FastMapConfig(configfile)
        struck        = conf.get('general', 'struck')
        scaler        = conf.get('general', 'scaler')
        basedir       = conf.get('general', 'basedir')
        mapdb         = conf.get('general', 'mapdb')
        self.use_xrd  = conf.get('xrd_ad', 'use')
        self.use_xrf  = conf.get('xrf',   'use')
        self.xrf_type = conf.get('xrf', 'type')
        self.xrf_pref = conf.get('xrf', 'prefix')
        
        self.mapper = mapper(prefix=mapdb)
        self.scan_t0  = time.time()
        self.Connect_ENV_PVs()

        self.ROI_Written = False
        self.ENV_Written = False
        self.ROWS_Written = False
        self.xps = XPSTrajectory(**conf.get('xps'))
        self.dtime = debugtime()

        self.struck = Struck(struck, scaler=scaler)
        self.struck.read_all_mcas()

        print 'Using xrf type/prefix= ', self.xrf_type, self.xrf_pref
        if self.use_xrf:
            if self.xrf_type.startswith('xmap'):
                self.xmap = MultiXMAP(self.xrf_pref)
            elif self.xrf_type.startswith('xsp'):
                self.xsp3 = XSP3(self.xrf_pref, fileroot='/T/')

        if self.use_xrd:
            filesaver = conf.get('xrd_ad', 'fileplugin')
            prefix = conf.get('xrd_ad', 'prefix')
            self.xrdcam = PerkinElmer_AD(prefix, filesaver=filesaver)

        self.positioners = {}
        for pname in conf.get('slow_positioners'):
            self.positioners[pname] = self.PV(pname)
        self.mapper.add_callback('Start', self.onStart)
        self.mapper.add_callback('Abort', self.onAbort)
        self.mapper.add_callback('basedir', self.onDirectoryChange)
        self.prepare_beam_ok()

    def prepare_beam_ok(self):
        conf = self.mapconf.get('beam_ok')
        self.flux_val_pv = self.PV(conf['flux_val_pv'])
        self.flux_min_pv = self.PV(conf['flux_min_pv'])
        self.shutter_status = [self.PV(x.strip()) for x in conf['shutter_status'].split('&')]
        self.shutter_open = [self.PV(x.strip()) for x in conf['shutter_open'].split('&')]

    def write(self, msg, flush=True):
        sys.stdout.write("%s\n"% msg)
        if flush:
            sys.stdout.flush()

    def PV(self, pvname):
        """return epics.PV for a device attribute"""
        if pvname not in self._pvs:
            self._pvs[pvname] = epics.PV(pvname)
        if not self._pvs[pvname].connected:
            self._pvs[pvname].wait_for_connection()
        return self._pvs[pvname]

    def onStart(self, pvname=None, value=None, **kw):
        if value == 1:
            self.state = 'start'

    def onAbort(self, pvname=None, value=None, **kw):
        if value == 1:
            self.state = 'abort'
        else:
            self.state = 'idle'

    def onDirectoryChange(self,value=None,char_value=None,**kw):
        if char_value is not None:
            os.chdir(os.path.abspath(nativepath(char_value)))

    def setWorkingDirectory(self):
        top_path = basepath(self.mapper.basedir)
        basedir = os.path.abspath(nativepath(self.mapper.basedir))
        try:
            os.chdir(basedir)
        except:
            self.write('Cannot chdir to %s' % basedir)

        fname = fix_filename(self.mapper.filename)
        subdir = fname + '_rawmap'
        counter = 0
        while os.path.exists(subdir) and counter < 9999:
            fname = increment_filename(fname)
            subdir = fname + '_rawmap'
            counter += 1
                
        os.mkdir(subdir)
        self.mapper.filename = fname

        # write h5 file stub (with name of folder) for viewing program
        h5fname = os.path.abspath(os.path.join(basedir, "%s.h5" % fname))
        fout  = open(h5fname, 'w')
        fout.write("%s\n"% subdir)
        fout.close()

        self.mapper.workdir = subdir
        self.workdir = os.path.abspath(os.path.join(basedir, subdir))
        self.write('=Scan folder: %s' % self.workdir)

        self.ROI_Written = False
        self.ENV_Written = False
        self.ROWS_Written = False
        return subdir

    def prescan(self, filename=None, filenumber=1, npulses=11,
                scantime=1.0, **kw):
        """ put all pieces (trajectory, struck, xmap) into
        the proper modes for trajectory scan"""
        self.npulses = npulses
        self.mapper.setTime()
        if self.use_xrf:
            if self.xrf_type.startswith('xmap'):
                self.xmap.setFileTemplate('%s%s.%4.4d')
                self.xmap.setFileWriteMode(2)
                self.xmap.MCAMode(filename='xmap', npulses=npulses)
                self.xmap.setFileNumber(1)
            elif self.xrf_type.startswith('xsp'):
                self.xsp3.ERASE = 1
                self.xsp3.NumImages = min(4000, npulses + 1)
                self.xsp3.setFileWriteMode(2)
                self.xsp3.useExternalTrigger()
                self.xsp3.setFileTemplate('%s%s.%4.4d')
                self.xsp3.setFileName('xsp3')
                self.xsp3.setFileNumber(1)
                time.sleep(0.1)
                self.xsp3.UPDATE = 1
                self.xsp3.ERASE = 1
                time.sleep(0.1)

        if self.use_xrd:
            self.xrdcam.setFilePath(winpath(self.workdir))
            time_per_pixel = scantime/(npulses-1)
            # print 'PreScan XRD Camera: Time per pixel ', npulses, time_per_pixel
            self.xrdcam.SetExposureTime(time_per_pixel)
            self.xrdcam.SetMultiFrames(npulses)
            self.xrdcam.setFileName('xrd')

        time.sleep(0.25)
        self.dtime.add('prescan xrf det')

        self.struck.ExternalMode()
        self.struck.put('PresetReal', 0.0)
        self.struck.put('Prescale',   1.0)
        self.dtime.add('prescan struck')

        self.ROI_Written = False
        self.ENV_Written = False

    def postscan(self):
        """ put all pieces (trajectory, struck, xmap) into
        the non-trajectory scan mode"""
        self.mapper.setTime()
        if self.use_xrf:
            if self.xrf_type.startswith('xmap'):
                self.Wait_XMAPWrite(irow=0)
                self.dtime.add('postscan xmap data written')
                self.xmap.SpectraMode()
                self.dtime.add('postscan xmap in Spectra Mode')
            else:
                if self.xrf_type.startswith('xsp'):
                    self.Wait_Xspress3Write(irow=0)

            time.sleep(0.25)
            if self.use_xrd:
                self.xrdcam.filePut('EnableCallbacks', 0)
                self.xrdcam.ImageMode = 2
                self.xrdcam.TriggerMode = 0
        self.setIdle()
        self.dtime.add('postscan done')

    def save_positions(self, poslist=None):
        plist = self.positioners.keys()
        if poslist is not None:
            for p in poslist:
                if p not in plist:
                    plist.append(p)

        self.__savedpos={}
        for pvname in plist:
            self.__savedpos[pvname] = self.PV(pvname).get()
        self.dtime.add('save_positions done')

    def restore_positions(self):
        for pvname,val in self.__savedpos.items():
            self.PV(pvname).put(val)
        self.dtime.add('restore_positions done')

    def Wait_XMAPWrite(self, irow=0):
        """wait for XMAP to finish writing its data"""
        fnum = irow
        # print 'Wait for XRF file', self.use_xrf, self.xrf_type
        if self.use_xrf and self.xrf_type.startswith('xmap'):
            # wait for previous netcdf file to be written
            t0 = time.time()
            time.sleep(0.1)
            if not self.xmap.FileWriteComplete():
                xmap_ok, npix = self.xmap.finish_pixels(timeout=5.0)
                self.rowdata_ok = xmap_ok
                if not xmap_ok:
                    self.write('Bad data -- XMAP too few pixels')

            while not self.xmap.FileWriteComplete():
                time.sleep(0.25)
                if time.time()-t0 > 3.0:
                    self.mapper.message = 'XMAP File Writing Not Complete!'
                    self.rowdata_ok = False
                    self.xmap.FileCaptureOff()
                    time.sleep(0.5)
                    self.xmap.SpectraMode()
                    time.sleep(0.5)
                    self.xmap.MCAMode(filename='xmap', npulses=self.npulses)
                    time.sleep(0.5)
                    print 'XMAP could not complete file writing!'
                    self.write('Bad data -- XMAP could not complete file writing')
                    break
            xmap_fname = nativepath(self.xmap.getLastFileName())[:-1]
            folder,xmap_fname = os.path.split(xmap_fname)
            prefix, suffix = os.path.splitext(xmap_fname)
            suffix = suffix.replace('.','')
            try:
                fnum = int(suffix)
            except:
                fnum = 1
        return fnum

    def Wait_Xspress3Write(self, irow=0):
        """wait for Xspress3 to finish writing its data"""
        fnum = irow
        if self.use_xrf and self.xrf_type.startswith('xsp'):
            # wait for previous file writing to complete
            if not self.xsp3.FileWriteComplete():
                t0 = time.time()
                while not self.xsp3.FileWriteComplete() and (time.time()-t0 < 5.0):
                    time.sleep(0.1)
                if not self.xsp3.FileWriteComplete():
                    self.mapper.message = 'Xspress3 File Writing Not Complete!'
                    self.rowdata_ok = False
                    time.sleep(0.5)
                    self.xsp3.stop()
                    time.sleep(0.5)
                    self.write('Bad data -- Xspress3 could not complete file writing')

            x_fname = nativepath(self.xsp3.getLastFileName())[:-1]
            folder, x_fname = os.path.split(x_fname)
            prefix, suffix = os.path.splitext(x_fname)
            suffix = suffix.replace('.','')
            try:
                fnum = int(suffix)
            except:
                fnum = 1
        return fnum

    def run_scan(self, filename='TestMap',scantime=10, accel=None,
                 pos1='13XRM:m1', start1=0, stop1=1, step1=0.1,
                 dimension=1,
                 pos2=None, start2=0, stop2=1, step2=0.1, **kws):
        self.dtime.clear()
        if pos1 not in self.positioners:
            raise ValueError(' %s is not a trajectory positioner' % pos1)

        self.mapper.status = 1
        npts1, start1, stop1, step1 = fix_range(start1, stop1, step1, addstep=True)

        step2_positive =  start2 < stop2
        npts2, start2, stop2, step2 = fix_range(start2, stop2, step2, addstep=False)
        if not step2_positive:
            start2, stop2 = stop2, start2
            step2 = -step2
        # set offset for whether to start with foreward or backward trajectory
        dir_offset = 0
        if start1 > stop1:
            dir_offset = 1

        self.mapper.npts = npts1
        self.mapper.setNrow(0)
        self.mapper.maxrow  = npts2
        self.mapper.info    = 'Pending'
        self.mapper.message = "will execute %i points in %.2f sec" % (npts1,scantime)
        self.state = 'pending'

        self.save_positions()
        self.dtime.add( 'Saved Positions')
        if pos2 is None:
            dimension = 1
            npts2 = 1
        self.nrows_expected = npts2

        self.scan_t0 = time.time()
        self.MasterFile.write('#SCAN.version   = %s\n' % SCAN_VERSION)
        self.MasterFile.write('#SCAN.starttime = %s\n' % time.ctime())
        self.MasterFile.write('#SCAN.filename  = %s\n' % filename)
        self.MasterFile.write('#SCAN.dimension = %i\n' % dimension)
        self.MasterFile.write('#SCAN.nrows_expected = %i\n' % npts2)
        self.MasterFile.write('#SCAN.time_per_row_expected = %.2f\n' % scantime)
        self.MasterFile.write('#Y.positioner  = %s\n' %  str(pos2))
        self.MasterFile.write('#Y.start_stop_step = %f, %f, %f \n' %  (start2, stop2, step2))
        self.MasterFile.write('#------------------------------------\n')
        self.MasterFile.write('# yposition  xmap_file  struck_file  xps_file    time\n')

        self.dtime.add( 'Master header')
        kw = dict(scantime=scantime, accel=accel,
                  filename=self.mapper.filename, filenumber=0,
                  dimension=dimension, npulses=npts1-1, scan_pt=1)

        axis1 = self.mapconf.get('fast_positioners', pos1).upper()

        linescan = dict(start=start1, stop=stop1, step=step1,
                        axis=axis1, scantime=scantime, accel=accel)

        if not self.xps.DefineLineTrajectories(**linescan):
            print 'Failed to define trajectory!!'
            self.postscan()
            return
        print 'Run_Scan: Defined Trajectories'
        self.dtime.add('trajectory defined')

        # move to starting position
        dir_offset += POSITIONER_OFFSETS[axis1]
        p1_start = start1
        if dir_offset % 2 != 0:
            p1_start = stop1

        self.PV(pos1).put(p1_start, wait=False)
        self.dtime.add( 'put #1 done')
        if dimension > 1:
            self.PV(pos2).put(start2, wait=False)
        self.dtime.add('put positioners to starting positions')
        self.prescan(**kw)
        self.dtime.add( 'prescan done')
        self.PV(pos1).put(p1_start, wait=True)
        if dimension > 1:
            self.PV(pos2).put(start2, wait=True)

        if self.use_xrf:
            if self.xrf_type.startswith('xmap'):
                self.xmap.FileCaptureOn()
            elif self.xrf_type.startswith('xsp'):
                pass

        irow = 0
        while irow < npts2:
            self.mapper.status = 1
            self.rowdata_ok = True
            irow = irow + 1
            self.dtime.add('======== map row %i ' % irow)
            # print 'ROW ', irow, start1, stop1, step1, dir_offset
            dirx = (dir_offset + irow) % 2
            traj, p1_this, p1_next = [('backward', stop1, start1),
                                      ('foreward', start1, stop1)][dirx]

            if dimension > 1:
                self.mapper.info =  'Row %i / %i (%s)' % (irow,npts2,traj)
            else:
                self.mapper.info =  'Scanning'
            self.mapper.setTime()
            kw['filenumber'] = irow
            kw['scan_pt']    = irow
            if self.state == 'abort':
                self.mapper.message = 'Map aborted before starting!'
                break
            ypos = 0

            self.PV(pos1).put(p1_this, wait=True)
            if dimension > 1:
                self.PV(pos2).put(start2 + (irow-1)*step2, wait=True)
            self.dtime.add('positioners ready %.5f' % p1_this)

            if dimension > 1:
                ypos = self.PV(pos2).get()

            self.mapper.status = 2
            self.dtime.add('before exec traj')
            mt0 = time.time()
            self.ExecuteTrajectory(name=traj, **kw)
            self.mapper.status = 3
            self.dtime.add('after exec traj')

            if dimension > 1:
                self.PV(pos2).put(start2 + irow*step2, wait=False)
            self.PV(pos1).put(p1_next, wait=False)

            # note:
            #  First WriteRowData will write data from XPS and struck,
            #  Then we wait for the XMAP to finish writing its data.
            nxps, nxmap, rowinfo = self.WriteRowData(scan_pt=irow,
                                                     ypos=ypos,
                                                     npts=npts1)
            if irow % 5 == 0:
                self.write('row %i/%i' % (irow, npts2))
            self.dtime.add('xrf data saved')
            if not self.rowdata_ok:
                self.write('Bad data for row: redoing this row')
                irow = irow - 1
                self.PV(pos1).put(p1_this, wait=False)
            else:
                self.MasterFile.write(rowinfo)
                self.MasterFile.flush()

            self.mapper.setTime()
            self.mapper.setNrow(irow)
            if self.state == 'abort':
                self.mapper.message = 'Map aborted!'
                break
            self.check_beam_ok()
            self.dtime.add('row done')
            # self.dtime.show(clear=True)
        # print 'Restore positions..'
        self.restore_positions()
        self.mapper.info = "Finished"
        self.dtime.add('after writing last row')
        self.postscan()
        self.write('done.')
        self.dtime.add('map after postscan')
        # self.dtime.show()
        self.dtime.clear()

    def check_beam_ok(self, timeout=120):
        conf = self.mapconf.get('beam_ok')
        flux_min = float(self.flux_min_pv.get())
        flux_val = float(self.flux_val_pv.get())
        if flux_val > flux_min:
            return

        # if we get here, we'll want to redo the previous row
        self.rowdata_ok = False
        print 'Flux low... checking shutters'
        def shutters_open():
            return all([x.get()==1 for x in self.shutter_status])

        shutter_ok = shutters_open()
        t0 = time.time()
        while not shutter_ok:
            for sh_open in self.shutter_open:
                sh_open.put(1)
                time.sleep(0.50)
            shutter_ok = shutters_open()
            if self.state == 'abort' or time.time() > t0 + timeout:
                return

        # shutters are open.... check flux again,
        # adjust mono if needed.
        time.sleep(2.0)
        flux_min = float(self.flux_min_pv.get())
        flux_val = float(self.flux_val_pv.get())
        if flux_val < flux_min and USE_MONO_CONTROL:
            set_mono_tilt()
        return

    def ExecuteTrajectory(self, name='line', filename='TestMap',
                          scan_pt=1, scantime=10, dimension=1,
                          npulses=11, wait=False, **kw):
        """ run individual trajectory"""
        t0 = time.time()
        if self.use_xrf:
            if self.xrf_type.startswith('xmap'):
                self.xmap.setFileNumber(scan_pt)
                self.xmap.FileCaptureOn()
                time.sleep(0.1)
                self.xmap.EraseStart = 1
                while self.xmap.Acquiring != 1 and time.time()-t0 < 10.0:
                    self.xmap.EraseStart = 1
                    time.sleep(0.1)
                    self.dtime.add('exec: xmap armed? %s' % (repr(1==self.xmap.Acquiring)))
            elif self.xrf_type.startswith('xsp'):
                # complex Acquire On / FileCapture On:
                self.xsp3.setFileNumber(scan_pt)
                self.xsp3.Acquire = 0
                time.sleep(0.1)
                self.xsp3.ERASE  = 1
                self.xsp3.FileCaptureOn()
                time.sleep(0.1)
                self.xsp3.Acquire = 1
                time.sleep(0.10)

        self.struck.start()
        time.sleep(0.10)

        if self.use_xrd:
            self.xrdcam.StartStreaming()

        self.mapper.PV('Abort').put(0)
        self.dtime.add('exec: struck started.')
        self.mapper.setTime()

        # self.write('Ready to start trajectory')
        scan_thread = Thread(target=self.xps.RunLineTrajectory,
                             kwargs=dict(name=name, save=False),
                             name='scannerthread')

        scan_thread.start()

        self.state = 'scanning'
        self.dtime.add('ExecTraj: traj thread begun')
        t0 = time.time()
        if self.use_xrf and not self.ROI_Written:
            xrfdet = self.xmap
            if self.xrf_type.startswith('xsp'):
                xrfdet = self.xsp3
            fout = os.path.join(self.workdir, 'ROI.dat')
            # print(" SAVE ROI.dat to ", fout, xrfdet)
            if True: # try:
                fh = open(fout, 'w')
                fh.write('\n'.join(xrfdet.roi_calib_info()))
                fh.close()
                self.ROI_Written = True
                self.dtime.add('ExecTraj: ROI done')
            else:# except:
                self.dtime.add('ExecTraj: ROI saving failed!!')

        if not self.ENV_Written:
            fout    = os.path.join(self.workdir, 'Environ.dat')
            self.Write_EnvData(filename=fout)
            self.ENV_Written = True
            self.dtime.add('ExecTraj: Env done')

        # now wait for scanning thread to complete
        scan_thread.join()
        beacon_time = time.time()
        while scan_thread.isAlive() and time.time()-t0 < scantime+5.0:
            epics.poll()
            time.sleep(0.002)
            if time.time() - beacon_time > 5.0:
                self.mapper.setTime()
                beacon_time = time.time()
 
        # wait for Xspress3 to finish
        if self.use_xrf and self.xrf_type.startswith('xsp'):
            if self.xsp3.DetectorState_RBV != 0:
                xsp3_ready = False
                count = 0
                while not xsp3_ready:
                    xsp3_ready = (self.xsp3.DetectorState_RBV == 0) or (count > 50)
                    time.sleep(0.1)
                    count = count + 1
                    if count > 10:
                        self.xsp3.Acquire = 0
                        self.xsp3.FileCaptureOff()

        self.dtime.add('ExecTraj: Scan Thread complete.')
        time.sleep(0.05)

    def WriteEscanData(self):
        self.escan_saver.folder = self.workdir
        try:
            new_lines = self.escan_saver.process()
        except:
            new_lines = 0
        if new_lines < 0:
            return

        self.data_fname  = os.path.abspath(os.path.join(nativepath(self.mapper.basedir),
                                                        self.mapper.filename))
        if os.path.isdir(self.data_fname) or '.' not in self.data_fname:
            self.mapper.filename = increment_filename("%s.000" % self.mapper.filename)

            self.data_fname  = os.path.abspath(os.path.join(nativepath(self.mapper.basedir),
                                                            self.mapper.filename))

        if new_lines > 0:
            try:
                f = open(self.data_fname, self.data_mode)
                f.write("%s\n" % '\n'.join(self.escan_saver.buff))
                f.close()
            except IOError:
                self.write('WARNING: Could not write Scan Data to ESCAN Format')
            self.data_mode  = 'a'
        try:
            self.escan_saver.clear()
        except:
            pass

    def WriteRowData(self, filename='TestMap', scan_pt=1, ypos=0, npts=None):
        # NOTE:!!  should return here, write files separately.

        self.struck.stop()
        strk_fname = self.make_filename('struck', scan_pt)
        xps_fname  = self.make_filename('xps', scan_pt)

        if self.use_xrf and self.xrf_type.startswith('xmap'):
            xrf_fname = self.make_filename('xmap', scan_pt)
        elif self.use_xrf and self.xrf_type.startswith('xsp'):
            xrf_fname = self.make_filename('xsp3', scan_pt)

        self.dtime.add('Write Row Data: start %i, ypos=%f ' % (scan_pt,  ypos))

        saver_thread = Thread(target=self.xps.SaveResults, name='saver',
                              args=(xps_fname,))
        saver_thread.start()
        # self.xps.SaveResults(xps_fname)
        nxmap = 0
        self.dtime.add('Write: start xps save thread')

        if self.use_xrf and self.xrf_type.startswith('xmap'):
            xrf_fname = nativepath(self.xmap.getFileNameByIndex(scan_pt))[:-1]
            nxmap = self.Wait_XMAPWrite(irow=scan_pt)
        elif self.use_xrf and self.xrf_type.startswith('xsp'):
            nxmap = self.Wait_Xspress3Write(irow=scan_pt)

        self.dtime.add('Write: xrf data saved')

        wrote_struck = False
        t0 =  time.time()
        counter = 0
        n_sis = -1
        while not wrote_struck and time.time()-t0 < 15.0:
            counter = counter + 1
            try:
                nsmcas, nspts = self.struck.saveMCAdata(fname=strk_fname)
                n_sis = nspts
                wrote_struck = (self.struck.CurrentChannel - nspts) < 2
            except:
                print 'trouble saving struck data.. will retry'
            time.sleep(0.05 + 0.2*counter)
        if not wrote_struck:
            self.rowdata_ok = False
            self.write('Bad data -- Could not SAVE STRUCK DATA!')
        self.dtime.add('Write: struck saved (%i tries)' % counter)

        saver_thread.join()
        self.dtime.add('Write: xps saved')
        rowinfo = self.make_rowinfo(xrf_fname, strk_fname, xps_fname, ypos=ypos)

        if self.use_xrd:
            if not self.xrdcam.FinishStreaming():
                self.write('Bad data: not enough XRD captures: %i' %
                           self.xrdcam.fileGet('NumCaptured_RBV'))
                self.rowdata_ok = False

        n_xps = self.xps.nlines_out
        n_xrf = -1
        if self.use_xrf and self.xrf_type.startswith('xmap'):
            n_xrf = self.xmap.PixelsPerRun
        elif self.use_xrf and self.xrf_type.startswith('xsp'):
            n_xrf = self.xsp3.NumImages_RBV

        sys.stdout.write(ROW_MSG % (scan_pt, n_xps, n_sis, n_xrf))
        sys.stdout.flush()
        self.dtime.add('WriteRowData done: %i, %s' %(self.xps.nlines_out, rowinfo))
        return (self.xps.nlines_out, nxmap, rowinfo)

    def make_filename(self, name, number):
        fout = os.path.join(self.workdir, "%s.%4.4i" % (name,number))
        return  os.path.abspath(fout)

    def make_rowinfo(self, x_fname, s_fname, g_fname, ypos=0):
        x = os.path.split(x_fname)[1]
        s = os.path.split(s_fname)[1]
        g = os.path.split(g_fname)[1]
        dt = time.time() - self.scan_t0
        return '%.4f %s %s %s %9.2f\n' % (ypos, x, s, g, dt)

    def Write_EnvData(self,filename='Environ.dat'):
        fh = open(filename,'w')
        for pvname, title, pv in self.env_pvs:
            val = pv.get(as_string=True)
            fh.write("; %s (%s) = %s \n" % (title,pvname,val))
        fh.close()

    def Connect_ENV_PVs(self):
        self.env_pvs  = []
        envfile = self.mapconf.get('general', 'envfile')
        try:
            f = open(envfile,'r')
            lines = f.readlines()
            f.close()
        except:
            self.write('ENV_FILE: could not read %s' % envfile)
            return
        for line in lines:
            words = line.split(' ', 1)
            pvname =words[0].strip().rstrip()
            if len(pvname) < 2 or pvname.startswith('#'): continue
            title = pvname
            try:
                title = words[1][:-1].strip().rstrip()
            except:
                pass
            if pvname not in self.env_pvs:
                self.env_pvs.append((pvname, title, epics.PV(pvname)))
        return

    def setIdle(self):
        self.state = self.mapper.info = 'idle'
        self.mapper.ClearAbort()
        self.mapper.status = 0
        self.mapper.setTime()        

    def StartScan(self):
        self.dtime.clear()

        self.dtime.add(' set working folder')
        subdir = self.setWorkingDirectory()
        top_path = basepath(self.mapper.basedir)


        self.mapconf.Read(os.path.abspath(self.mapper.scanfile) )
        conf = self.mapconf

        self.use_xrd  = conf.get('xrd_ad', 'use')

        det_path  = os.path.join(top_path, subdir)

        if self.use_xrd:
            filesaver = conf.get('xrd_ad', 'fileplugin')
            prefix = conf.get('xrd_ad', 'prefix')
            self.xrdcam = PerkinElmer_AD(prefix, filesaver=filesaver)
            self.xrdcam.setFilePath(winpath(det_path))

        if self.use_xrf:
            if self.xrf_type.startswith('xmap'):
                self.xmap.setFilePath(winpath(det_path))
                self.xmap.SpectraMode()
                self.xmap.start()
            elif self.xrf_type.startswith('xsp'):
                self.xsp3.setFilePath(det_path)

        self.check_beam_ok()
        self.dtime.add(' read config')
        self.mapper.message = 'preparing scan...'
        self.mapper.info  = 'Starting'
        fname = fix_filename(self.mapper.filename)
        self.mapconf.set_datafilename(fname)
        self.dtime.add('set datafile')

        self.MasterFile = open(os.path.join(self.workdir, 'Master.dat'), 'w')

        self.mapconf.Save(os.path.join(self.workdir, 'Scan.ini'))
        self.dtime.add(' saved scan.ini')
        self.data_mode   = 'w'
        # self.escan_saver = EscanWriter(folder=self.workdir)

        scan = self.mapconf.get('scan')
        scan['scantime'] = scan['time1']
        if scan['dimension'] == 1:
            scan['pos2'] = None
            scan['start2'] = 0
            scan['stop2'] = 0
        scan['filename'] = self.mapper.filename
        # print 'Scan FileName is ', scan['filename']
        # self.dtime.show()
        self.run_scan(**scan)
        self.MasterFile.close()
        self.mapper.message = 'Scan finished: %s' % (scan['filename'])
        self.setIdle()
        # self.dtime.show()

    def mainloop(self):
        self.write('FastMap collector starting up...  %s' % (time.ctime()))
        self.mapper.ClearAbort()
        self.mapper.setTime()
        self.mapper.message = 'Ready to Start Map'
        self.mapper.info = 'Ready'
        self.setIdle()
        epics.poll()
        time.sleep(0.10)
        t0 = time.time()
        self.state = 'idle'

        self.write('FastMap collector ready.')
        while True:
            try:
                epics.poll()
                if time.time()-t0 > 0.2:
                    t0 = time.time()
                    self.mapper.setTime()
                if self.state  == 'start':
                    self.mapper.AbortScan()
                    self.StartScan()
                elif self.state  == 'abort':
                    self.write('Fastmap aborting')
                    self.mapper.ClearAbort()
                    time.sleep(0.5)
                    self.state = 'idle'
                elif self.state  == 'pending':
                    self.write('Fastmap state=pending')
                elif self.state  == 'reboot':
                    self.mapper.info = 'Rebooting'
                    sys.exit()
                elif self.state == 'waiting':
                    self.mapper.ClearAbort()
                elif self.state  != 'idle':
                    self.write('Fastmap: unknown state: %s' % self.state)
                time.sleep(0.01)
            except KeyboardInterrupt:
                break

#if __name__ == '__main__':
#    t = TrajectoryScan()
