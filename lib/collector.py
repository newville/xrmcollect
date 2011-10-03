import time
import os
import sys
import numpy
import epics
from threading import Thread

from .utils import debugtime, new_filename, nativepath, winpath, fix_filename, increment_filename

from struck import Struck
from xmap import MultiXMAP

from .xps.xps_trajectory import XPSTrajectory
from mapper import mapper

from config import FastMapConfig
from mono_control import mono_control
from escan_writer import EscanWriter

USE_XMAP = True
USE_STRUCK = True
USE_MONO_CONTROL = False # True

# this should go into the configFile, but then again,
# mono_control is highly specialized to a setup.....
MONO_PREFIX = '13IDA:'

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
    npts = 1 + int(0.25 + abs(span/step))
    stop = start + step * (npts-1)
    return (npts,start,stop,step)

class TrajectoryScan(object):
    subdir_fmt = 'scan%4.4i'
    def __init__(self, configfile=None):
        self._pvs = {}
        self.state = 'idle'
        conf = self.mapconf = FastMapConfig(configfile)
        struck      = conf.get('general', 'struck')
        scaler      = conf.get('general', 'scaler')
        xmappv      = conf.get('general', 'xmap')
        basedir     = conf.get('general', 'basedir')
        fileplugin  = conf.get('general', 'fileplugin')
        mapdb       = conf.get('general', 'mapdb')
        self.mapper = mapper(prefix=mapdb)
        self.mono_control = mono_control(MONO_PREFIX)
        self.subdir_index = 0
        self.scan_t0  = time.time()
        self.Connect_ENV_PVs()

        self.ROI_Written = False
        self.ENV_Written = False
        self.ROWS_Written = False
        self.xps = XPSTrajectory(**conf.get('xps'))
        self.dtime = debugtime()
        self.struck = None
        self.xmap = None
        if USE_STRUCK:
            self.struck = Struck(struck, scaler=scaler)
        if USE_XMAP:
            self.xmap = MultiXMAP(xmappv, filesaver=fileplugin)
            self.xmap.SpectraMode()
        self.positioners = {}
        for pname in conf.get('slow_positioners'):
            self.positioners[pname] = self.PV(pname)
        self.mapper.add_callback('Start', self.onStart)
        self.mapper.add_callback('Abort', self.onAbort)
        self.mapper.add_callback('basedir', self.onDirectoryChange)

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
        self.write('=Creating can subfolder: %s / %s' % (os.getcwd(),
                                                         self.mapper.basedir))
        basedir = os.path.abspath(nativepath(self.mapper.basedir))
        try:
            os.chdir(basedir)
        except:
            self.write('Cannot chdir to %s' % basedir)

        fname = fix_filename(self.mapper.filename)
        dirname = fname
        ext = 1
        if '.' in fname:
            dirname, ext = fname.split('.')
        if len(dirname) > 45:
            dirname = dirname[:45]
        subdir = "%s_%s" % (dirname, ext)
        if os.path.exists(subdir):
            i = 0
            while i < 10000:
                i = i + 1
                newdir = "%s_%3.3i" % (dirname, i)
                if not os.path.exists(newdir):
                    subdir = newdir
                    break

        os.mkdir(subdir)
        self.mapper.workdir = subdir
        self.workdir = os.path.abspath(os.path.join(basedir,subdir))

        if USE_XMAP:
            self.xmap.setFilePath(self.workdir)
        self.ROI_Written = False
        self.ENV_Written = False
        self.ROWS_Written = False
        return subdir

    def prescan(self,filename=None,filenumber=1,npulses=11,**kw):
        """ put all pieces (trajectory, struck, xmap) into
        the proper modes for trajectory scan"""
        if USE_STRUCK:
            self.struck.ExternalMode()

        if USE_XMAP:
            # self.xmap.setFileTemplate('%s%s_%4.4d.nc')
            self.xmap.setFileTemplate('%s%s.%4.4d')
            self.xmap.setFileWriteMode(2)
            self.xmap.MCAMode(filename='xmap', # filename,
                              npulses=npulses)
        self.ROI_Written = False
        self.ENV_Written = False
        self.dtime.add('prescan done %s %s' %(repr(USE_STRUCK), repr(USE_XMAP)))

    def postscan(self):
        """ put all pieces (trajectory, struck, xmap) into
        the non-trajectory scan mode"""
        if USE_XMAP:
            self.Wait_XMAPWrite(irow=0)
            time.sleep(0.25)
            self.xmap.SpectraMode()
        self.setIdle()
        if USE_XMAP:
            self.WriteEscanData()
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
        if USE_XMAP:
            # wait for previous netcdf file to be written
            t0 = time.time()
            time.sleep(0.05)
            while not self.xmap.FileWriteComplete():
                self.xmap.finish_pixels()
                time.sleep(0.1)
                if time.time()-t0 > 5:
                    self.mapper.message = 'XMAP File Writing Not Complete!'
                    # self.MasterFile.write('#WARN xmap write failed: row %i\n' % (irow-1))
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

    def mapscan(self, filename='TestMap',scantime=10, accel=1,
                pos1='13XRM:m1',start1=0,stop1=1,step1=0.1, dimension=1,
                pos2=None,start2=0,stop2=1,step2=0.1, **kw):

        self.dtime.clear()

        if pos1 not in self.positioners:
            raise ValueError(' %s is not a trajectory positioner' % pos1)

        self.mapper.status = 1
        npts1,start1,stop1,step1 = fix_range(start1,stop1,step1, addstep=True)
        if pos1 in ('13XRM:m1', '13XRM:m2'):
            start1, stop1 = stop1, start1
        step2_positive =  start2 < stop2
        npts2,start2,stop2,step2 = fix_range(start2,stop2,step2, addstep=False)
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

        if pos2 is None:
            dimension = 1
            npts2 = 1

        self.scan_t0 = time.time()
        self.MasterFile.write('#SCAN started at %s\n' % time.ctime())
        self.MasterFile.write('#SCAN file name = %s\n' % filename)
        self.MasterFile.write('#SCAN dimension = %i\n' % dimension)
        self.MasterFile.write('#SCAN nrows (expected) = %i\n' % npts2)
        self.MasterFile.write('#SCAN time per row (expected) [s] = %.2f\n' % scantime)
        self.MasterFile.write('#Y positioner = %s\n' %  str(pos2))
        self.MasterFile.write('#Y start, stop, step = %f, %f, %f \n' %  (start2, stop2, step2))
        self.MasterFile.write('#------------------------------------\n')
        self.MasterFile.write('# yposition  xmap_file  struck_file  xps_file    time\n')

        kw = dict(scantime=scantime, accel=accel,
                  filename=self.mapper.filename, filenumber=0,
                  dimension=dimension, npulses=npts1, scan_pt=1)

        axis1 = self.mapconf.get('fast_positioners', pos1).lower()
        linescan = dict(start=start1, stop=stop1, step=step1,
                        axis=axis1, scantime=scantime, accel=accel)

        self.xps.DefineLineTrajectories(**linescan)
        self.dtime.add('trajectory defined')

        self.PV(pos1).put(start1, wait=False)

        if dimension > 1:
            self.PV(pos2).put(start2, wait=False)
        self.dtime.add('put positioners to starting positions')

        self.prescan(**kw)

        irow = 0
        while irow < npts2:
            self.mapper.status = 1
            irow = irow + 1
            self.dtime.add('======== map row %i ' % irow)
            dirx = (dir_offset + irow) % 2
            traj, p1_this, p1_next = [('foreward', start1, stop1),
                                      ('backward', stop1, start1)][dirx]
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
            self.ExecuteTrajectory(name=traj, **kw)
            self.mapper.status = 3
            self.dtime.add('after exec traj')

            if dimension > 1:
                self.PV(pos2).put(start2 + irow*step2, wait=False)
            self.PV(pos1).put(p1_next, wait=False)

            if USE_MONO_CONTROL:
                self.mono_control.CheckMonoPitch()

            # note:
            #  First WriteRowData will write data from XPS and struck,
            #  Then we wait for the XMAP to finish writing its data.
            xps_lines, rowinfo = self.WriteRowData(scan_pt=irow,
                                                   ypos=ypos, npts=npts1)
            xmap_fnum = self.Wait_XMAPWrite(irow=irow)
            self.write('Wrote data for row %i' % (irow))
            self.dtime.add('xmap saved')
            if irow > xmap_fnum:
                self.write('Missing XMAP File!')
                irow = irow - 1
            else:
                self.MasterFile.write(rowinfo)
                self.MasterFile.flush()

            self.mapper.setNrow(irow)
            if self.state == 'abort':
                self.mapper.message = 'Map aborted!'
                break
            self.dtime.add('all done')
            # self.dtime.show(clear=True)

        self.restore_positions()
        self.mapper.info = "Finished"
        self.dtime.add('after writing last row')
        self.postscan()
        self.dtime.add('map after postscan')

    def ExecuteTrajectory(self, name='line', filename='TestMap',
                          scan_pt=1, scantime=10, dimension=1,
                          npulses=11, wait=False, **kw):

        if USE_XMAP:
            t0 = time.time()
            self.xmap.setFileNumber(scan_pt)
            self.xmap.FileCaptureOn()
            self.xmap.start()
            self.dtime.add('exec: xmap armed.')

        if USE_STRUCK:
            self.struck.start()

        self.mapper.PV('Abort').put(0)
        self.dtime.add('exec: struck started.')

        if USE_XMAP:
            while self.xmap.Acquiring != 1:
                time.sleep(0.01)
                if time.time() - t0 > 10.0 :
                    break
            self.dtime.add('exec: xmap armed? %s ' % (repr(1==self.xmap.Acquiring)))

        # self.mapper.message = "scanning %s" % name
        self.write('Ready to start trajectory')

        scan_thread = Thread(target=self.xps.RunLineTrajectory,
                             kwargs=dict(name=name, save=False),
                             name='scannerthread')

        scan_thread.start()
        self.state = 'scanning'
        t0 = time.time()
        self.dtime.add('ExecTraj: traj thread begun')

        if USE_XMAP and not self.ROI_Written:
            xmap_prefix = self.mapconf.get('general', 'xmap')
            fout    = os.path.join(self.workdir, 'ROI.dat')
            try:
                fh = open(fout, 'w')
                fh.write('\n'.join(self.xmap.roi_calib_info()))
                fh.close()
                self.ROI_Written = True
                self.dtime.add('ExecTraj: ROI done')
            except:
                self.dtime.add('ExecTraj: ROI saving failed!!')
        if not self.ENV_Written:
            fout    = os.path.join(self.workdir, 'Environ.dat')
            self.Write_EnvData(filename=fout)
            self.ENV_Written = True
            self.dtime.add('ExecTraj: Env done')

        if USE_XMAP:
            self.WriteEscanData()
        # now wait for scanning thread to complete
        scan_thread.join()  # max(0.1, scantime-5.0))

        while scan_thread.isAlive() and time.time()-t0 < scantime+5.0:
            time.sleep(0.002)
        self.dtime.add('ExecTraj: Scan Thread complete.')
        time.sleep(0.002)

    def WriteEscanData(self):
        self.escan_saver.folder =self.workdir
        try:
            new_lines = self.escan_saver.process()
        except:
            # print 'no scan data to process'
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
            # print 'Wrote %i lines to %s ' % (new_lines, self.data_fname)
        try:
            self.escan_saver.clear()
        except:
            pass

    def WriteRowData(self, filename='TestMap', scan_pt=1, ypos=0, npts=None):
        # NOTE:!!  should return here, write files separately.
        strk_fname = self.make_filename('struck', scan_pt)
        xmap_fname = self.make_filename('xmap', scan_pt)
        xps_fname  = self.make_filename('xps', scan_pt)
        self.dtime.add('Write Row Data: start %i, ypos=%f ' % (scan_pt,  ypos))

        saver_thread = Thread(target=self.xps.SaveResults, name='saver',
                              args=(xps_fname,))
        saver_thread.start()
        # self.xps.SaveResults(xps_fname)

        if USE_XMAP:
            xmap_fname = nativepath(self.xmap.getFileNameByIndex(scan_pt))[:-1]

            # print 'would turn off xmap here' # self.xmap.stop()
            # self.xmap.FileCaptureOff()


        if USE_STRUCK:
            self.struck.stop()
            time.sleep(0.1)
            self.struck.saveMCAdata(fname=strk_fname, npts=npts, ignore_prefix='_')
            self.dtime.add('struck saved')

        # wait for saving of gathering file to complete
        saver_thread.join()
        self.dtime.add('xps saved')
        rowinfo = self.make_rowinfo(xmap_fname, strk_fname, xps_fname, ypos=ypos)
        self.dtime.add('Write Row Data: done')
        return (self.xps.nlines_out, rowinfo)

    def make_filename(self, name, number):
        fout = os.path.join(self.workdir, "%s.%4.4i" % (name,number))
        return  os.path.abspath(fout)

    def make_rowinfo(self, x_fname, s_fname, g_fname, ypos=0):
        x = os.path.split(x_fname)[1]
        s = os.path.split(s_fname)[1]
        g = os.path.split(g_fname)[1]
        dt = time.time() - self.scan_t0
        return '%.4f %s %s %s %9.2f\n' % (ypos,x,s,g,dt)

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

    def StartScan(self):
        self.dtime.clear()
        self.setWorkingDirectory()

        self.mapconf.Read(os.path.abspath(self.mapper.scanfile) )
        self.mapper.message = 'preparing scan...'
        self.mapper.info  = 'Starting'

        self.MasterFile = open(os.path.join(self.workdir, 'Master.dat'), 'w')

        self.mapconf.Save(os.path.join(self.workdir, 'Scan.ini'))
        self.data_mode   = 'w'
        self.escan_saver = EscanWriter(folder=self.workdir)

        scan = self.mapconf.get('scan')
        scan['scantime'] = scan['time1']
        if scan['dimension'] == 1:
            scan['pos2'] = None
            scan['start2'] = 0
            scan['stop2'] = 0

        self.mapscan(**scan)
        self.MasterFile.close()
        self.mapper.message = 'Scan finished: %s' % (scan['filename'])
        self.setIdle()
        # self.dtime.show()

    def mainloop(self):
        self.write('FastMap collector starting up....')
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

if __name__ == '__main__':
    t = TrajectoryScan()
    t.mainloop()

