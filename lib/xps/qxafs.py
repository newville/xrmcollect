#!/usr/bin/python
# now using MultipleAxesPVT, not XYLineArc trajectories

msg = '''
  run_qxafs [options] energy1 energy2
 options:
  -t     time for full line (in sec)
  -n     number of pulses

  qxafs  -t 5.0 -n 1001  8000 9000

scans from 8000 eV to 9000 eV in 5 seconds, 1001 pulses
'''

import sys
import time
import getopt
from threading import Thread
import epics
from epics.devices.struck import Struck

from epicscollect.io.file_utils import increment_filename
import ftplib
import numpy as np

from cStringIO import StringIO
from string import printable
from XPS_C8_drivers import  XPS


RAD2DEG = 180/np.pi
HC      = 12398.417
def en2angle(energy, dspace):
    omega   = HC/(2.0 * dspace)
    return RAD2DEG * np.arcsin(omega/energy)


class QXAFS_XPS:
    host = '164.54.160.41'
    port = 5001
    timeout = 1
    username = 'Administrator'
    password = 'Administrator'
    traj_folder = 'Public/Trajectories'
    traj_name = 'qxafs.trj'
    group_name = 'MONO'
    positioners = 'THETA HEIGHT'
    gather_outputs = ['MONO.THETA.SetpointPosition',
                      'MONO.THETA.CurrentPosition',
                      'MONO.THETA.FollowingError',
                      'MONO.THETA.SetpointVelocity',
                      'MONO.THETA.CurrentVelocity',
                      'MONO.THETA.SetpointAcceleration',
                      'MONO.THETA.CurrentAcceleration',
                      'MONO.HEIGHT.CurrentPosition',
                      ]

    def __init__(self, mono_pv='13IDA:m65',
                 energy_pv='13IDE:En:', use_undulator=True):
        self.xps = XPS()
        self.mono =  epics.Motor(mono_pv)
        self.use_undulator = use_undulator
        self.dspace_pv = epics.PV("%sdspace" % energy_pv)
        self.traj = ''
        self.backup_angle = 0 # angle scanned in ramp-up portion of trajectory
        self.connect_xps()
        self.nsegments = 3
        self.struck = Struck('13IDE:SIS1:', scaler='13IDE:scaler1')
        for i in range(8):
            s = self.struck.get('mca%i' % (i+1))

    def connect_xps(self):
        self.sid = self.xps.TCP_ConnectToServer(self.host, self.port, self.timeout)
        self.xps.Login(self.sid, self.username, self.password)
        time.sleep(0.25)
        self.xps.GroupMotionEnable(self.sid, self.group_name)


    def create_trajectory(self, dwelltime=10, span=1.00):
        """create a PVT trajectory file for a single linear motion
        of length 'span' and time 'dt', with an offset ramp distance of 'ramp'
        """

        dwelltime  = abs(dwelltime)
        sign       = span / abs(span)
        line_speed = span / dwelltime

        max_accel = 50.0
        ramp = span / 10.0

        ramp_time  = 1.5 * ramp/line_speed
        ramp_accel = line_speed/ramp_time
        count = 1
        self.traj  = ''
        while abs(ramp_accel) > abs(max_accel):
            ramp_time = ramp_time *  1.5
            ramp      = ramp * 1.5
            ramp_accel = line_speed/ramp_time
            count += 1
            if count > 20:
                print 'Could not compute a valid trajectory!'
                return

        yd_ramp = yd_line = yvelo = 0.00
        xd_ramp, xd_line, xvelo = ramp, span, line_speed
        # ramp_time = ramp_time*1.5
        traj = [
            "%f, %f, %f, %f, %f" % (ramp_time, xd_ramp,     xvelo, yd_ramp,     0),
            "%f, %f, %f, %f, %f" % (dwelltime, xd_line,     xvelo, yd_line, yvelo),
            "%f, %f, %f, %f, %f" % (ramp_time, xd_ramp,         0, yd_ramp,     0),
            ]
        self.traj = traj
        self.backup_angle = xd_ramp


    def create_sinewave_traj(self, period=1.0, npts=100, n=10, yrange=1):
        """create sine wave trajectory:
        arguments
        ----------
        period   time (sec) for 1 oscillation
        n        number of periods
        npts     number of pulses per period
        yrange   amplitude of oscillation
        """
        npulses = n*npts
        i = arange(npulses)

        dt  = period / npts
        amp = yrange /2
        velo = amp * sin(i*2*yamp*dt)
        dist = dt * velo

        self.traj = []
        for d, v in zip(dist, velo):
            self.traj.append("%.6f, %.6f, %.6f, 0, 0" % (dt, d, v))
        self.backup_angle=0

    def read_trajectory_file(self, fname):
        f = open(fname, 'r')
        self.traj = f.readlines()
        dtime = 0
        self.backup_angle = 0
        for line in self.traj:
            vals = [float(w) for w in line.split(',')]
            if self.backup_angle == 0:
                self.backup_angle = vals[1]
            dtime = dtime + vals[0]
        self.dwelltime = dtime
        print 'Read Trajectory ', len(self.traj), self.dwelltime, self.backup_angle
        self.start_angle = self.start_angle - self.backup_angle
        print 'start angle = ', self.start_angle

    def upload_trajectory(self):

        text = StringIO('\n'.join(self.traj))
        self.nsegments = len(self.traj)

        ftpconn = ftplib.FTP()
        ftpconn.connect(self.host)
        ftpconn.login(self.username, self.password)
        ftpconn.cwd(self.traj_folder)
        ftpconn.storbinary('STOR %s' % self.traj_name, text)
        ftpconn.close()
        print 'uploaded trajectories'

    def check_return(self, cmd, ret):
        if ret[0] != 0:
            print  'Command: ' , cmd, ' returned -> ', ret
            raise ValueError

    def read_gathering(self):
        "read XPS gathering"
        self.xps.GatheringStop(self.sid)
        ret, npulses, nx = self.xps.GatheringCurrentNumberGet(self.sid)
        print 'Read XPS Gathering ', ret, npulses, nx

        counter = 0
        while npulses < 1 and counter < 5:
            counter += 1
            time.sleep(1.0)
            ret, npulses, nx = self.xps.GatheringCurrentNumberGet(self.sid)
            print 'Had to do repeat XPS Gathering: ', ret, npulses, nx

        ret, buff = self.xps.GatheringDataMultipleLinesGet(self.sid, 0, npulses)


        if ret < 0:  # gathering too long: need to read in chunks
            print 'Need to read Data in Chunks!!!'  # how many chunks are needed??
            Nchunks = 3
            nx    = int( (npulses-2) / Nchunks)
            ret = 1
            while True:
                time.sleep(0.1)
                ret, xbuff = self.xps.GatheringDataMultipleLinesGet(self.sid, 0, nx)
                if ret == 0:
                    break
                Nchunks = Nchunks + 2
                nx      = int( (npulses-2) / Nchunks)
                if Nchunks > 10:
                    print 'looks like something is wrong with the XPS!'
                    break
            buff = [xbuff]
            for i in range(1, Nchunks):
                ret, xbuff = self.xps.GatheringDataMultipleLinesGet(self.sid, i*nx, nx)
                buff.append(xbuff)
            ret, xbuff = self.xps.GatheringDataMultipleLinesGet(self.sid, Nchunks*nx,
                                                                npulses-Nchunks*nx)
            buff.append(xbuff)
            buff = ''.join(buff)
        print 'READ Gathering ', len(buff)

        self.gather = buff[:]
        for x in ';\r\t':
            self.gather = self.gather.replace(x,' ')


    def save_gathering(self, fname='qxafs_xps'):
        gname = "%s_xps.000" % fname
        sname = "%s_struck.000" % fname
        gname = increment_filename(gname)
        sname = increment_filename(sname)
        f = open(gname, 'w')
        f.write("# QXAFS Data saved %s\n" % time.ctime())
        f.write("#-------------------\n")
        f.write("# %s\n" % ' '.join(self.gather_outputs))
        f.write(self.gather)
        f.close()
        time.sleep(0.1)
        self.struck.saveMCAdata(fname = sname)
        print  'Saved data: ', gname, ' / ', sname
        return gname

    def build_scan(self, energy1, energy2, dtime=2.0, npulses=1001):
        dspace = self.dspace_pv.get()
        a1 = en2angle(energy1, dspace)
        a2 = en2angle(energy2, dspace)
        da = int(100*(a1- a2))/100.0
        self.energy1 = energy1
        self.energy2 = energy2
        self.start_angle = a1
        self.npulses = npulses + 1
        self.dwelltime = dtime
        self.create_trajectory(dwelltime=dtime, span=(a2-a1))
        self.upload_trajectory()
        self.start_angle = a1 - self.backup_angle
        print 'Built Scan OK'

    def onStruckPulse(self, pvname=None, value=0, **kws):
        if value % 25 == 0:
            self.new_id_en = value
            # print 'STruck value ', value

    def prepare_scan(self, npulses=None):
        """ put xps in Ready for Scan mode"""
        self.clear_xps_events()

        if npulses is not None:
            self.npulses = npulses
        if self.use_undulator:
            self.set_undulator_scan()

        dt = self.dwelltime / (self.npulses-1)
        ret = self.xps.GatheringReset(self.sid)
        ret = self.xps.MultipleAxesPVTPulseOutputSet(self.sid,
                              self.group_name,  1, self.nsegments, dt)

        self.check_return('MultipleAxesPVTPulseOutputSet', ret)

        ret = self.xps.MultipleAxesPVTPulseOutputGet(self.sid, self.group_name)
        self.check_return('MultipleAxesPVTPulseOutputGet', ret)

        ret = self.xps.MultipleAxesPVTVerification(self.sid,
                                                   self.group_name, self.traj_name)

        self.check_return('MultipleAxesPVTVerification', ret)

        ret = self.xps.GatheringConfigurationSet(self.sid, self.gather_outputs)

        self.check_return('GatheringConfigurationSet', ret)

        ret = self.xps.GatheringConfigurationGet(self.sid)
        self.check_return('GatheringConfigurationGet', ret)

        triggers = ('Always', 'MONO.PVT.TrajectoryPulse',)
        ret = self.xps.EventExtendedConfigurationTriggerSet(self.sid, triggers,
                    ('0','0'), ('0','0'),('0','0'),('0','0'))

        self.check_return('EventExtConfTriggerSet', ret)

        ret = self.xps.EventExtendedConfigurationActionSet(self.sid,
                   ('GatheringOneData',), ('0',), ('0',),('0',),('0',))

        self.check_return('EventExtConfActionSet',  ret)

        time.sleep(0.1)
        print 'Scan Prepare OK'

    def set_undulator_scan(self):
        self.id_dat = np.loadtxt('Harmonic1.dat').transpose()
        en = self.id_dat[0]
        gap = self.id_dat[1]

        gap1 = np.interp(self.energy1, en, gap)
        gap2 = np.interp(self.energy2, en, gap)

        epics.caput('ID13us:SSStartGap', gap1)
        epics.caput('ID13us:SSEndGap', gap2)
        epics.caput('ID13us:SSTime', self.dwelltime)

    def execute_trajectory(self):
        return self.xps.MultipleAxesPVTExecution(self.sid, self.group_name,
                                                 self.traj_name, 1)

    def clear_xps_events(self):
        """clear any existing events"""
        ret = self.xps.EventExtendedAllGet(self.sid)
        if ret[0] == -83:  # No Events Defined!
            return
        self.check_return('EventExtendedAllGet',  ret)

        for eventID in  ret[1].split(';'):
            ret = self.xps.EventExtendedRemove(self.sid, eventID)
            self.check_return('EventExtRemove', ret)

    def run(self):
        """run traj"""
        print 'Run Trajectory'
        if self.use_undulator:
            id_offset = epics.caget('13IDE:En:id_off')
            id1 = self.energy1/1000.0 + id_offset
            id2 = self.energy2/1000.0 + id_offset
        self.mono.move(self.start_angle)

        if self.use_undulator:
            epics.caput('13IDE:En:id_track', 0)
            epics.caput('ID13us:ScanEnergy', id1)

        struck_pv = epics.PV('13IDE:SIS1:CurrentChannel')
        struck_pv.add_callback(self.onStruckPulse)

        self.mono.move(self.start_angle, wait=True)


        if self.use_undulator:
            epics.caput('ID13us:SyncScanMode', 1)
        print 'Before Trajectory: '
        print '   Angle  = ', epics.caget('13IDA:m65.VAL'), epics.caget('13IDA:m65.DVAL')
        print '   Energy = ', epics.caget('13IDE:En:Energy')
        print '   Und    = ', epics.caget('ID13us:Energy')
        self.struck.scaler.OneShotMode()
        self.struck.ExternalMode()
        self.struck.PresetReal = 0.0

        time.sleep(0.010)

        eventID, m = self.xps.EventExtendedStart(self.sid)
        print 'Event : ', eventID, m

        scan_thread = Thread(target=self.execute_trajectory)

        if self.use_undulator:
            epics.caput('ID13us:SSStart', 1)
            print 'Waiting for Undulator Sync'
            while True:
                if epics.caget('ID13us:SSState') == 2:
                    break
        self.struck.start()
        scan_thread.start()
        print 'ID scanning, started trajectory'

        scan_thread.join()
        print 'Trajectory Thread Done!'


        ret = self.xps.GatheringStop(self.sid)

        self.check_return('GatheringStop', ret)
        self.struck.stop()
        if self.use_undulator:
            epics.caput('ID13us:SyncScanMode', 0)


if __name__ == '__main__':

    opts, args = getopt.getopt(sys.argv[1:], "n:t:", ['npulses=','time='])

    if len(args) < 2:
        print msg
        sys.exit()

    energy1, energy2 = float(args[0]), float(args[1])
    for key, val in opts:
        if key in ('-n', '--npulses'):
            npulses = int(val)
        elif key in ('-t', '--time'):
            dwelltime = float(val)


    q = QXAFS_XPS(mono_pv='13IDA:m65',  energy_pv='13IDE:En:', use_undulator=False)
    q.build_scan(energy1, energy2, dtime=dwelltime, npulses=npulses)
    start_time = time.time()

    print 'Traj: '
    print q.traj
    q.prepare_scan()
    q.run()
    q.read_gathering()
    q.save_gathering()

    print 'Done: time = %.2f sec ' % (time.time()-start_time)

