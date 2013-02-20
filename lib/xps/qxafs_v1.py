#!/usr/bin/python
# now using MultipleAxesPVT, not XYLineArc trajectories

msg = '''
  run_qxafs [options] energy1 energy2
 options:
  -t     time for full line (in sec)
  -n     number of pulses

  qxafs  -t 2.0 -n 1001  8000 9000

scans from 8000 eV to 9000 eV in 2 seconds, 1001 pulses
'''

import sys
import time
import getopt
import epics

from epics.devices.struck import Struck

import ftplib
import numpy as np

from cStringIO import StringIO
from string import printable
from XPS_C8_drivers import  XPS

opts, args = getopt.getopt(sys.argv[1:], "n:t:", ['npulses=','time='])

if len(args) < 2:
    print msg
    sys.exit()

energy1, energy2 = float(args[0]), float(args[1])
print opts

for key, val in opts:
    if key in ('-n', '--npulses'):
        npulses = int(val)
    elif key in ('-t', '--time'):
        dwelltime = float(val)


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
                      'MONO.THETA.CurrentVelocity',                      
                      'MONO.HEIGHT.CurrentPosition',
                      ]
    
    def __init__(self, mono_pv='13IDA:m65',  energy_pv='13IDE:En:'):
        self.xps = XPS()
        self.mono =  epics.Motor(mono_pv)
        self.dspace_pv = epics.PV("%sdspace" % energy_pv)
        self.traj = ''
        self.backup_angle = 0 # angle scanned in ramp-up portion of trajectory
        self.connect_xps()
        self.struck = Struck('13IDE:SIS1:')


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

        max_accel = 5.0
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
        print 'TRAJ:  ', count, line_speed, ramp_time, ramp, ramp_accel

        yd_ramp = yd_line = yvelo = 0.00
        xd_ramp, xd_line, xvelo = ramp, span, line_speed
        # ramp_time = ramp_time*1.5  
        traj = [
            "%f, %f, %f, %f, %f" % (ramp_time, xd_ramp,     xvelo, yd_ramp,     0),
            "%f, %f, %f, %f, %f" % (dwelltime, xd_line,     xvelo, yd_line, yvelo),
            "%f, %f, %f, %f, %f" % (ramp_time, xd_ramp,         0, yd_ramp,     0),
            ]
        self.traj = '\n'.join(traj)
        self.backup_angle = xd_ramp

    def upload_trajectory(self):
        ftpconn = ftplib.FTP()
        ftpconn.connect(self.host)
        ftpconn.login(self.username, self.password)
        ftpconn.cwd(self.traj_folder)

        ftpconn.storbinary('STOR %s' % self.traj_name, StringIO(self.traj))

        ftpconn.close()
        print 'uploaded trajectories'
        print '## qxafs.trj'
        print self.traj
        print '##'
        ftpconn.close()

    def check_return(self, cmd, ret):
        if ret[0] != 0:
            print  'Command: ' , cmd, ' returned -> ', ret
            raise ValueError

    def read_gathering(self):
        "read XPS gathering"
        self.xps.GatheringStop(self.sid)
        ret, npulses, nx = self.xps.GatheringCurrentNumberGet(self.sid)
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

        self.gather = buff[:]
        for x in ';\r\t':
            self.gather = self.gather.replace(x,' ')


    def save_gathering(self, fname='qxafs_xps.dat'):
        f = open(fname, 'w')
        f.write("# QXAFS Data saved %s\n" % time.ctime())
        f.write("#-------------------\n")
        f.write("# %s\n" % ' '.join(self.gather_outputs))
        f.write(self.gather)
        f.close()


    def build_scan(self, energy1, energy2, dtime=2.0, npulses=1001):
        dspace = self.dspace_pv.get()
        print 'DSPACE ', dspace
        a1 = en2angle(energy1, dspace)
        a2 = en2angle(energy2, dspace)
        da = int(100*(a1- a2))/100.0

        span = a2 - a1
        print 'Span = ', span, a1, a2
        self.start_angle = a1
        self.npulses = npulses + 1
        self.dwelltime = dtime
        self.create_trajectory(dwelltime=dtime, span=span)
        self.upload_trajectory()
        self.start_angle = a1 - self.backup_angle
        print 'Starting Angles: ', self.start_angle, a1, a2


    def prepare_scan(self):
        """ put xps in Ready for Scan mode"""
        dt = self.dwelltime / (self.npulses-1)
        print 'DT ', dt
        ret = self.xps.GatheringReset(self.sid)
        ret = self.xps.MultipleAxesPVTPulseOutputSet(self.sid, self.group_name,  1, 4, dt)

        self.check_return('MultipleAxesPVTPulseOutputSet', ret)

        ret = self.xps.MultipleAxesPVTPulseOutputGet(self.sid, self.group_name)
        self.check_return('MultipleAxesPVTPulseOutputGet', ret)

        ret = self.xps.MultipleAxesPVTVerification(self.sid, 
                                                   self.group_name, self.traj_name)

        self.check_return('MultipleAxesPVTVerification', ret)

        ret = self.xps.GatheringConfigurationSet(self.sid, self.gather_outputs)

        self.check_return('GatheringConfigurationSet', ret)

        ret = self.xps.GatheringConfigurationGet(self.sid)


        triggers = ('Always', 'MONO.PVT.TrajectoryPulse',)
        ret = self.xps.EventExtendedConfigurationTriggerSet(self.sid, triggers, 
                    ('0','0'), ('0','0'),('0','0'),('0','0'))

        self.check_return('EventExtConfTriggerSet', ret)

        ret = self.xps.EventExtendedConfigurationActionSet(self.sid, 
                   ('GatheringOneData',), ('0',), ('0',),('0',),('0',))

        self.check_return('EventExtConfActionSet',  ret)

        time.sleep(0.1)
        print 'OK!'

    def run(self):
        """run traj"""
        self.mono.move(self.start_angle, wait=True)
        self.struck.ExternalMode()


        print 'Run Traj:  angle at ', self.start_angle
        time.sleep(2.0)

        self.struck.start()
        self.event_id, m = self.xps.EventExtendedStart(self.sid)
        ret = self.xps.EventExtendedAllGet(self.sid)
        self.check_return('EventExtendedAllGet',  ret)

        print 'EventExtendedStart ', self.event_id, m, ret
        time.sleep(0.1)
        
        ret = self.xps.MultipleAxesPVTExecution(self.sid, self.group_name, self.traj_name, 1)

        self.check_return('MultipleAxesExecution', ret)

        ret = self.xps.EventExtendedRemove(self.sid, self.event_id)
        self.check_return('EventExtRemove', ret)

        ret = self.xps.GatheringStop(self.sid)

        self.check_return('GatheringStop', ret)
        self.struck.stop()
        


# ret = xps.EventExtendedConfigurationActionGet(self.sid)
# print 'Event Action: ', ret

# ret = xps.EventExtendedConfigurationTriggerGet(self.sid)
# print 'Event Trigger: ', ret



q = QXAFS_XPS(mono_pv='13IDA:m65',  energy_pv='13IDE:En:')
q.build_scan(energy1, energy2, dtime=dwelltime, npulses=npulses)
q.prepare_scan()
q.run()
q.read_gathering()
q.save_gathering()

print '##### '
sys.exit()


time.sleep(0.25)

eventID, m = xps.EventExtendedStart(self.sid)
ret = xps.EventExtendedAllGet(self.sid)
print 'EventExtendedStart ', eventID, m, ret
time.sleep(0.1)


ret = xps.MultipleAxesPVTExecution(self.sid, config.group_name, traj_name, 1)

CheckReturn('MultipleAxesExecution', ret)

ret = xps.EventExtendedRemove(self.sid, eventID)

# CheckReturn('EventExtRemove', ret)

# struck.stop()

ret = xps.GatheringStop(self.sid)

CheckReturn('GatheringStop', ret)

ret, npulses_out, max_pulses = xps.GatheringCurrentNumberGet(self.sid)

print 'GatheringCurrentNumberGet: ', ret, npulses_out   

ret,  buff = xps.GatheringDataMultipleLinesGet(self.sid, 0, npulses_out)
print 'GatheringDataMultipleLinesGet: ', ret, len(buff)
if ret < 0:
    time.sleep(0.1)
    ret = xps.GatheringStopAndSave(self.sid)
    buff =  ReadFTPGathering()
    print 'Read %i lines from FTP Gathering File' % len(buff)
    
write_xpsfile(fname='xps.dat', titles = ' '.join(config.gather_outputs), buffer=buff)

if len(buff) < 50:
    print 'Warning: very short gathering file!!'
# struck.saveMCAdata(fname='struck.dat', npts=npulses_out, ignore_prefix='_')



