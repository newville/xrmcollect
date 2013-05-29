#!/usr/bin/python
# now using MultipleAxesPVT, not XYLineArc trajectories

#  run_xps [options]
# options:
#  -f     foreward
#  -b     backward
#  -a x   run x axis
#  -a y   run y axis
#  -s     span (in mm)
#  -d     delta, step (in mm)
#  -t     time for full line (in sec)
#
#  run_xps  -a x  -f -s 2.0 -d 0.01  -t 10.0
#
#  scan x foreward 2.0 mm in 10 seconds, with pulses every 0.01 mm.

import sys
import time
import getopt
import ftplib
from cStringIO import StringIO
from string import printable
from XPS_C8_drivers import  XPS

opts, args = getopt.getopt(sys.argv[1:], "fba:s:d:t:", ['fore', 'back', 'axis=','span=','delta=','time='])

traj_name = 'foreward.trj'
axis  = 'th'
span  = 1.00000
delta = 0.002
dwelltime = 5.0

for key, val in opts:
    if key in ('-b','--back'):
        traj_name = 'backward.trj'
    elif key in ('-a', '--axis'):
        if val in ('x', 'y', 'th'):
            axis = val
    elif key in ('-s', '--span'):
        span = float(val)
    elif key in ('-d', '--delta'):
        delta = float(val)
    elif key in ('-t', '--time'):
        dwelltime = float(val)

from epics.devices  import Struck

class config:
    host = '164.54.160.180'
    port = 5001
    timeout = 10
    user = 'Administrator'
    passwd = 'Administrator'
    traj_folder = 'Public/trajectories'
    group_name = 'FINE'
    positioners = ('X', 'Y', 'THETA')
    gather_outputs = [
        'FINE.X.SetpointPosition',
        'FINE.X.CurrentPosition',
        'FINE.Y.SetpointPosition',
        'FINE.Y.CurrentPosition',
        'FINE.THETA.SetpointPosition',
        'FINE.THETA.CurrentPosition',
        ]
    
def Create_LineTraj(axis='x', dwelltime=10, span=1.00, step=0.01):
    """create a PVT trajectory file for a single linear motion
    of length 'span' and time 'dt', with an offset ramp distance of 'step'
    """
    span       = span*1.0
    dwelltime  = abs(dwelltime)

    sign       = span / abs(span)
    line_speed = span / dwelltime

    print '==== Create LineTraj:: '
    print '   Distance = ',  span, sign
    print '   Total time ',  dwelltime
    print '   Line Speed ',  line_speed
    
    max_accel = 10.0
    ramp_time = abs(line_speed / max_accel)
    ramp_dist = line_speed * ramp_time ### * 2

    print '   Ramp time = ', ramp_time , ramp_dist, ' accel = ', max_accel
    
    yramp = ydist = yvelo = 0.00
    tramp = tdist = tvelo = 0.00
    xramp = xdist = xvelo = 0.00
    
    if axis.lower().startswith('y'):
        yramp, ydist, yvelo = ramp_dist, span, line_speed
    elif axis.lower().startswith('t'):
        tramp, tdist, tvelo = ramp_dist, span, line_speed
    else:
        xramp, xdist, xvelo = ramp_dist, span, line_speed
        
    fmt = "%f, %f, %f, %f, %f, %f, %f"
    traj = [fmt % (ramp_time, xramp, xvelo, yramp, yvelo, tramp, tvelo),
            fmt % (dwelltime, xdist, xvelo, ydist, yvelo, tdist, tvelo),
            fmt % (ramp_time, xramp,     0, yramp,     0, tramp,     0)]

    # print  '\n'.join(traj), xd_ramp, yd_ramp
    print '================================'
    
    return '\n'.join(traj), (-xramp, -yramp, -tramp)

def upload_trajectories(axis='x', dwelltime=10, span=1.00, step=0.01):
    kws = dict(axis=axis, dwelltime=dwelltime, step=step)
    f_traj, f_ramps = Create_LineTraj(span=span, **kws)
    b_traj, b_ramps = Create_LineTraj(span=-span, **kws)

    print ': UPLOAD TRAJECTORY::\n', f_traj, '\n  fore_ramps=', f_ramps

    ftpconn = ftplib.FTP()
    ftpconn.connect(config.host)
    ftpconn.login(config.user, config.passwd)
    ftpconn.cwd(config.traj_folder)

    ftpconn.storbinary('STOR foreward.trj', StringIO(f_traj))
    ftpconn.storbinary('STOR backward.trj', StringIO(b_traj))
    ftpconn.close()
    return  f_ramps, b_ramps

def CheckReturn(fname, ret):
    if ret[0] != 0:
        print  'RET: ' , fname, ' -> ', ret

def ReadFTPGathering():
    ftpconn = ftplib.FTP()
    ftpconn.connect(config.host)
    ftpconn.login(config.user, config.passwd)
    ftpconn.cwd('Public')

    output = []
    ftpconn.retrbinary('RETR Gathering.dat', output.append)
    ftpconn.close()
    
    data = ''.join(output)
    cleandata = ''.join(s for s in data if s in printable)
    if len(cleandata) != len(data):
        print '****Warning: possibly corrupt Gathering data! ****'
    return  '\n'.join(cleandata.split('\n')[2:])


def write_xpsfile(fname='xps.dat', titles='', buffer=''):
    f = open(fname, 'w')
    f.write('# %s\n'%  titles)
    f.write(buffer.replace(';','   '))
    f.close()

xps = XPS()
struck = Struck('13IDE:SIS1:', scaler='13IDE:scaler1')

struck.ExternalMode()
struck.Prescale = 1
for m in struck.mcas:
    tmp = m.get('VAL')
    

socketID = xps.TCP_ConnectToServer(config.host, config.port, config.timeout)
xps.Login(socketID, config.user, config.passwd)
        
xps.GroupMotionDisable(socketID, config.group_name)

time.sleep(0.25)
xps.GroupMotionEnable(socketID, config.group_name)

xps.GroupMoveAbsolute(socketID, 'FINE', (0.0, 0.0))

time.sleep(0.05)

#scan ranges       
speed = span / dwelltime

npts = int(1+ span/delta)

steps_per_sec = (npts-1)/dwelltime

dt = 1/steps_per_sec

print 'Scan Range = ', span, 'dwelltime = ', dwelltime, ' step size = ', delta
print 'Time Per Step = ', dt,  '  Steps per second = ', steps_per_sec, ' Npts = ', npts

# struck.Dwell = dt

offset = max(2, int(0.01 +  0.05 * steps_per_sec)) * delta
if axis == 'x':
    delx, dely = span + 2*offset, 0
else:
    dely, delx = span + 2*offset, 0

print 'OFFSET = ', offset, delta, delx, dely


f_ramps, b_ramps = upload_trajectories(axis=axis, dwelltime=dwelltime, span=span, step=delta)

ramps = f_ramps
if traj_name.startswith('back'):
    ramps = b_ramps
print 'MOVE TO -Ramps: ', dt, ramps

xps.GroupMoveRelative(socketID, 'FINE', ramps)

time.sleep(0.1)
ret = xps.GroupStatusGet(socketID, config.group_name)
if ret < 10:
    print 'Motion not ready'
    sys.exit()
if ret > 39:
    time.sleep(0.25)
    ret = xps.GroupStatusGet(socketID, config.group_name)    


ret = xps.GroupStatusGet(socketID, config.group_name)

struck.start()

ret = xps.GatheringReset(socketID)

ret = xps.GatheringConfigurationSet(socketID, config.gather_outputs)
ret = xps.MultipleAxesPVTPulseOutputSet(socketID, config.group_name,  2, 3, dt)

CheckReturn('MultipleAxesPVTPulseOutputSet', ret)

ret = xps.MultipleAxesPVTPulseOutputGet(socketID, config.group_name)

#print 'MultipleAxesPVTPulseOutputGet', ret


ret = xps.MultipleAxesPVTVerification(socketID, config.group_name, traj_name)

#print  ('MultipleAxesPVTVerification', ret)

triggers = ('Always', 'FINE.PVT.TrajectoryPulse',)
ret1 = xps.EventExtendedConfigurationTriggerSet(socketID, triggers, ('0','0'), ('0','0'),('0','0'),('0','0'))
ret2 = xps.EventExtendedConfigurationActionSet(socketID, ('GatheringOneData',), ('0',), ('0',),('0',),('0',))

CheckReturn('EventExtConfTriggerSet', ret1)
CheckReturn('EventExtConfActionSet',  ret2)

time.sleep(0.1)
ret = xps.EventExtendedConfigurationActionGet(socketID)
# print 'Event Action: ', ret

ret = xps.EventExtendedConfigurationTriggerGet(socketID)
# print 'Event Trigger: ', ret

eventID, m = xps.EventExtendedStart(socketID)
ret = xps.EventExtendedAllGet(socketID)

time.sleep(0.1)


ret = xps.MultipleAxesPVTExecution(socketID, config.group_name, traj_name, 1)

CheckReturn('MultipleAxesExecution', ret)

ret = xps.EventExtendedRemove(socketID, eventID)

# CheckReturn('EventExtRemove', ret)

struck.stop()

ret = xps.GatheringStop(socketID)

CheckReturn('GatheringStop', ret)

ret, npulses_out, max_pulses = xps.GatheringCurrentNumberGet(socketID)

# print 'GatheringCurrentNumberGet: ', ret, npulses_out   

ret,  buff = xps.GatheringDataMultipleLinesGet(socketID, 0, npulses_out)
# print 'GatheringDataMultipleLinesGet: ', ret, len(buff)
if ret < 0:
    time.sleep(0.1)
    ret = xps.GatheringStopAndSave(socketID)
    buff =  ReadFTPGathering()
    print 'Read %i lines from FTP Gathering File' % len(buff)
    
write_xpsfile(fname='xps.dat', titles = ' '.join(config.gather_outputs), buffer=buff)

if len(buff) < 50:
    print 'Warning: very short gathering file!!'
# time.sleep(0.01)
# struck.readmca()
time.sleep(0.01)
struck.saveMCAdata(fname='struck.dat',npts=npulses_out, ignore_prefix='_')



