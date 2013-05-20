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
axis  = 'y'
span  = 1.00000
delta = 0.002
dwelltime = 5.0

for key, val in opts:
    if key in ('-b','--back'):
        traj_name = 'backward.trj'
    elif key in ('-a', '--axis'):
        if val in ('x','y'):
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
    positioners = 'X Y THETA'
#    gather_outputs = ['FINE.X.CurrentPosition',
#                       'FINE.X.FollowingError',
#                       'FINE.X.CurrentVelocity',                      
#                       'FINE.X.SetpointAcceleration',                      
#                       'FINE.Y.CurrentPosition',
#                       'FINE.Y.FollowingError',
#                       'FINE.Y.CurrentVelocity',                                            
#                       'FINE.Y.SetpointAcceleration',
#                       ]
    
    gather_outputs = [
        'FINE.X.SetpointPosition',
        'FINE.X.CurrentPosition',
        'FINE.X.CurrentVelocity',
        #'FINE.Y.SetpointPosition',
        #'FINE.Y.CurrentPosition',
        #'FINE.Y.CurrentVelocity',
        #'FINE.THETA.SetpointPosition',
        #'FINE.THETA.CurrentPosition',
        #'FINE.THETA.CurrentVelocity',                      
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
    ramp_dist = line_speed * ramp_time / 2.0

    print '   Ramp time = ', ramp_time , ramp_dist, ' accel = ', max_accel
    
    yd_ramp = yd_line = yvelo = 0.00
    xd_ramp = xd_line = xvelo = 0.00
    
    if axis.lower().startswith('y'):
        yd_ramp, yd_line, yvelo = ramp_dist, span, line_speed
    else:
        xd_ramp, xd_line, xvelo = ramp_dist, span, line_speed
        
    traj = [
        "%f, %f, %f, %f, %f, 0, 0" % (ramp_time, xd_ramp, xvelo, yd_ramp, yvelo),
        "%f, %f, %f, %f, %f, 0, 0" % (dwelltime, xd_line, xvelo, yd_line, yvelo),
        "%f, %f, %f, %f, %f, 0, 0" % (ramp_time, xd_ramp,     0, yd_ramp,     0),
        ]
    # print  '\n'.join(traj), xd_ramp, yd_ramp
    print '================================'
    
    return '\n'.join(traj), xd_ramp, yd_ramp

def upload_trajectories(axis='x', dwelltime=10, span=1.00, step=0.01):
    fore_traj, xdr1, ydr1 = Create_LineTraj(axis=axis, dwelltime=dwelltime, span= span, step=step)
    back_traj, xdr2, ydr2 = Create_LineTraj(axis=axis, dwelltime=dwelltime, span=-span, step=step)

    # print ': UPLOAD TRAJECTORY:: \n ', fore_traj, '\n  x/y dr = ', xdr1, ydr1

    ftpconn = ftplib.FTP()
    ftpconn.connect(config.host)
    ftpconn.login(config.user, config.passwd)
    ftpconn.cwd(config.traj_folder)

    ftpconn.storbinary('STOR foreward.trj', StringIO(fore_traj))
    ftpconn.storbinary('STOR backward.trj', StringIO(back_traj))
    ftpconn.close()
    print 'uploaded trajectories'
    print '##Fore'
    print fore_traj
    #print '##Back'
    #print back_traj
    ftpconn.close()
    print 'Ramps: ', xdr1, xdr2, ydr1, ydr2
    return  xdr1, ydr1


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

struck.Dwell = dt

offset = max(2, int(0.01 +  0.05 * steps_per_sec)) * delta
if axis == 'x':
    delx, dely = span + 2*offset, 0
else:
    dely, delx = span + 2*offset, 0

print 'OFFSET = ', offset, delta, delx, dely


xdr1, ydr1 = upload_trajectories(axis=axis, dwelltime=dwelltime, span=span, step=delta)

print 'MOVE TO -Ramps: ', xdr1, ydr1
## xps.GroupMoveAbsolute(socketID, 'FINE', (-xdr1, -ydr1, 0))
xps.GroupMoveRelative(socketID, 'FINE', (-xdr1, -ydr1, 0))

time.sleep(0.250)

ret = xps.GatheringReset(socketID)

ret = xps.GatheringConfigurationSet(socketID, config.gather_outputs)
ret = xps.MultipleAxesPVTPulseOutputSet(socketID, config.group_name,  2, 2, dt)

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
print 'EventExtendedStart ', eventID, m, ret
time.sleep(0.1)
struck.start()

ret = xps.MultipleAxesPVTExecution(socketID, config.group_name, traj_name, 1)

CheckReturn('MultipleAxesExecution', ret)

ret = xps.EventExtendedRemove(socketID, eventID)

# CheckReturn('EventExtRemove', ret)

struck.stop()

ret = xps.GatheringStop(socketID)

CheckReturn('GatheringStop', ret)

ret, npulses_out, max_pulses = xps.GatheringCurrentNumberGet(socketID)

print 'GatheringCurrentNumberGet: ', ret, npulses_out   

ret,  buff = xps.GatheringDataMultipleLinesGet(socketID, 0, npulses_out)
print 'GatheringDataMultipleLinesGet: ', ret, len(buff)
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



