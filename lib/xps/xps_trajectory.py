import time
import sys
import ftplib
from cStringIO import StringIO
from string import printable
from copy import deepcopy
from ..utils import debugtime
from XPS_C8_drivers import  XPS

##
## used methods for collector.py
##    abortScan, clearabort
##    done ftp_connect
##    done ftp_disconnect
##
## mapscan:   Build (twice!)
## linescan:  Build , clearabort
## ExecTraj;  Execute(),   building<attribute>, executing<attribute>
## WriteTrajData:  Read_FTP(), SaveGatheringData()
##
## need to have env and ROI written during traj scan:
##   use a separate thread for ROI and ENV, allow
##   XY trajectory to block.

class config:
    host    = '164.54.160.180'
    port    = 5001
    timeout = 10
    user    = 'Administrator'
    passwd  = 'Administrator'
    traj_folder   = 'Public/trajectories'
    group_name    = 'FINE'
    positioners   = 'X Y THETA'
    gather_titles = "# XPS Gathering Data\n#--------------"
    gather_outputs =  ('CurrentPosition')

class XPSTrajectory(object):
    """XPS trajectory....
    """
    xylinetraj_text = """FirstTangent = 0
DiscontinuityAngle = 0.01

Line = %f, %f
"""
    pvt_template = """%(ramptime)f, %(xramp)f, %(xvelo)f, %(yramp)f, %(yvelo)f, %(tramp)f, %(tvelo)f
%(scantime)f, %(xdist)f, %(xvelo)f, %(ydist)f, %(yvelo)f, %(tdist)f, %(tvelo)f
%(ramptime)f, %(xramp)f,       0.0, %(yramp)f,       0.0, %(tramp)f,   0.0
"""

    def __init__(self, host=None, user=None, passwd=None,
                 group=None, positioners=None, mode=None, type=None):
        self.host = host or config.host
        self.user = user or config.user
        self.passwd = passwd or config.passwd
        self.group_name = group or config.group_name
        self.positioners = positioners or config.positioners
        self.positioners = tuple(self.positioners.replace(',', ' ').split())

        gout = []
        gtit = []
        for pname in self.positioners:
            for out in config.gather_outputs:
                gout.append('%s.%s.%s' % (self.group_name, pname, out))
                gtit.append('%s.%s' % (pname, out))
        self.gather_outputs = gout
        self.gather_titles  = "%s\n#%s\n" % (config.gather_titles,
                                          "  ".join(gtit))

        # self.gather_titles  = "%s %s\n" % " ".join(gtit)

        self.xps = XPS()
        self.ssid = self.xps.TCP_ConnectToServer(self.host, config.port, config.timeout)
        ret = self.xps.Login(self.ssid, self.user, self.passwd)
        self.trajectories = {}

        self.ftpconn = ftplib.FTP()

        self.nlines_out = 0

        self.xps.GroupMotionDisable(self.ssid, self.group_name)
        time.sleep(0.1)
        self.xps.GroupMotionEnable(self.ssid, self.group_name)

        for i in range(64):
            self.xps.EventExtendedRemove(self.ssid,i)


    def ftp_connect(self):
        self.ftpconn.connect(self.host)
        self.ftpconn.login(self.user,self.passwd)
        self.FTP_connected = True

    def ftp_disconnect(self):
        "close ftp connnection"
        self.ftpconn.close()
        self.FTP_connected=False

    def upload_trajectoryFile(self, fname,  data):
        self.ftp_connect()
        self.ftpconn.cwd(config.traj_folder)
        self.ftpconn.storbinary('STOR %s' %fname, StringIO(data))
        self.ftp_disconnect()
        # print 'Uploaded trajectory ', fname
        # print data

    def DefineLineTrajectories(self, axis='x', start=0, stop=1, accel=10,
                               step=0.001, scantime=10.0, **kws):

        """defines 'forward' and 'backward' trajectories for a line scan
        in PVT Mode"""
        span = (stop - start)*1.0
        sign = span/abs(span)
        scantime = abs(scantime)
        pixeltime = scantime * step / abs(span)
        speed    = span / scantime
        ramptime = abs(speed / accel)
        ramp     = speed * ramptime
        axis =  axis[0].lower()   # 'x', 'y', 't'

        fore_traj = {'scantime':scantime, 'axis':axis, 'accel': accel,
                     'ramptime': ramptime, 'pixeltime': pixeltime}

        for ax in ('x', 'y', 't'):
            for attr in ('start', 'stop', 'step', 'velo', 'ramp', 'dist'):
                fore_traj["%s%s" % (ax, attr)] = 0.

        back_traj = fore_traj.copy()

        if axis == 't':
            fore_traj.update({'tstart': start, 'tstop': stop,  'tstep': step,
                              'tvelo':  speed, 'tramp': ramp,  'tdist': span})
            back_traj.update({'tstart': stop,  'tstop': start, 'tstep': step,
                              'tvelo': -speed, 'tramp':-ramp,  'tdist': -span})
        elif axis == 'y':
            fore_traj.update({'ystart': start, 'ystop': stop,  'ystep': step,
                              'yvelo':  speed, 'yramp': ramp,  'ydist': span})
            back_traj.update({'ystart': stop,  'ystop': start, 'ystep': step,
                              'yvelo': -speed, 'yramp':-ramp,  'ydist': -span})
        elif axis == 'x':
            fore_traj.update({'xstart': start, 'xstop': stop,  'xstep': step,
                              'xvelo':  speed, 'xramp': ramp,  'xdist': span})
            back_traj.update({'xstart': stop,  'xstop': start, 'xstep': step,
                              'xvelo': -speed, 'xramp': -ramp, 'xdist':-span})

        self.trajectories['backward'] = back_traj
        self.trajectories['foreward'] = fore_traj

        ret = False
        try:
            self.upload_trajectoryFile('foreward.trj', self.pvt_template % fore_traj)
            self.upload_trajectoryFile('backward.trj', self.pvt_template % back_traj)
            ret = True
        except:
            pass
        return ret
    

    def RunLineTrajectory(self, name='foreward', verbose=False, save=True,
                          outfile='Gather.dat',  debug=False):
        """run trajectory in PVT mode"""
        traj = self.trajectories.get(name, None)
        if traj is None:
            print 'Cannot find trajectory named %s' %  name
            return

        traj_file = '%s.trj'  % name
        axis = traj['axis']
        dtime = traj['pixeltime']

        ramps = (-traj['xramp'], -traj['yramp'], -traj['tramp'])

        self.xps.GroupMoveRelative(self.ssid, 'FINE', ramps)

        posname = axis.upper()
        if axis == 'x':
            start = traj['xstart']
        elif axis == 'y':
            start = traj['ystart']
        elif axis == 't':
            start = traj['tstart']
            posname = 'THETA'
        else:
            print "Cannot figure out number of pulses for trajectory"
            return -1

        self.gather_outputs = []
        gather_titles = []
        for out in config.gather_outputs:
            self.gather_outputs.append('%s.%s.%s' % (self.group_name, posname, out))
            gather_titles.append('%s.%s' % (posname, out))
        self.gather_titles  = "%s\n#%s\n" % (config.gather_titles,
                                             "  ".join(gather_titles))
           

        self.xps.GatheringReset(self.ssid)
        self.xps.GatheringConfigurationSet(self.ssid, self.gather_outputs)

        ret = self.xps.MultipleAxesPVTPulseOutputSet(self.ssid, config.group_name,
                                                     2, 3, dtime)
        ret = self.xps.MultipleAxesPVTVerification(self.ssid, config.group_name, traj_file)

        buffer = ('Always', 'FINE.PVT.TrajectoryPulse',)
        o = self.xps.EventExtendedConfigurationTriggerSet(self.ssid, buffer,
                                                          ('0','0'), ('0','0'),
                                                          ('0','0'), ('0','0'))

        o = self.xps.EventExtendedConfigurationActionSet(self.ssid,  ('GatheringOneData',),
                                                         ('',), ('',),('',),('',))

        eventID, m = self.xps.EventExtendedStart(self.ssid)

        ret = self.xps.MultipleAxesPVTExecution(self.ssid, self.group_name, traj_file, 1)
        o = self.xps.EventExtendedRemove(self.ssid, eventID)
        o = self.xps.GatheringStop(self.ssid)

        npulses = 0
        if save:
            npulses = self.SaveResults(outfile, verbose=verbose)
        return npulses

    def abortScan(self):
        pass

    def Move(self, xpos=None, ypos=None, tpos=None):
        "move XY positioner to supplied position"
        ret = self.xps.GroupPositionCurrentGet(self.ssid, 'FINE', 3)
        if xpos is None:  xpos = ret[1]
        if ypos is None:  ypos = ret[2]
        if tpos is None:  tpos = ret[3]
        self.xps.GroupMoveAbsolute(self.ssid, 'FINE', (xpos, ypos, tpos))

    def OLD_RunGenericTrajectory(self, name='foreward',
                             pulse_range=1, pulse_step=0.01,
                             speed = 1.0,
                             verbose=False, save=True,
                             outfile='Gather.dat', debug=False):
        traj_file = '%s.trj'  % name
        print 'Run Gen Traj', pulse_range, pulse_step

        self.xps.GatheringReset(self.ssid)
        self.xps.GatheringConfigurationSet(self.ssid, self.gather_outputs)

        ret = self.xps.XYLineArcVerification(self.ssid, self.group_name, traj_file)
        self.xps.XYLineArcPulseOutputSet(self.ssid, self.group_name,  0, pulse_range, pulse_step)

        buffer = ('Always', 'FINE.XYLineArc.TrajectoryPulse',)
        self.xps.EventExtendedConfigurationTriggerSet(self.ssid, buffer,
                                                      ('0','0'), ('0','0'),
                                                      ('0','0'), ('0','0'))

        self.xps.EventExtendedConfigurationActionSet(self.ssid,  ('GatheringOneData',),
                                                     ('',), ('',),('',),('',))

        eventID, m = self.xps.EventExtendedStart(self.ssid)
        print 'Execute',  traj_file, eventID
        ret = self.xps.XYLineArcExecution(self.ssid, self.group_name, traj_file, speed, 1, 1)
        o = self.xps.EventExtendedRemove(self.ssid, eventID)
        o = self.xps.GatheringStop(self.ssid)

        if save:
            npulses = self.SaveResults(outfile, verbose=verbose)
        return npulses

    def SaveResults(self,  fname, verbose=False):
        """read gathering data from XPS
        """
        # self.xps.GatheringStop(self.ssid)
        # db = debugtime()
        ret, npulses, nx = self.xps.GatheringCurrentNumberGet(self.ssid)
        counter = 0
        while npulses < 1 and counter < 5:
            counter += 1
            time.sleep(1.50)
            ret, npulses, nx = self.xps.GatheringCurrentNumberGet(self.ssid)
            print 'Had to do repeat XPS Gathering: ', ret, npulses, nx
            
        # db.add(' Will Save %i pulses , ret=%i ' % (npulses, ret))
        ret, buff = self.xps.GatheringDataMultipleLinesGet(self.ssid, 0, npulses)
        # db.add('MLGet ret=%i, buff_len = %i ' % (ret, len(buff)))

        if ret < 0:  # gathering too long: need to read in chunks
            print 'Need to read Data in Chunks!!!'  # how many chunks are needed??
            Nchunks = 3
            nx    = int( (npulses-2) / Nchunks)
            ret = 1
            while True:
                time.sleep(0.1)
                ret, xbuff = self.xps.GatheringDataMultipleLinesGet(self.ssid, 0, nx)
                if ret == 0:
                    break
                Nchunks = Nchunks + 2
                nx      = int( (npulses-2) / Nchunks)
                if Nchunks > 10:
                    print 'looks like something is wrong with the XPS!'
                    break
            print  ' -- will use %i Chunks for %i Pulses ' % (Nchunks, npulses)
            # db.add(' Will use %i chunks ' % (Nchunks))
            buff = [xbuff]
            for i in range(1, Nchunks):
                ret, xbuff = self.xps.GatheringDataMultipleLinesGet(self.ssid, i*nx, nx)
                buff.append(xbuff)
                db.add('   chunk %i' % (i))
            ret, xbuff = self.xps.GatheringDataMultipleLinesGet(self.ssid, Nchunks*nx,
                                                                npulses-Nchunks*nx)
            buff.append(xbuff)
            buff = ''.join(buff)
            # db.add('   chunk last')

        obuff = buff[:]
        for x in ';\r\t':
            obuff = obuff.replace(x,' ')
        # db.add('  data fixed')
        f = open(fname, 'w')
        f.write(self.gather_titles)
        # db.add('  file open')
        f.write(obuff)
        # db.add('  file write')
        f.close()
        # db.add('  file closed')
        nlines = len(obuff.split('\n')) - 1
        if verbose:
            print 'Wrote %i lines, %i bytes to %s' % (nlines, len(buff), fname)
        self.nlines_out = nlines
        # db.show()
        return npulses


if __name__ == '__main__':
    xps = XPSTrajectory()
    xps.DefineLineTrajectories(axis='x', start=-2., stop=2., scantime=20, step=0.004)
    print xps.trajectories
    xps.Move(-2.0, 0.1, 0)
    time.sleep(0.02)
    xps.RunLineTrajectory(name='foreward', outfile='Out.dat')

