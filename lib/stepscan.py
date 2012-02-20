#!/usr/bin/python
"""
Classes and Functions for simple step scanning for epics.

This does not used the Epics SScan Record, and the scan is intended to run
as a python application, but many concepts from the Epics SScan Record are
borrowed.  Where appropriate, the difference will be noted here.

A Step Scan consists of the following objects:
   a list of Positioners
   a list of Triggers
   a list of Counters

Each Positioner will have a list (or numpy array) of position values
corresponding to the steps in the scan.  As there is a fixed number of
steps in the scan, the position list for each positioners must have the
same length -- the number of points in the scan.  Note that, unlike the
SScan Record, the list of points (not start, stop, step, npts) must be
given.  Also note that the number of positioners or number of points is not
limited.

A Trigger is simply an Epics PV that will start a particular detector,
usually by having 1 written to its field.  It is assumed that when the
Epics put to the trigger completes, the Counters associated with the
triggered detector will be ready to read.

A Counter is simple a PV whose value should be recorded at every step in
the scan.  Any PV can be a Counter, including waveform records.  For many
detector types, it is possible to build a specialized class that creates
many counters.

In addition to the core components (Positioners, Triggers, Counters),

   breakpoints   a list of scan indices at which to pause and write data
                 collected so far to disk.
   extra_pvs     a list of PVs that are not recorded at each step in the
                 scan, but recorded at the beginning of scan, and at each
                 breakpoint, and to be recorded to disk file.
   pre_scan()    method to run prior to scan.
   post_scan()   method to run after scan.
   at_break()    method to run at each breakpoint.

With all these concepts, a Step Scan ends up being fairly simple, going
roughly (that is, skipping error checking) as:

   run_pre_scan()
   [p.move_to_start() for p in PositionerList]
   record_extra_pvs()
   for i in range(scan_pts):
       [pos.move_to_pos(i) for pos in PositionerList]
       while not all([pos.done for pos in PositionerList]):
           time.sleep(0.001)
       [trig.start() for trig in TriggerList]
       while not all([trig.done for trig in TriggerList]):
           time.sleep(0.001)
       [det.read() for det in CountersList]

       if i in breakpoints:
           write_data()
           record_exrta_pvs()
           run_at_break()
   write_data()
   run_post_scan()

Note that multi-dimensional mesh scans are not explicitly supported, but
that these can be easily emulated with the more flexible mechanism of
unlimited list of points and breakpoints.  Non-mesh scans are possible.

A step scan can have an Epics SScan Record or StepScan database associated
with it.  It will use these for PVs to post data at each point of the scan.

"""
import time
import numpy as np
from epics import PV, caget
from epics.devices import Scaler, Mca, Struck
from utils import OrderedDict

class Positioner(object):
    """a positioner for a scan
This **ONLY** sets an ordinate value for scan, it does *NOT*
do a readback on this position -- add a ScanDetector for that!
    """
    def __init__(self, pvname, label=None, array=None, **kws):
        self._pv = PV(pvname)
        self.label = label
        self.array = array

    def __onComplete(self, pvname=None, **kws):
        self.done = True

    def move_to_start(self, wait=False):
        """ move to starting position"""
        self.move_to_pos(0, wait=wait)

    def move_to_pos(self, i, wait=False, timeout=600):
        """move to i-th position in positioner array"""
        if self.array is None or not self._pv.connected:
            return
        self.done = False
        self._pv.put(self.array[i], callback=self.__onComplete)
        if wait:
            t0 = time.time()
            while not self.done and time.time()-t0<timeout:
                time.sleep(0.001)


class Trigger(object):
    """Detector Trigger for a scan:
The interface is:
    trig = ScanDetectorTrigger(pvname, value=1)
           defines a trigger PV and trigger value

    trig.start(value=None)
          starts the trigger (value will override value set on creation)

    trig.done       True if the start has completed.
    trig.runtime    time for last .start() to complete

Example usage:
    trig = ScanDetectorTrigger(pvname)
    trig.start()
    while not trig.is_done:
        time.sleep(0.01)
    <read detector data>
    """
    def __init__(self, pvname, value=1, label=None, **kws):
        self._pv  = PV(pvname)
        self._val = value
        self.done = False
        self._t0 = 0
        self.runtime = -1

    def __onComplete(self, pvname=None, **kws):
        self.done = True
        self.runtime = time.time() - self._t0

    def start(self, value=None):
        """triggers detector"""
        self.done = False
        self.runtime = -1
        self._t0 = time.time()
        if value is None:
            value = self._val
        self._pv.put(value, callback=self.__onComplete)

class Counter(object):
    """simple scan counter object --
    a value that will be counted at each point in the scan"""
    def __init__(self, pvname, label=''):
        self.pv  = PV(pvname)
        self.label = label
        self.clear()

    def read(self):
        self.buff.append(self.pv.get())

    def clear(self):
        self.buff = []

    def get_buffers(self):
        return {self.label: self.buff}

class DeviceCounter():
    """Generic Multi-PV Counter to be base class for
    MotorCounter, ScalerCounter, MCACounter, etc
    """
    invalid_device_msg = 'DeviceCounter of incorrect type'
    def __init__(self, prefix, rtype=None, fields=None, outpvs=None):
        if prefix.endswith('.VAL'):
            prefix = prefix[-4]
        self.prefix = prefix
        if rtype is not None:
            if not caget("%s.RTYP" % self.prefix) == rtype:
                raise TypeError(invalid_device_msg)
        self.outpvs = outpvs
        self.set_counters(fields)

    def set_counters(self, fields):
        self.counters = OrderedDict()
        if not hasattr(fields, '__iter__'):
            return
        for suf, lab in fields:
            self.counters[lab] = Counter("%s%s" % (self.prefix, suf), label=lab)

    def postvalues(self):
        """post first N counter values to output PVs
        (N being the number of output PVs)

        May want ot override this method....
        """
        if self.outpvs is not None:
            for cname, pv in zip(self.counters, self.outpvs):
                pv.put(self.counters[cname].buff)

    def read(self):
        "read counters"
        for c in self.counters.values():
            c.read()
        self.postvalues()

    def clear(self):
        "clear counters"
        for c in self.counters.values():
            c.clear()

    def get_buffers(self):
        o = OrderedDict()
        for cname in self.counters:
            o[cname] = self.counters[cname].buff
        return o

class MotorCounter(DeviceCounter):
    """Motor Counter: save Readback value
    """
    invalid_device_msg = 'MotorCounter must use a motor'
    def __init__(self, prefix, outpvs=None):
        desc = "%s readback" % caget('%s.DESC' % prefix)
        DeviceCounter.__init__(self, prefix, rtype='motor', outpvs=outpvs)
        fields = [('.RBV', '%s readbback' % caget(self.prefix + '.DESC'))]
        self.set_counters(fields)

class ScalerCounter(DeviceCounter):
    invalid_device_msg = 'ScalerCounter must use a scaler'
    def __init__(self, prefix, outpvs=None, nchan=8,
                 use_calc=False,  use_unlabeled=False):

        DeviceCounter.__init__(self, prefix, rtype='scaler', outpvs=outpvs)
        prefix = self.prefix
        fields = []
        for i in range(1, nnchan+1):
            label = caget('%s.NM%i' % (prefix, i))
            if len(label) > 0 or use_unlabeled:
                suff = '.S%i' % i
                if use_calc:
                    suff = '_calc%i.VAL' % i
                fields.append((suff, label))
        self.set_counters(fields)

class DXPCounter(DeviceCounter):
    """DXP Counter: saves all input and output count rates"""
    _fields = (('.InputCountRate', 'ICR'),
               ('.OutputCountRate', 'OCR'))
    def __init__(self, prefix, outpvs=None):
        DeviceCounter.__init__(self, prefix, rtype=None, outpvs=outpvs)
        prefix = self.prefix
        self.set_counters(self._fields)

class MCACounter():
    """Simple MCA Counter: saves all ROIs (total or net) and, optionally full spectra
    """
    invalid_device_msg = 'MCACounter must use a mca'
    def __init__(self, prefix, outpvs=None, nrois=32,
                 use_net=False,  use_unlabeled=False, use_full=True):
        DeviceCounter.__init__(self, prefix, rtype='mca', outpvs=outpvs)
        prefix = self.prefix
        fields = []
        for i in range(nrois):
            label = caget('%s.R%iNM' % (prefix, i))
            if len(label) > 0 or use_unlabeled:
                suff = '.R%i' % i
                if use_net:
                    suff = '.R%iN' % i
                fields.append((suff, label))
        if use_full:
            fields.append(('.VAL', 'mca spectra'))
        self.set_counters(fields)

class MultiMCACounter():
    invalid_device_msg = 'MCACounter must use a med'
    _dxp_fields = (('.InputCountRate', 'ICR'),
                   ('.OutputCountRate', 'OCR'))
    def __init__(self, prefix, outpvs=None, nmcas=4, nrois=32,
                 use_net=False,  use_unlabeled=False, use_full=True):
        DeviceCounter.__init__(self, prefix, rtype=None, outpvs=outpvs)
        prefix = self.prefix
        fields = []
        for imca in range(1, nmcas+1):
            mcaname = 'mca%i' % i
            dxpname = 'dxp%i' % i
            for i in range(nrois):
                roiname = caget('%s:%s.R%iNM' % (prefix, mcaname, i))
                label = '%s (%s)'% (roiname, mcaname)
                if len(label) > 0 or use_unlabeled:
                    suff = ':%s.R%i' % (mcaname, i)
                if use_net:
                    suff = ':%s.R%iN' %  (mcaname, i)
                fields.append((suff, label))
            # for dsuff, dname in self._dxp_fields:
            #     fields.append()... add dxp
            if use_full:
                fields.append(('.VAL', 'mca spectra (%s)' % mcaname))

class DetectorMixin(object):
    def __init__(self, prefix, **kws):
        self.prefix = prefix
        self.trigger = None
        self.counters = None

    def pre_scan(self, **kws):
        pass

    def post_scan(self, **kws):
        pass

    def at_breakpoint(self, **kws):
        pass

class SimpleDetector(DetectorMixin):
    def __init__(self, prefix):
        Detector.__init__(self, prefix)
        self.trigger = None
        self.counters = [Counter(prefix)]

class ScalerDetector(DetectorMixin):
    def __init__(self, prefix, nchan=8, use_calc=True):
        Detector.__init__(self, prefix)
        self.scaler = Scaler(prefix, nchan=nchan)
        self.trigger = Trigger("%s.CNT" % prefix)
        self.counters = ScalerCounter(prefix, nchan=nchan, use_calc=use_calc)

    def pre_scan(self, **kws):
        self.scaler.OneShot()


class McaDetector(DetectorMixin):
    def __init__(self, prefix, save_spectra=True):
        Detector.__init__(self, prefix)
        self.mca = Mca(prefix)
        self.trigger = Trigger("%sEraseStart" % prefix)
        self.counters = ScalerCounter(prefix, nchan=nchan, use_calc=use_calc)

    def pre_scan(self, **kws):
        self.scaler.OneShot()

class Scan(object):
    def __init__(self, positioners=None, triggers=None, detectors=None):
        print 'scan obj'

