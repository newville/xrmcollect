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
Epics ca.put() to the trigger completes, the Counters associated with the
triggered detector will be ready to read.

A Counter is simple a PV whose value should be recorded at every step in
the scan.  Any PV can be a Counter, including waveform records.  For many
detector types, it is possible to build a specialized class that creates
many counters.

Because Triggers and Counters are closely associated with detectors, a
Detector is also defined, which simply contains a single Trigger and a list
of Counters, and will cover most real use cases.

In addition to the core components (Positioners, Triggers, Counters, Detectors),
a Step Scan contains the following objects:

   breakpoints   a list of scan indices at which to pause and write data
                 collected so far to disk.
   extra_pvs     a list of PVs that are not recorded at each step in the
                 scan, but recorded at the beginning of scan, and at each
                 breakpoint, and to be recorded to disk file.
   pre_scan()    method to run prior to scan.
   post_scan()   method to run after scan.
   at_break()    method to run at each breakpoint.

Note that Postioners and Detectors may add their own pieces into extra_pvs,
pre_scan(), post_scan(), and at_break().

With these concepts, a Step Scan ends up being a fairly simple loop, going
roughly (that is, skipping error checking) as:

   pos = <DEFINE POSITIONER LIST>
   det = <DEFINE DETECTOR LIST>
   run_pre_scan(pos, det)
   [p.move_to_start() for p in pos]
   record_extra_pvs(pos, det)
   for i in range(len(pos[0].array)):
       [p.move_to_pos(i) for p in pos]
       while not all([p.done for p in pos]):
           time.sleep(0.001)
       [trig.start() for trig in det.triggers]
       while not all([trig.done for trig in det.triggers]):
           time.sleep(0.001)
       [det.read() for det in det.counters]

       if i in breakpoints:
           write_data(pos, det)
           record_exrta_pvs(pos, det)
           run_at_break(pos, det)
   write_data(pos, det)
   run_post_scan(pos, det)

Note that multi-dimensional mesh scans over a rectangular grid is not
explicitly supported, but these can be easily emulated with the more
flexible mechanism of unlimited list of positions and breakpoints.
Non-mesh scans are also possible.

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
        self._pv.get()
        self._pv.get_ctrlvars()

        self.label = label
        self.array = array
        self.extra_pvs = []

    def __onComplete(self, pvname=None, **kws):
        self.done = True

    def move_to_start(self, wait=False):
        """ move to starting position"""
        self.move_to_pos(0, wait=wait)

    def verify_array(self):
        """return True if array is within the """
        if array is None:
            return True
        if ((self._pv.upper_ctrl_limit is not None and
             self._pv.upper_ctrl_limit < max(array)) or
            (self._pv.lower_ctrl_limit is not None and
             self._pv.lower_ctrl_limit > min(array))):
            return False
        return True

    def move_to_pos(self, i, wait=False, timeout=600):
        """move to i-th position in positioner array"""
        if self.array is None or not self._pv.connected:
            return
        self.done = False
        self._pv.put(self.array[i], callback=self.__onComplete)
        if wait:
            t0 = time.time()
            while not self.done and time.time()-t0 < timeout:
                time.sleep(1.e-4)

    def pre_scan(self):
        "method to run prior to scan: override for real action"
        pass

    def post_scan(self):
        "method to run after to scan: override for real action"
        pass

    def at_break(self):
        "method to run at break points: override for real action"
        pass


class Trigger(object):
    """Detector Trigger for a scan. The interface is:
    trig = ScanTrigger(pvname, value=1)
           defines a trigger PV and trigger value

    trig.start(value=None)
          starts the trigger (value will override value set on creation)

    trig.done       True if the start has completed.
    trig.runtime    time for last .start() to complete

Example usage:
    trig = ScanTrigger(pvname)
    trig.start()
    while not trig.done:
        time.sleep(1.e-4)
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
    def __init__(self, pvname, label=None):
        self.pv  = PV(pvname)
        if label is None:
            label = pvname
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
        for i in range(1, nchan+1):
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

class MCACounter(DeviceCounter):
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

class MultiMCACounter(DeviceCounter):
    invalid_device_msg = 'MCACounter must use a med'
    _dxp_fields = (('.InputCountRate', 'ICR'),
                   ('.OutputCountRate', 'OCR'))
    def __init__(self, prefix, outpvs=None, nmcas=4, nrois=32,
                 search_all = False,  use_net=False,
                 use_unlabeled=False, use_full=True):
        DeviceCounter.__init__(self, prefix, rtype=None, outpvs=outpvs)
        prefix = self.prefix
        fields = []
        for imca in range(1, nmcas+1):
            mcaname = 'mca%i' % imca
            dxpname = 'dxp%i' % imca
            for i in range(nrois):
                roiname = caget('%s:%s.R%iNM' % (prefix, mcaname, i)).strip()
                roi_hi  = caget('%s:%s.R%iHI' % (prefix, mcaname, i))
                label = '%s (%s)'% (roiname, mcaname)
                if (len(roiname) > 0 and roi_hi > 0) or use_unlabeled:
                    suff = ':%s.R%i' % (mcaname, i)
                    if use_net:
                        suff = ':%s.R%iN' %  (mcaname, i)
                    fields.append((suff, label))
                if roi_hi < 1 and not search_all:
                    break
            # for dsuff, dname in self._dxp_fields:
            #     fields.append()... add dxp
            if use_full:
                fields.append((':%s.VAL' % mcaname, 'mca spectra (%s)' % mcaname))
        self.set_counters(fields)

class DetectorMixin(object):
    trigger_suffix = None
    def __init__(self, prefix, **kws):
        self.prefix = prefix
        self.trigger = None
        if self.trigger_suffix is not None:
            self.trigger = Trigger("%s%s" % (prefix, self.trigger_suffix))
        self.counters = []
        self.extra_pvs = []

    def pre_scan(self, **kws):
        pass

    def post_scan(self, **kws):
        pass

    def at_break(self, **kws):
        pass

class SimpleDetector(DetectorMixin):
    "Simple Detector: a single Counter without a trigger"
    trigger_suffix = None
    def __init__(self, prefix):
        DetectorMixin.__init__(self, prefix)
        self.counters = [Counter(prefix)]

class ScalerDetector(DetectorMixin):
    trigger_suffix = '.CNT'

    def __init__(self, prefix, nchan=8, use_calc=True):
        DetectorMixin.__init__(self, prefix)
        self.scaler = Scaler(prefix, nchan=nchan)
        self.counters = ScalerCounter(prefix, nchan=nchan, use_calc=use_calc)

    def pre_scan(self, **kws):
        self.scaler.OneShotMode()

    def post_scan(self, **kws):
        self.scaler.AutoCountMode()

class McaDetector(DetectorMixin):
    trigger_suffix = 'EraseStart'
    def __init__(self, prefix, save_spectra=True):
        DetectorMixin.__init__(self, prefix)
        self.mca = Mca(prefix)
        self.trigger = Trigger("%sEraseStart" % prefix)
        self.counters = ScalerCounter(prefix, nchan=nchan, use_calc=use_calc)

    def pre_scan(self, **kws):
        pass

class StepScan(object):
    def __init__(self):
        self.pos_settle_time = 0
        self.pos_maxmove_time = 3600.0
        self.det_settle_time = 0
        self.det_maxcount_time = 86400.0
        self.extra_pvs = []
        self.positioners = []
        self.triggers = []
        self.counters = []
        self.breakpoints = []
        self.at_break_methods = []
        self.pre_scan_methods = []
        self.post_scan_methods = []
        self.verified = False

    def add_counter(self, counter, label=None):
        "add simple counter"
        if isinstance(counter, str):
            counter = Counter(counter, label)
        if isinstance(counter, Counter):
            self.counters.append(counter)
        else:
            print( 'Cannot add Counter? ', counter)
        self.verified = False

    def add_extra_pvs(self, pvs):
        """add extra pvs (list of pv names)"""
        if isinstance(pvs, str):
            self.extra_pvs.append(PV(pvs))
        else:
            self.extra_pvs.extend([PV(p) for p in pvs])

    def add_positioner(self, pos):
        """ add a Positioner """
        self.extra_pvs.extend(pos.extra_pvs)
        self.at_break_methods.extend(pos.at_break)
        self.post_scan_methods.extend(pos.post_scan)
        self.pre_scan_methods.extend(pos.pre_scan)
        self.verified = False

    def add_detector(self, det):
        """ add a Detector -- needs to be derived from Detector_Mixin"""
        self.extra_pvs.extend(det.extra_pvs)
        self.triggers.append(det.trigger)
        self.counters.extend(det.counters)
        self.at_break_methods.extend(det.at_break)
        self.post_scan_methods.extend(det.post_scan)
        self.pre_scan_methods.extend(det.pre_scan)
        self.verified = False

    def at_break(self, breakpoint=0):
        out = [m() for m in self.at_break_methods]
        self.read_extra_pvs()
        self.write_data(breakpoint=breakpoint)

    def pre_scan(self):
        return [m() for m in self.pre_scan_methods]

    def post_scan(self):
        return [m() for m in self.pre_scan_methods]

    def verify_scan(self):
        """ this does some simple checks of Scans, checking that
    the length of the positions array matches the length of the
    positioners array.

    For each Positioner, the max and min position is checked against
    the HLM and LLM field (if available)
    """
        npts = None
        self.error_message = ''
        for p in self.positioners:
            if not p.verify_array():
                self.error_message = 'Positioner %s array out of bounds' % p._pv.pvname
                return False
            if npts is None:
                npts = len(p.array)
            if len(p.array) != npts:
                self.error_message = 'Inconsistent positioner array length'
                return False
        return True

    def run(self):
        print 'run scan!'
        if not self.verify_scan():
            print 'Cannot execute scan -- out of bounds'
            return
        out = self.pre_scan()
        self.checkout_outputs(out)

        out = [p.move_to_start() for p in self.positions]
        self.checkout_outputs(out)

        self.open_datafile()
        self.read_extra_pvs()
        self.write_data(breakpoint=0)

        print 'len of positioner arrays ' , self.positioners[0].array
        npts = self.positioners[0].array
        for i in range(npts):
            [p.move_to_pos(i) for p in self.positioners]
        self.t0 = time.time()
        while (not all([p.done for p in pos]) and
               time.time() - self.t0 < self.pos_maxmove_time):
            time.sleep(0.001)
        time.sleep(self.pos_settle_time)
        [trig.start() for trig in self.triggers]
        self.t0 = time.time()
        while (not all([trig.done for trig in det.triggers]) and
               time.time() - self.t0 < self.det_maxcount_time):
               time.sleep(0.001)
        time.sleep(self.det_settle_time)
        [c.read() for c in self.counters]
        if i in breakpoints:
            self.at_break()

    self.write_data(closefile=True)

    def write_data(self, breakpoint=0, closefile=False):
        print 'write data!'
