import time
from epics import caget, caput

def find_max_intensity(readpv, drivepv, vals, minval=0.1):
    """find a max in an intensity while sweeping through an
    array of drive values,  around a current position, and
    move to the position with max intensity.

    Parameters
    ----------
    readpv:   PV for reading intensity
    drivepv:  PV for driving positions
    vals:     array of RELATIVE positions (from current value)
    minval:   minimum acceptable intensity [defualt = 0.1]

    Notes:
    -------
     1. PRIVATE method, not exposed in user-macros
     2. if the best intensity is below minval, the position is
        moved back to the original position.
     
    """
    _orig = _best = caget(drivepv)
    i0max = caget(readpv)
    for val in _orig+vals:
        caput(drivepv, val)
        sleep(0.1)
        i0 = caget(readpv)
        if i0 > i0max:
            i0max, _best = i0, val
        #endif
    #endfor
    if i0max < minval: _best = _orig
    caput(drivepv, _best)
    return i0max, _best
#enddef

def set_mono_tilt(enable_fb_roll=True, enable_fb_pitch=False):
    """Adjust IDE monochromator 2nd crystal tilt and roll
    to maximize intensity.

    Parameters
    ----------
    enable_fb_roll:  True (default) or False:
                     enable roll feedback after best position is found.
    enable_fb_pitch: True or False (default):
                     enable pitch feedback after best position is found.

    Notes:
    -------
     This works by
        1. adjusting pitch to maximize intensity at BPM
        2. adjusting roll to maximize intensity at I0 Ion Chamber
        3. adjusting pitch to maximize intensity at I0 Ion Chamber
    """

    print 'Set Mono Tilt June 2015'
    with_roll = True
    tilt_pv = '13IDA:DAC1_7.VAL'
    roll_pv = '13IDA:DAC1_8.VAL'
    i0_pv   = '13IDE:IP330_1.VAL'
    sum_pv  = '13IDA:QE2:SumAll:MeanValue_RBV'

    caput('13XRM:edb:use_fb', 0)
    caput('13IDA:efast_pitch_pid.FBON', 0)
    caput('13IDA:efast_roll_pid.FBON', 0)

    i0_minval = 0.1   # expected smallest I0 Voltage

    # stop, restart Quad Electrometer
    caput('13IDA:QE2:Acquire', 0) ;     sleep(0.25)
    caput('13IDA:QE2:Acquire', 1) ;     sleep(0.25)
    caput('13IDA:QE2:ReadData.PROC', 1)

    # find best tilt value with BPM sum
    out = find_max_intensity(sum_pv, tilt_pv, linspace(-2.5, 2.5, 101))
    print 'Best Pitch (BPM): %.3f at %.3f ' % (out)
    sleep(0.5)

    # find best tilt value with IO
    out = find_max_intensity(i0_pv, tilt_pv, linspace(-1.0, 1.0, 51))
    print 'Best Pitch (I0): %.3f at %.3f ' % (out)
    sleep(0.25)

    # find best roll with I0
    if with_roll:
        print 'doing roll..'
        out = find_max_intensity(i0_pv, roll_pv, linspace(-3.5, 3.5, 141))
        print 'roll first pass ', out
        if out[0] > 0.002:
            out = find_max_intensity(i0_pv, roll_pv, linspace(0.75 -0.75, 76))
        #endif
        print 'Best Roll %.3f at %.3f ' % (out)
        sleep(0.25)
    #endif

    # re-find best tilt value, now using I0
    out = find_max_intensity(i0_pv, tilt_pv, linspace(-1, 1, 51))
    print 'Best Pitch: %.3f at %.3f ' % (out)
    sleep(1.0)
    caput('13IDA:QE2:ComputePosOffsetX.PROC', 1, wait=True)
    caput('13IDA:QE2:ComputePosOffsetY.PROC', 1, wait=True)
    sleep(0.5)
    caput('13IDA:efast_pitch_pid.FBON', 0)
    if enable_fb_roll:
        caput('13IDA:efast_roll_pid.FBON', 1)
    if enable_fb_pitch:
        caput('13XRM:edb:use_fb', 1)
    
#enddef

