import time
from epics import caget, caput

i0_pv    = '13IDE:IP330_1.VAL'
tilt_pv  = '13IDA:DAC1_7.VAL'
roll_pv  = '13IDA:DAC1_8.VAL'
ts_pv    = '13XRM:edb:arg01'

def find_peak_intensity(ctrl_pv, read_pv, delay_time=0.10, 
                        delta=0.025, npoints=50, minval=0.1):

    """
    find value for ctrl pv (ie, DAC) around current value
    that gives max intensity in read_pv
    """
    
    time.sleep(delay_time)
    
    ctrl_best = ctrl_orig = caget(ctrl_pv)
    read_best = caget(read_pv)
    for i in range(2*npoints):
        cval =  ctrl_orig + (i-npoints) * delta
        caput(ctrl_pv, cval)
        time.sleep(delay_time)
        if i == 0:
            time.sleep(delay_time)
            
        read_val = caget(read_pv)
        if read_val > read_best:
            read_best = read_val
            ctrl_best = cval
    
    if read_best < minval:
        ctrl_best = ctrl_orig
    print 'Peak:  %s=%f at %s=%f' % (read_pv, read_best,
                                     ctrl_pv, ctrl_best)
    
    caput(ctrl_pv, ctrl_best)

def set_mono_tilt(use_bpm=False, do_roll=True):
    # adjust IDE mono tilt and roll DAC to maximize mono pitch
    print 'SET MONO TILT  using bpm = ', use_bpm, ' doing roll = ', do_roll
    tilt_pv = '13IDA:DAC1_7.VAL'
    roll_pv = '13IDA:DAC1_8.VAL'
    i0_pv   = '13IDE:IP330_1.VAL'

    caput('13IDA:efast_pitch_pid.FBON', 0)
    caput('13IDA:efast_roll_pid.FBON', 0)
    if use_bpm:
        i0_pv = '13IDA:QE2:Sum1234:MeanValue_RBV'

    i0_minval = 0.1   # expected smallest I0 Voltage
    dac_delta = 0.025 # min step in DAC Voltage
    npoints   = 100   # max number of DAC steps +/- current position

    # find best tilt value
    tilt_best = tilt_orig = caget(tilt_pv) 
    i0_best = caget(i0_pv)

    for i in range(2*npoints):
        tval =  tilt_orig + (i-npoints) * dac_delta
        caput(tilt_pv, tval)
        time.sleep(0.1)
        i0 = caget(i0_pv)
        if i0 > i0_best:
            i0_best, tilt_best = i0, tval

    if i0_best < i0_minval:
        tilt_best = tilt_orig
    caput(tilt_pv, tilt_best)
    time.sleep(2.0)
    caput('13IDA:QE2:ComputePosOffset12.PROC', 1)
    caput('13IDA:efast_pitch_pid.FBON', 0)
    
    if not do_roll:
        return
    print 'doing roll..'
    roll_best = roll_orig = caget(roll_pv)
    i0_best = caget(i0_pv)
    for i in range(2*npoints):
        tval =  roll_orig + (i-npoints) * dac_delta
        caput(roll_pv, tval)
        time.sleep(0.1)
        i0  = caget(i0_pv)
        if i0 > i0_best:
            i0_best, roll_best = i0, tval
    if i0_best < i0_minval:
        roll_best = roll_orig
    caput(roll_pv, roll_best)
    time.sleep(2.0)
    caput('13IDA:QE2:ComputePosOffset34.PROC', 1)
    caput('13IDA:efast_roll_pid.FBON', 0)


if __name__ == '__main__':
    set_mono_tilt()
