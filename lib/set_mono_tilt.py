import time
from epics import caget, caput

def find_peak_intensity(ctrl_pv, read_pv, delay_time=0.050, 
                        delta=0.025, npoints=50, minval=0.1):

    # find value for ctrl pv (ie, DAC) that gives
    # around current value that gives max intensity
    # in read_pv
    ctrl_best = ctrl_orig = caget(ctrl_pv)
    read_best = -1  
    for i in range(2*npoints):
        cval =  ctrl_orig + (i-npoints) * delta
        caput(ctrl_pv, cval)
        time.sleep(delay_time)

        read_val = caget(read_pv)
        if read_val > read_best:
            read_best = read_val
            ctrl_best = cval
    
    if read_best < minval:
        ctrl_best = ctrl_orig
    print 'Found Peak:  %s=%f at %s=%f' % (read_pv, read_best,
                                           ctrl_pv, ctrl_best)

    caput(ctrl_pv, ctrl_best)

def set_mono_tilt(timeout=3600.0, force=False):
    # adjust IDE mono tilt and roll DAC to maximize mono pitch
    i0_pv    = '13IDE:IP330_1.VAL'
    tilt_pv  = '13IDA:DAC1_7.VAL'
    roll_pv  = '13IDA:DAC1_8.VAL'
    t0  = time.time()
    last_ts   = caget('13XRM:edb:arg01', as_string=True)
    try:
        last_ts = int(last_ts)
    except ValueError:
        last_ts = 0

    if not force:
        if time.time() - last_ts < timeout:
            return
    
    # find best tilt value
    find_peak_intensity(tilt_pv, i0_pv, 
                        delta=0.025, npoints=50,  minval=0.1)

    # find best roll value
    find_peak_intensity(roll_pv, i0_pv,
                        delta=0.025, npoints=50,  minval=0.1)
    caput('13XRM:edb:arg01', "%i"% time.time())
    
    print ' done.  %.2f sec' % (time.time()-t0)

if __name__ == '__main__':
    set_mono_tilt()
