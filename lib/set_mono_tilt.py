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

def set_mono_tilt(timeout=3600.0, force=False):
    # adjust IDE mono tilt and roll DAC to maximize mono pitch
    print 'SET MONO TILT Turned Off!!'
    return 
#     last_ts  = caget(ts_pv, as_string=True)
#     try:
#         last_ts = int(last_ts)
#     except ValueError:
#         last_ts = 0
# 
#     if not force and (time.time() - last_ts < timeout):
#         return
#     
#     # find best tilt value, then roll value
#     find_peak_intensity(tilt_pv, i0_pv)
#     find_peak_intensity(roll_pv, i0_pv)
# 
#     caput(ts_pv, "%i"% time.time())

if __name__ == '__main__':
    set_mono_tilt(force=True)
