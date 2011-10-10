"""
utilities for reading files from raw scan folder
"""

from ConfigParser import  ConfigParser

def readASCII(fname, nskip=0, isnumeric=True):
    dat, header = [], []
    for line in open(fname,'r').readlines():
        if line.startswith('#') or line.startswith(';'):
            header.append(line[:-1])
            continue
        if nskip > 0:
            nskip -= 1
            header.append(line[:-1])
            continue
        if isnumeric:
            dat.append([float(x) for x in line[:-1].split()])
        else:
            dat.append(line[:-1].split())
    if isnumeric:
        dat = numpy.array(dat)
    return header, dat

def readMasterFile(fname):
    return readASCII(fname, nskip=0, isnumeric=False)

def readEnvironFile(fname):
    h, d = readASCII(fname, nskip=0, isnumeric=False)
    return h

def readScanConfig(folder):
    sfiles = [os.path.join(folder, 'Scan.ini'),
              os.path.join(folder, 'Scan.cnf')]
    found = False
    for sfile in sfiles:
        if os.path.exists(sfile):
            found = True
            break
    if not found:
        raise IOError('No configuration file found')

    cp =  ConfigParser()
    cp.read(sfile)
    scan = {}
    for a in cp.options('scan'):
        scan[a]  = cp.get('scan',a)
    general = {}
    for a in cp.options('general'):
        general[a]  = cp.get('general',a)
    return scan, general

def readROIFile(hfile):
    cp =  ConfigParser()
    cp.read(hfile)
    output = []
    try:
        rois = cp.options('rois')
    except:
        print 'rois not found'
        return []

    for a in cp.options('rois'):
        if a.lower().startswith('roi'):
            iroi = int(a[3:])
            name, dat = cp.get('rois',a).split('|')
            xdat = [int(i) for i in dat.split()]
            dat = [(xdat[0], xdat[1]), (xdat[2], xdat[3]),
                   (xdat[4], xdat[5]), (xdat[6], xdat[7])]
            output.append((iroi, name.strip(), dat))
    output = sorted(output)
    print 'Read ROI data: %i ROIS ' % len(output)
    return output
