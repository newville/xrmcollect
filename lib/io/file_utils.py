#!/usr/bin/python
import time
import os
from string import printable, maketrans
from random import seed, randrange

WIN_BASE = 'T:\\'
UNIX_BASE = '/cars5/Data/'

BAD_FILECHARS = ';~,`!%$@?*#:"/|\'\\\t\r\n (){}[]<>'
BAD_FILETABLE = maketrans(BAD_FILECHARS, '_'*len(BAD_FILECHARS))

def fix_filename(s):
    """fix string to be a 'good' filename.
    This may be a more restrictive than the OS, but
    avoids nasty cases."""
    t = s.translate(BAD_FILETABLE)
    if t.count('.') > 1:
        for i in range(t.count('.') - 1):
            idot = t.find('.')
            t = "%s_%s" % (t[:idot], t[idot+1:])
    return t

def unixpath(d):
    if d.startswith(WIN_BASE):
        d = d.replace(WIN_BASE, UNIX_BASE)

    d = d.replace('\\','/')
    if not d.endswith('/'): d = '%s/' % d        
    return d

def winpath(d):
    if d.startswith('//'): d = d[1:]
    if d.startswith(UNIX_BASE):
        d = d.replace(UNIX_BASE, WIN_BASE)
    d = d.replace('/','\\')
    if not d.endswith('\\'): d = '%s\\' % d            
    return d

def basepath(d):
    if d.startswith(WIN_BASE):
        d = d.replace(WIN_BASE, '')
    if d.startswith(UNIX_BASE):
        d = d.replace(UNIX_BASE, '')
    return nativepath(d)

def nativepath(d):
    if os.name == 'nt':
        return winpath(d)
    return unixpath(d)

def random_string(n):
    """  random_string(n)
    generates a random string of length n, that will match this pattern:
       [a-z][a-z0-9](n-1)
    """
    seed(time.time())
    s = [printable[randrange(0,36)] for i in range(n-1)]
    s.insert(0, printable[randrange(10,36)])
    return ''.join(s)

def pathOf(dir, base, ext, delim='.'):
    p = os.path
    #return p.normpath(p.normcase(p.join(dir,"%s.%s" % (base,ext))))
    return p.normpath(p.join(dir,"%s%s%s" % (base, delim, ext)))

def increment_filename(inpfile,ndigits=3, delim='.'):
    """
    increment a data filename, returning a new (non-existing) filename
 
       first see if a number is before '.'.  if so, increment it.
       second look for number in the prefix. if so, increment it.
       lastly, insert a '_001' before the '.', preserving suffix.

    the numerical part of the file name will contain at least three digits.

    >>> increment_filename('a.002')
    'a.003'
    >>> increment_filename('a.999')
    'a.1000'
    >>> increment_filename('b_017.xrf')
    'b_018.xrf'
    >>> increment_filename('x_10300243.dat')
    'x_10300244.dat'
    
    >>> increment_filename('x.dat')
    'x_001.dat'

    >>> increment_filename('C:/program files/oo/data/x.002')
    'C:/program files/ifeffit/data/x.003'

    >>> increment_filename('a_001.dat')
    'a_002.dat'
    >>> increment_filename('a_6.dat')
    'a_007.dat'
    
    >>> increment_filename('a_001.002')
    'a_001.003'

    >>> increment_filename("path/a.003")
    'path/a.004'
"""

    dirname,  filename = os.path.split(inpfile)
    base, ext = os.path.splitext(filename)
    if ext == '':
        ext = '.000'

    if ext.startswith('.'):
        ext   = ext[1:]
    if ndigits < 3:
        ndigits = 3
    form  = "%%.%ii" % (ndigits)

    def _incr(base, ext):
        try: # first, try incrementing the file extension
            ext = form % (int(ext)+1)
        except ValueError:
            try: #  try incrementing the part of the base after the last '_'
                bparts = base.split('_')
                bparts[-1] = form % (int(bparts[-1])+1)
                base = '_'.join(bparts)
            except:  # last, add a '_001' appendix
                base = "%s_001" % base
        return (base,ext)

    # increment once

    base,ext = _incr(base, ext)
    fout     = pathOf(dirname, base, ext, delim=delim)

    # then gaurantee that file does not exist,
    # continuing to increment if necessary
    while(os.path.exists(fout)):
        base,ext = _incr(base, ext)
        fout     = pathOf(dirname, base, ext, delim=delim)
    return fout

def new_filename(fname=None,ndigits=3):
    """ generate a new file name, either based on
    filename or generating a random one
    
    >>> new_filename(fname='x.001')   
    'x.002'
    # if 'x.001' exists
    """
    if fname is None:
        ext = ("%%.%ii" % ndigits) % 1
        fname = "%s.%s" % (random_string(6), ext)
        
    if os.path.exists(fname):  
        fname = increment_filename(fname,ndigits=ndigits)

    return fname

def new_dirname(dirname=None, ndigits=3):
    """ generate a new subdirectory name (no '.' in name), either
    based on dirname or generating a random one
    
    >>> new_dirname('x.001')   
    'x_002'
    # if 'x_001' exists
    """
    if dirname is None:
        ext = ("%%_%ii" % ndigits) % 1
        dirname = "%s_%s" % (random_string(6), ext)
        
    dirname = dirname.replace('.', '_')
    if os.path.exists(dirname):
        dirname = increment_filename(dirname, ndigits=ndigits, delim='_')
        

    return dirname

if (__name__ == '__main__'):
    test = ( ('a.002', 'a.003'),
             ('a.999', 'a.1000'),
             ('b_017.xrf',  'b_018.xrf'),
             ('x_10300243.dat', 'x_10300244.dat'),
             ('x.dat' , 'x_001.dat'),
             ('C:/program files/data/x.002',
              'C:/program files/data/x.003'),
             ('a_001.dat', 'a_002.dat'),
             ('a_6.dat', 'a_007.dat'),
             ('a_001.002', 'a_001.003'),
             ('path/a.003',  'path/a.004'))
    npass = nfail = 0
    for inp,out in test:
        tval = increment_filename(inp)
        if tval != out:
            print "Error converting " , inp
            print "Got '%s'  expected '%s'" % (tval, out)
            nfail = nfail + 1
        else:
            npass = npass + 1
    print 'Passed %i of %i tests' % (npass, npass+nfail)
