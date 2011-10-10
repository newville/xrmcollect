#!/usr/bin/python

import os
import sys
import copy
import time
import gc


try:
    import numpy 
except ImportError:
    print "Error: Escan_data can't load numpy"
    sys.exit(1)

try:
    import h5py
    has_h5 = True
except ImportError:
    print "Warning HDF5 not available"
    has_h5 = False

has_h5 = False
def _cleanfile(x):
    for o in ' ./?(){}[]",&%^#@$': x = x.replace(o,'_')
    return x

class escan_data:
    """ Epics Scan Data """
    mode_names = ('2d', 'epics scan',
                  'user titles', 'pv list',
                  '-----','=====','n_points',
                  'scan began at', 'scan ended at',
                  'column labels', 'scan regions','data')
    
    h5_attrs = {'Version': '1.0.0',
                'Title': 'Epics Scan Data',
                'Beamline': 'GSECARS, 13-IDC / APS'}

    def __init__(self,file=None,**args):
        self.filename    = file
        self.clear_data()

        self.progress    = None
        self.message     = self.message_printer

        for k in args.keys():
            if (k == 'progress'): self.progress = args[k]
            if (k == 'message'):  self.message  = args[k]


        if self.filename not in ('',None):
            self.read_data_file(fname=self.filename)
        
    def clear_data(self):
        self.xdesc       = ''
        self.ydesc       = ''
        self.xaddr       = ''
        self.yaddr       = ''
        self.start_time  = ''
        self.stop_time   = ''
        self.dimension   = 1
        self.scan_prefix = ''
        self.user_titles = []
        self.scan_regions= []


        self.env_desc    = []
        self.env_addr    = []
        self.env_val     = []

        self.pos         = []
        self.det         = []

        self.pos_desc    = []
        self.pos_addr    = []
        self.det_desc    = []
        self.det_addr    = []

        self.sums       = []
        self.sums_names = []
        self.sums_list  = []
        self.dt_factor       = None

        self.has_fullxrf    = False
        self.xrf_data     = []
        self.xrf_sum      = []
        self.xrf_energies = []
        self.xrf_header = ''
        self.xrf_dict   = {}
        self.xrf_merge  = None
        self.xrf_merge_corr  = None
        self.roi_names  = []
        self.roi_llim   = []
        self.roi_hlim  = []
        
        self.x = numpy.array(0)
        self.y = numpy.array(0)
        gc.collect()
        

    def message_printer(self,s,val):
        sys.stdout.write("%s\n" % val)

    def my_progress(self,val):
        sys.stdout.write("%f .. " % val)
        sys.stdout.flush()
            
    def filetype(self,fname=None):
        """ checks file type of file, returning:
        'escan'  for  Epics Scan
        None     otherwise
        """
        try:
            u = open(fname,'r')
            t = u.readline()
            u.close()
            if 'Epics Scan' in t: return 'escan'

        except IOError:
            pass

        return None

    def get_map(self,name=None,norm=None):
        return self.get_data(name=name,norm=norm)

    def get_data(self,name=None,norm=None,correct=True):
        """return data array by name"""
        dat = self._getarray(name,correct=correct)
        if dat is None: return data
        if norm is not None:
            norm = self._getarray(norm,correct=True)
            dat  = dat/norm            
        return dat
    
    def match_detector_name(self, str, strict=False):
        """return index in self.det_desc most closely matching supplied string"""
        s  = str.lower()
        sw = s.split()
        b  = [i.lower() for i in self.det_desc]
        # look for exact match
        for i in b:
            if (s == i):  return b.index(i)
        
        # look for inexact match 1: compare 1st words
        for i in b:
            sx = i.split()
            if (sw[0] == sx[0]):   return b.index(i)

        # check for 1st word in the det name
        if not strict:
            for i in b:
                j = i.find(sw[0])
                if (j >= 0):  return b.index(i)
        # found no matches
        return -1

    def ShowProgress(self,val,row=-1):
        if (self.progress != None):
            self.progress(val)
        elif (row>-1):
            print " %3i " % (row),
            if (row %10 == 0): print ""

    def ShowMessage(self,val,state='state'):
        if (self.message != None):
            self.message(state,val)

    def PrintMessage(self,s):
        sys.stdout.write(s)
        sys.stdout.flush()
        

    def read_data_file(self, fname=None, use_h5=True):
        """generic data file reader"""
        if fname is None: fname = self.filename
        if fname.endswith('.h5'):
            self.filename = fname = fname[:-3]
        h5name = "%s.h5" % fname
        read_ascii = True
        # print 'read_Data_file: ', has_h5, use_h5, os.path.exists(h5name)
        if has_h5 and use_h5 and os.path.exists(h5name):
            try:
                mtime_ascii = os.stat(fname)[8]
            except:
                mtime_ascii = 0
            mtime_h5    = os.stat(h5name)[8]
            if  (mtime_h5 >=  mtime_ascii):
                retval = self.read_h5file(h5name)
                if retval is None:
                    msg = "file %s read OK" % h5name
                    self.ShowMessage(msg)
                    read_ascii = False
            else:
                print 'h5 exists, ascii is newer!'

        if read_ascii:
            retval = self.read_ascii(fname=fname)
            if retval is None:
                msg = "file %s read OK" % fname
            else:
                msg = "problem reading file %s" % fname
            self.ShowMessage(msg)
            if has_h5 and retval is None:
                if True: #try:
                    self.write_h5file(h5name)
                    x = h5name
                else: # except:
                    if os.path.exists(h5name):
                        print 'should removing %s due to error.' % h5name
                        # os.unlink(h5name)
            gc.collect()
        return retval

    def write_h5file(self,h5name):
        try:
            fh = h5py.File(h5name, 'w')
            print 'saving hdf5 file %s' %h5name
        except:
            print 'write_h5file error??? ', h5name

        def add_group(group,name,dat=None,attrs=None):
            g = group.create_group(name)
            if isinstance(dat,dict):
                for key,val in dat.items():
                    g[key] = val
            if isinstance(attrs,dict):
                for key,val in attrs.items():
                    g.attrs[key] = val
            return g

        def add_data(group,name,data, attrs=None, **kws):
            # print 'create group in HDF file ', name
            kwargs = {'compression':4}
            kwargs.update(kws)
            d = group.create_dataset(name, data=data, **kwargs)
            if isinstance(attrs,dict):
                for key,val in attrs.items():
                    d.attrs[key] = val

            return d


        mainattrs = copy.deepcopy(self.h5_attrs)
        mainattrs.update({'Collection Time': self.start_time})
        
        maingroup = add_group(fh,'data', attrs=mainattrs)

        g = add_group(maingroup,'environ')
        add_data(g,'desc',self.env_desc)
        add_data(g,'addr',self.env_addr)
        add_data(g,'val', self.env_val)

        scan_attrs = {'dimension':self.dimension,
                      'stop_time':self.stop_time,
                      'start_time':self.start_time,
                      'scan_prefix':self.scan_prefix,
                      'correct_deadtime': repr(self.correct_deadtime)}

        scangroup = add_group(maingroup,'scan', attrs=scan_attrs)

        scan_data = ['det', 'pos', 'sums','sums_list', 'sums_names',
                     'scan_regions', 'user_titles', 'realtime',
                     'pos_desc', 'pos_addr', 'det_desc', 'det_addr']
        
        for attr in scan_data:
            d = getattr(self,attr)
            if len(d)== 0 and isinstance(d,list): d.append('')
            add_data(scangroup,attr,d)

        if self.correct_deadtime:
            add_data(scangroup,'dt_factor', self.dt_factor)
            add_data(scangroup,'det_corrected', self.det_corr)
            add_data(scangroup,'sums_corrected', self.sums_corr)


        add_data(scangroup,'x', self.x, attrs={'desc':self.xdesc, 'addr':self.xaddr})
        
        if self.dimension  > 1:
            add_data(scangroup,'y', self.y, attrs={'desc':self.ydesc, 'addr':self.yaddr})            

        if self.has_fullxrf:
            en_attrs = {'units':'keV'}
            
            xrf_shape = self.xrf_data.shape
            gattrs = {'dimension':self.dimension,'nmca':xrf_shape[-1]}

            g = add_group(maingroup,'merged_xrf',attrs=gattrs)
            add_data(g, 'data', self.xrf_merge)
            add_data(g, 'data_corrected', self.xrf_merge_corr)
            add_data(g, 'energies',  self.xrf_energies[0,:], attrs=en_attrs)



            gattrs.update({'ndetectors':xrf_shape[-2]})
            g = add_group(maingroup,'full_xrf',attrs=gattrs)
            add_data(g, 'header', self.xrf_header)
            add_data(g, 'data',   self.xrf_data)
            add_data(g, 'energies', self.xrf_energies, attrs=en_attrs)

            add_data(g, 'roi_labels',  self.roi_names)
            add_data(g, 'roi_lo_limit',self.roi_llim)
            add_data(g, 'roi_hi_limit',self.roi_hlim)
            
        fh.close()
        return None

        
    def read_h5file(self,h5name):
        fh = h5py.File(h5name,'r')
        root = fh['data']

        isValid = False
        attrs  = root.attrs
        try:
            version = root.attrs['Version']
            title   = root.attrs['Title']
            if title  != 'Epics Scan Data': raise KeyError
            beamline = root.attrs['Beamline']
            isValid = True
        except KeyError:
            isValid = False
        if not isValid:
            raise

        g = root['environ']
        self.env_desc    = g['desc'].value
        self.env_addr    = g['addr'].value
        self.env_val     = g['val'].value

        
        g = root['scan']
        self.stop_time  = g.attrs['stop_time']
        self.start_time = g.attrs['start_time']
        self.dimension  = g.attrs['dimension']
        self.correct_deadtime = g.attrs['correct_deadtime'] == 'True'

        self.x     = g['x'].value
        self.xdesc = g['x'].attrs['desc']
        self.xaddr = g['x'].attrs['addr']
        self.y     = []
        self.ydesc  = ''
        if self.dimension > 1:
            self.y = g['y'].value
            self.ydesc = g['y'].attrs['desc']
            self.yaddr = g['y'].attrs['addr']


        for attr in ['det', 'pos', 'sums', 'sums_list', 'sums_names',
                     'pos_desc', 'pos_addr', 'det_desc', 'det_addr',
                     'scan_regions', 'user_titles']:
            setattr(self,attr,  g[attr].value)

        if self.correct_deadtime:
            setattr(self, 'dt_factor', g['dt_factor'].value)
            setattr(self, 'det_corr',  g['det_corrected'].value)
            setattr(self, 'sums_corr', g['sums_corrected'].value)

            
        self.has_fullxrf = 'full_xrf' in root.keys()
        if self.has_fullxrf:
            g = root['merged_xrf']
            self.xrf_merge = g['data'].value
            self.xrf_merge_corr = g['data_corrected'].value

            g = root['full_xrf']
            self.xrf_header = g['header'].value
            self.xrf_energies = g['energies'].value
            self.xrf_data = g['data'] .value

            self.roi_names = g['roi_labels'].value
            self.roi_llim  = g['roi_lo_limit'].value
            self.roi_hlim  = g['roi_hi_limit'].value

        fh.close()
        return None
        
    def _getarray(self, name=None, correct=True):
        i = None
        arr = None
        if name in self.sums_names:
            i = list(self.sums_names).index(name)
            arr = self.sums
            if correct:
                arr = self.sums_corr
        else:
            i = self.match_detector_name(name)
            arr = self.det
            if correct:
                arr = self.det_corr
        if i is not None:
            return arr[i]
        
        return None
        
                
    def _open_ascii(self,fname=None):
        """open ascii file, return lines after some checking"""
        if fname is None: fname = self.filename
        if fname is None: return None

        self.ShowProgress(1.0)
        #  self.ShowMessage("opening file %s  ... " % fname)
        try:
            f = open(fname,'r')
            lines = f.readlines()
            lines.reverse()
            f.close()
        except:
            self.ShowMessage("ERROR: general error reading file %s " % fname)
            return None

        line1    = lines.pop()
        if 'Epics Scan' not in line1:
            self.ShowMessage("Error: %s is not an Epics Scan file" % fname)
            return None
        return lines
        
    def _getline(self,lines):
        "return mode keyword,"
        inp = lines.pop()
        is_comment = True
        mode = None
        if len(inp) > 2:
            is_comment = inp[0] in (';','#')
            s   = inp[1:].strip().lower()
            for j in self.mode_names:
                if s.startswith(j):
                    mode = j
                    break
            if mode is None and not is_comment:
                w1 = inp.strip().split()[0]
                try:
                    x = float(w1)
                    mode = 'data'
                except ValueError:
                    pass
        return (mode, inp)
        

    def _make_arrays(self, tmp_dat, col_legend, col_details):
        # convert tmp_dat to numpy 2d array
        dat = numpy.array(tmp_dat).transpose()
        
        # make raw position and detector data, using column labels
        npos = len( [i for i in col_legend if i.lower().startswith('p')])
        ndet = len( [i for i in col_legend if i.lower().startswith('d')])

        self.pos  = dat[0:npos,:]
        self.det  = dat[npos:,:]

        # parse detector labels
        for i in col_details:
            try:
                key,detail = i.split('=')
            except:
                break
            label,pvname = [i.strip() for i in detail.split('-->')]
            label = label[1:-1]
            if key.startswith('P'):
                self.pos_desc.append(label)
                self.pos_addr.append(pvname)
            else:
                self.det_desc.append(label)
                self.det_addr.append(pvname)
                
                

        # make sums of detectors with same name and isolate icr / ocr
        self.sums       = []
        self.sums_names = []
        self.sums_list  = []
        self.dt_factor       = None
        icr,ocr = [],[]
        isum = -1
        for i, det in enumerate(self.det_desc):
            thisname, thispv = det, self.det_addr[i]
            if 'mca' in thisname and ':' in thisname:
                thisname = thisname.replace('mca','').split(':')[1].strip()
            
            if thisname.endswith('InputCountRate'):
                icr.append(self.det[i][:])
            elif thisname.endswith('OutputCountRate'):
                ocr.append(self.det[i][:])
            elif thisname not in self.sums_names:
                self.sums_names.append(thisname)
                isum  = isum + 1
                self.sums.append( 1.0*self.det[i] )
                self.sums_list.append([i])
            else:
                self.sums[isum] = self.sums[isum][:] + self.det[i][:]

                self.sums_list[isum].append(i)

        self.sums = numpy.array(self.sums)
        # if icr/ocr data is included, pop them from
        # the detector lists.
        self.dt_factor = None
        self.correct_deadtime = False


        if len(icr)>0 and len(ocr)==len(icr):
            self.dt_factor  = numpy.array(icr)/numpy.array(ocr)
            n_icr     = self.dt_factor.shape[0]
            self.det  = self.det[0:-2*n_icr]
            self.det_desc   = self.det_desc[:-2*n_icr]
            self.det_addr   = self.det_addr[:-2*n_icr]
            self.correct_deadtime = True

        if self.dimension == 2:
            ny = len(self.y)
            nx = len(tmp_dat)/ny

            self.det.shape  = (self.det.shape[0],  ny, nx)
            self.pos.shape  = (self.pos.shape[0],  ny, nx)
            self.sums.shape = (self.sums.shape[0], ny, nx)
            if self.dt_factor is not None:
                self.dt_factor.shape = (self.dt_factor.shape[0], ny, nx)
           
            self.x = self.pos[0,0,:]
            self.realtime = self.pos[2,:,:]
        else:
            self.x        = self.pos[0]
            self.realtime = self.pos[2]            
            nx = len(self.x)
            self.y = []

        tnsums = [len(i) for i in self.sums_list]
        tnsums.sort()
        nsums = tnsums[-1]
        for s in self.sums_list:
            while len(s) < nsums:  s.append(-1)

        # finally, icr/ocr corrected sums
        self.det_corr  = 1.0 * self.det[:]
        self.sums_corr = 1.0 * self.sums[:]
        
        # print 'DT Sums ' , self.det.shape, self.sums.shape
        # print 'DT Factor ', self.dt_factor.shape
        if self.correct_deadtime:
            idet = -1
            for label, pvname in zip(self.det_desc,self.det_addr):
                idet = idet + 1
                if 'mca' in pvname:
                    nmca = int(pvname.split('mca')[1].split('.')[0]) -1
                    self.det_corr[idet,:] = self.det_corr[idet,:] * self.dt_factor[nmca,:]
            
            isum = -1
            for sumlist in self.sums_list:
                isum  = isum + 1
                if isinstance(sumlist, (list,tuple)):
                    self.sums_corr[isum] = self.det_corr[sumlist[0]]
                    for i in sumlist[1:]:
                        if i > 0:
                            self.sums_corr[isum] = self.sums_corr[isum] + self.det_corr[i][:]
                else:
                    self.sums_corr[isum] = self.det_corr[sumlist]


        #print '== After DT correction (raw detectors) =='
        #for i in range(6):
        #    print self.det_desc[i], self.det[i], self.det_corr[i]
        #print '=============='
        #print '== After DT correction (sums detectors) =='
        #for i in range(self.sums.shape[0]):
        #    print self.sums_names[i], self.sums_list[i], self.sums[i], self.sums_corr[i]
        #print '=============='


        return
        
    def read_ascii(self,fname=None):
        """read ascii data file"""
        lines = self._open_ascii(fname=fname)
        if lines is None: return -1
        
        maxlines = len(lines)

        iline = 1
        ndata_points = None
        tmp_dat = []
        tmp_y   = []
        col_details = []
        col_legend = None
        ntotal_at_2d = []
        mode = None
        while lines:
            key, raw = self._getline(lines)
            iline= iline+1
            if key is not None and key != mode:
                mode = key

            if (len(raw) < 3): continue
            self.ShowProgress( iline* 100.0 /(maxlines+1))

            if mode == '2d':
                self.dimension = 2
                sx   = raw.split()
                yval = float(sx[2])
                tmp_y.append(yval)
                self.yaddr = sx[1].strip()
                if self.yaddr.endswith(':'): self.yaddr = self.yaddr[:-1]
                mode = None
                if len(tmp_dat)>0:
                    ntotal_at_2d.append(len(tmp_dat))
                
            elif mode == 'epics scan':             # real numeric column data
                print 'Warning: file appears to have a second scan appended!'
                break
                
            elif mode == 'data':             # real numeric column data
                tmp_dat.append(numpy.array([float(i) for i in raw.split()]))

                
            elif mode == '-----':
                if col_legend is None:   
                    col_legend = lines.pop()[1:].strip().split()

            elif mode in ( '=====', 'n_points'):
                pass
            
            elif mode == 'user titles':
                self.user_titles.append(raw[1:].strip())

            elif mode == 'pv list':
                str = raw[1:].strip().replace('not connected',' = not connected')
                if str.lower().startswith(mode): continue
                desc = str
                addr = ''
                val  = 'unknown'
                try:
                    x =   str.split('=')
                    desc = x[0].replace('\t','').strip()
                    val = x[1].strip()
                    if '(' in desc and desc.endswith(')'):
                        n = desc.rfind('(')
                        addr = desc[n+1:-1]
                        desc = desc[:n].rstrip()        
                except:
                    pass
                self.env_addr.append(addr)
                self.env_desc.append(desc)
                self.env_val.append(val)
                        
            elif mode == 'scan regions':
                self.scan_regions.append(raw[1:].strip())

            elif mode == 'scan ended at':
                self.stop_time = raw[20:].strip()

            elif mode == 'scan began at':
                self.start_time = raw[20:].strip()

            elif mode == 'column labels':
                col_details.append(raw[1:].strip())

            elif mode is None:
                sx = [i.strip() for i in raw[1:].split('=')]
                if len(sx)>1:
                    if sx[0] == 'scan prefix':
                        self.scan_prefix = sx[1]
                    if sx[0] == 'scan dimension':
                        self.dimension = int(float(sx[1]))

            else:
                print 'UNKOWN MODE = ',mode, raw[:20]

        del lines
        
        try:        
            col_details.pop(0)

        except IndexError:
            print 'Empty Scan File'
            return -2
        
        if len(self.user_titles) > 1: self.user_titles.pop(0)
        if len(self.scan_regions) > 1: self.scan_regions.pop(0)

        # check that 2d maps are of consistent size
        if self.dimension == 2:
            ntotal_at_2d.append(len(tmp_dat))
            np_row0 = ntotal_at_2d[0]
            nrows   = len(ntotal_at_2d)
            npts    = len(tmp_dat)
            if npts != np_row0 * nrows:
                for i,n in enumerate(ntotal_at_2d):
                    if n == np_row0*(i+1):
                        nrows,npts_total = i+1,n

                if len(tmp_y) > nrows or len(tmp_dat)> npts_total:
                    print 'Warning: Some trailing data may be lost!'
                    tmp_y   = tmp_y[:nrows]
                    tmp_dat = tmp_dat[:npts_total]
            #
        self.y = numpy.array(tmp_y)
        # done reading file
        self._make_arrays(tmp_dat,col_legend,col_details)
        tmp_dat = None
       
        self.xaddr = self.pos_addr[0].strip()

        for addr,desc in zip(self.env_addr,self.env_desc):
            if self.xaddr == addr: self.xdesc = desc
            if self.yaddr == addr: self.ydesc = desc            

        # print self.xaddr, self.xdesc
        # print self.yaddr, self.ydesc
        
        self.has_fullxrf = False        
        if os.path.exists("%s.fullxrf" %fname):
            self.read_fullxrf("%s.fullxrf" %fname, len(self.x), len(self.y))

    def read_fullxrf(self,xrfname, n_xin, n_yin):
        inpf = open(xrfname,'r')

        atime = os.stat(xrfname)[8]
    
        prefix = os.path.splitext(xrfname)[0]
        print 'Reading Full XRF spectra from %s'  % xrfname

        first_line = inpf.readline()
        if not first_line.startswith('; MCA Spectra'):
            print 'Warning: %s is not a QuadXRF File' % xrffile
            inpf.close()
            return
        
        self.has_fullxrf = True
        isHeader= True
        nheader = 0
        header = {'CAL_OFFSET':None,'CAL_SLOPE':None,'CAL_QUAD':None}
        rois   = []

        n_energies = 2048

        while isHeader:
            line = inpf.readline()
            nheader = nheader + 1        
            isHeader = line.startswith(';') and not line.startswith(';----')
            words = line[2:-1].split(':')
            if words[0] in header.keys():
                header[words[0]] = [float(i) for i in words[1].split()]
            elif words[0].startswith('ROI'):
                roinum = int(words[0][3:])
                rois.append((words[1].strip(),int(words[2]),int(words[3])))

        # end of header: read one last line
        line = inpf.readline()
        nelem = self.nelem = len(header['CAL_OFFSET'])
        
        nheader = nheader + 1
        # print '==rois==' , len(rois), len(rois)/nelem, nelem

        allrois = []
        nrois =  len(rois)/nelem

        for i in range(nrois):
            tmp = [rois[i+j*nrois] for j in range(nelem)]
            allrois.append( tuple(tmp) )

        for i in range(nrois):
            nam = []
            lo = []
            hi = []
            for j in range(nelem):
                r = rois[i+j*nrois]
                nam.append(r[0])
                lo.append(r[1])
                hi.append(r[2])
            self.roi_names.append(nam)
            self.roi_llim.append(lo)
            self.roi_hlim.append(hi)

        roi_template ="""ROI_%i_LEFT:   %i %i %i %i
ROI_%i_RIGHT:  %i %i %i %i 
ROI_%i_LABEL:  %s & %s & %s & %s & """

        rout = []
        for i in range(nrois):
            vals = [i] + self.roi_llim[i] + [i] + self.roi_hlim[i] + [i] + self.roi_names[i]
            rout.append(roi_template % tuple(vals))

        xrf_header= """VERSION:    3.1
ELEMENTS:              %i
DATE:       %s
CHANNELS:           %i
ROIS:        %i %i %i %i
REAL_TIME:   1.0 1.0 1.0 1.0
LIVE_TIME:   1.0 1.0 1.0 1.0
CAL_OFFSET:  %15.8e %15.8e %15.8e %15.8e
CAL_SLOPE:   %15.8e %15.8e %15.8e %15.8e
CAL_QUAD:    %15.8e %15.8e %15.8e %15.8e
TWO_THETA:   10.0000000 10.0000000 10.0000000 10.0000000"""


        hout = [nelem, time.ctime(atime),n_energies, nrois, nrois, nrois, nrois]
        hout.extend( header['CAL_OFFSET'])
        hout.extend( header['CAL_SLOPE'])
        hout.extend( header['CAL_QUAD'])

        obuff ="%s\n%s" % (xrf_header % tuple(hout), '\n'.join(rout))
        rois = []
        allrois = []
        self.xrf_header = obuff

        # dir = prefix
        self.xrf_energies = []
        x_en = numpy.arange(n_energies)*1.0
        for i in range(nelem):
            off   = header['CAL_OFFSET'][i]
            slope = header['CAL_SLOPE'][i]
            quad  = header['CAL_QUAD'][i]            
            self.xrf_energies.append(off + x_en * (slope + x_en * quad))

        self.xrf_energies = numpy.array(self.xrf_energies)

        self.xrf_dict = {}
        processing = True
        iyold = 1
        ix    = 0
        iy    = 0
        # lines = inpf.readlines()

        progress_save = self.progress
        self.progress = self.my_progress
        # slow part: ascii text to numpy array
        for line in inpf:# enumerate(lines):
            try:
                raw = numpy.fromstring(line[:-1],sep=' ')
                ix  = raw[0]
                iy  = raw[1]
                dat = raw[2:]

                if iy != iyold:
                    iyold = iy
                    if iy>1: self.PrintMessage('. ')
                self.xrf_dict['%i/%i' % (ix,iy)] = dat
                
            except KeyboardInterrupt:
                return -3
        
        inpf.close()
        
        xrf_shape =  (n_xin, nelem, n_energies)
        if self.dimension == 2:
            xrf_shape =  (n_yin, n_xin, nelem, n_energies)            
        # print 'xrf_shape ', xrf_shape
        self.xrf_data = -1*numpy.ones(xrf_shape)
        xrf_dt_factor = self.dt_factor * 1.0

        if self.dimension == 2:
            xrf_dt_factor = xrf_dt_factor.transpose((1,2,0))[:,:,:,numpy.newaxis]
            for iy in range(n_yin):
                for ix in range(n_xin):
                    key = '%i/%i' % (ix+1,iy+1)
                    if key in self.xrf_dict:
                        d = numpy.array(self.xrf_dict[key])
                        d.shape = (nelem,n_energies)
                        self.xrf_data[iy,ix,:,:] = d
        else:
            xrf_dt_factor = xrf_dt_factor.transpose((1,0))[:,:,numpy.newaxis]
            for ix in range(n_xin):
                key = '%i/%i' % (ix+1,iy)
                d = numpy.array(self.xrf_dict[key])
                d.shape = (nelem, n_energies)
                self.xrf_data[ix,:,:] = d
            
        self.xrf_corr = self.xrf_data * xrf_dt_factor

        # merge XRF data

        en_merge = self.xrf_energies[0]
        if self.dimension == 2:
            self.xrf_merge      = self.xrf_data[:,:,0,:]*1.0
            self.xrf_merge_corr = self.xrf_corr[:,:,0,:]*1.0
            self.PrintMessage('\n')
            for iy in range(n_yin):
                self.PrintMessage('. ')
                for ix in range(n_xin):
                    sum_r = self.xrf_merge[iy,ix,:]*1.0
                    sum_c = self.xrf_merge_corr[iy,ix,:]*1.0
                    for idet in range(1,nelem):
                        en     = self.xrf_energies[idet]
                        dat_r  = self.xrf_data[iy,ix,idet,:]
                        dat_c  = self.xrf_corr[iy,ix,idet,:]
                        sum_r += numpy.interp(en_merge, en, dat_r)
                        sum_c += numpy.interp(en_merge, en, dat_c)
                    self.xrf_merge[iy,ix,:] = sum_r
                    self.xrf_merge_corr[iy,ix,:] = sum_c

        else:
            self.xrf_merge      = self.xrf_data[:,0,:]*1.0
            self.xrf_merge_corr = self.xrf_corr[:,0,:]*1.0

            for ix in range(n_xin):
                sum_r = self.xrf_merge[ix,:]*1.0
                sum_c = self.xrf_merge_corr[ix,:]*1.0
                for idet in range(1,nelem):
                    en     = self.xrf_energies[idet]
                    dat_r  = self.xrf_data[ix,idet,:]
                    dat_c  = self.xrf_corr[ix,idet,:]
                    sum_r += numpy.interp(en_merge, en, dat_r)
                    sum_c += numpy.interp(en_merge, en, dat_c)
                self.xrf_merge[ix,:] = sum_r
                self.xrf_merge_corr[ix,:] = sum_c

        self.progress = progress_save
        inpf.close()
        self.xrf_dict = None
        

    def save_sums_ascii(self,fname=None, correct=True,extension='dat'):
        if fname is None: fname = self.filename

        map = None
        correct = correct and hasattr(self,'det_corr')

        outf = _cleanfile(fname)

        fout = open("%s.%s" % (outf,extension),'w')
        fout.write("# ASCII data from  %s\n" % self.filename)
        fout.write("# x positioner %s = %s\n" % (self.xaddr,self.xdesc))
        if self.dimension==2:
            fout.write("# y positioner %s = %s\n" % (self.yaddr,self.ydesc))
            
        fout.write("# Dead Time Correction applied: %s\n" % correct)
        fout.write("#-----------------------------------------\n")

        labels = [self.xdesc]
        if self.dimension == 2:
            ydesc = self.ydesc
            if ydesc in ('',None): ydesc = 'Y'
            labels.append(ydesc)

        labels.extend(self.sums_names)
        labels = ["%5s" % _cleanfile(l) for l in labels]
        olabel = '        '.join(labels)
        fout.write("#  %s\n" % olabel)
        
        sums = self.sums
        if correct: sums = self.sums_corr

        
        if self.dimension ==1:
            for i,x in enumerate(self.x):
                o = ["%10.5f" % x]
                o.extend(["%12g" % s for s in sums[:,i]])
                fout.write(" %s\n" % " ".join(o) )

        else:
            for i,x in enumerate(self.x):
                for j,y in enumerate(self.y):                
                    o = [" %10.5f" % x, " %10.5f" % y]
                    o.extend(["%12g" % s for s in sums[:,j,i]])
                    fout.write(" %s\n" % " ".join(o))

        
        fout.close()


if (__name__ == '__main__'):
    import sys
    u = escan_data()
    u.read_data_file(sys.argv[1]) # , use_h5=False)

    # print u.pv_list

    
#     print '== positioners'
#     # print u.pos_names
#     print '== detectors'
#     #  print u.det_names
# 
#     print 'Pos = ', u.pos.shape
#     print 'Det = ', u.det.shape,  u.det_corr.shape
#     print 'Sums = ', u.sums.shape
#     print 'DT_FACTOR = ', u.dt_factor.shape
#     print 'XRF Energies = ', u.xrf_energies.shape
#     print 'XRF = ', u.xrf_data.shape
    
    # u.save_sums_ascii(correct=True,extension='dat')
    # u.save_sums_ascii(correct=False,extension='nocorr')


#     # 
#     #     print u.det[3]
#     #     print u.sums[1]
#     #     print u.pos[0]
#     #     #a
#     print 'X  = ', len(u.x)
#     print 'Y  = ', u.y
#     if u.has_fullxrf:
#         print 'Full XRF Spectra: ', u.xrf_energies.shape


    
    # print u.match_detector_name('ca')
    #  
    # 

#      print u.det[1,:20]
#     print u.det_corr[1,:20]
    
