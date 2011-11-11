;
;
; GSECARS XRM Scan Data Class version 2: read Full XRF Spectra
; 
; function gsexrm_data::open_file

; function gsexrm_data::get('param')

function get_h5data, group, attr
on_ioerror, ERROR
    if group eq 0 then return, 0
    return, h5d_read(h5d_open(group,attr))
ERROR:
return, 0
end

function get_h5attr, group, attr
   return, h5a_read(h5a_open_name(group,attr))
end

function gsexrm_data::validate_gse, root
on_ioerror, ERROR
  title    = get_h5attr(root,'Title')
  version  = get_h5attr(root,'Version')
  beamline = get_h5attr(root,'Beamline')

  if title ne 'Epics Scan Data' then return, 0
  if version ne '1.1.0' then begin
      print, "Wrong GSEXRM File Version -- need 1.1.0"
      return, 0
  endif

return, 1
ERROR: 
return, 0
end
function gsexrm_data::open_file, fname

;
; open a GSE XRM data file
;
   suffix = '.h5'

   print, format='(3a,$)', 'opening file ', fname, ' . '

   if strpos(fname,suffix) eq -1 then begin
        fname = fname + suffix
   endif

   self.fname = fname

   on_ioerror, ERROR_read
   self.fileh  = h5f_open(fname)
   root  = h5g_open(self.fileh,'/data')

   isValid = self->validate_gse(root)
   if isValid ne 1 then begin
       print, 'Not a valid GSE XRM file: ', fname
       h5f_close, self.fileh
       return, -1
   endif
   ngroups = h5g_get_num_objs(root)
   for i = 0, ngroups-1 do begin
       gname = h5g_get_obj_name_by_idx(root,i)
       group = h5g_open(root,gname)
       case gname of
           'rois':         self.g_rois    = group
           'roi_scan':     self.g_scan    = group
           'environ':      self.g_environ = group
           'full_xrf':     self.g_fullxrf = group
       endcase
   endfor

   self.start_time = get_h5attr(self.g_scan,'start_time')
   self.stop_time  = get_h5attr(self.g_scan,'stop_time')
   self.dimension  = get_h5attr(self.g_scan,'dimension')
   self.scan_prefix= get_h5attr(self.g_scan,'scan_prefix')
   
   self.file_ok = 1
   print, 'read ok.' 
   return, 0
   
ERROR_read:
   self.file_ok = 0
   print, 'problem reading file.'
   h5f_close, self.fileh
   return, -1
end


function gsexrm_data::close
   self.g_rois = 0
   self.g_scan =  0
   self.g_environ = 0
   self.file_ok = 0
   h5f_close, self.fileh
   return, 0
end

function gsexrm_data::get_merged_energy
   ;
   ; return energy array for merged data
   en = self->get('xrf_energies')

   return, en[*, 0]
end

function gsexrm_data::get_merged_spectra
   ;
   ; return full xrf data, dead-time corrected and merged to energy grid 
   ; of 1st detector element
   ;
    en = self->get('xrf_energies')
    dat= self->get('xrf_data')
    dt_factor = self->get('dt_factor')

    dsize = size(dat)
    nx = dsize[3]
    ny = dsize[4]
   
    eout = en[*,0]
    sum  = 1.0*reform(dat[*, 0, *, *])
    for ichan = 1, 3 do begin
        for ix = 0, nx-1 do begin
            for iy = 0, ny-1 do begin
                corr = reform(1.0*dat[*, ichan, ix, iy]) * dt_factor[ichan, ix, iy]
                sum[*, ix, iy]  += interpol(corr, en[*, ichan], eout)
            endfor
        endfor
    endfor

  return, sum
end

function gsexrm_data::get, param
;
; return a copy of an object member structure
; for outside manipulation and later 'set_param'ing

if self.file_ok eq 0 then return, 0

if (keyword_set(param) ne 0) then begin
    case param of
        'fname':        return, self.fname
        'file_ok':      return, self.file_ok
        'dimension':    return, self.dimension
        'start_time':   return, self.start_time
        'stop_time':    return, self.stop_time
        'scan_prefix':  return, self.scan_prefix  
        'has_dtime':    return, self.has_dtime

        'x':            return, get_h5data(self.g_scan,'x')
        'y':            begin
            if self.dimension eq 2 then return, get_h5data(self.g_scan,'y')
        end
        'pos':           return, get_h5data(self.g_scan,'pos')
        'det':           return, get_h5data(self.g_scan,'det')
        'det_corrected': return, get_h5data(self.g_scan,'det_corrected')
        'sums':          return, get_h5data(self.g_scan,'sums')
        'sums_corrected':return, get_h5data(self.g_scan,'sums_corrected')

        'det_names':     return, get_h5data(self.g_scan,'det_desc')
        'det_pvs':       return, get_h5data(self.g_scan,'det_addr')
 
        'pos_names':     return, get_h5data(self.g_scan,'pos_desc')
        'pos_pvs':       return, get_h5data(self.g_scan,'pos_addr')

        'sums_list':     return, get_h5data(self.g_scan,'sums_list')
        'sums_names':    return, get_h5data(self.g_scan,'sums_names')
        'user_titles':   return, get_h5data(self.g_scan,'user_titles')
        'scan_regions':  return, get_h5data(self.g_scan,'scan_regions')

        'env_names':     return, get_h5data(self.g_environ,'desc')
        'env_pvs':       return, get_h5data(self.g_environ,'addr')
        'env_vals':      return, get_h5data(self.g_environ,'val')

        'xrf_energies':  return, get_h5data(self.g_fullxrf,'energies')
        'xrf_data':      return, get_h5data(self.g_fullxrf,'data')
        'dt_factor':     return, get_h5data(self.g_fullxrf,'dt_factor')
        'livetime':      return, get_h5data(self.g_fullxrf,'livetime')
        'realtime':      return, get_h5data(self.g_fullxrf,'realtime')

        'roi_labels':    return, get_h5data(self.g_rois, 'roi_labels')
        'roi_llim':      return, get_h5data(self.g_rois, 'roi_lo_limit')
        'roi_hlim':      return, get_h5data(self.g_rois,'roi_hi_limit')
        
    endcase
endif
return, 0
end

function gsexrm_data::init, fname=fname
if (n_elements(fname) ne 0) then begin
    x = self->open_file(fname)
endif
return, 1
end

pro  gsexrm_data__define
    gsexrm_data = {gsexrm_data, $ 
                   fname:  '',      fileh: 0L, $  
                   file_ok: 0L,     has_dtime: 0L, $ 
                   dimension: 1L,   scan_prefix: '', $
                   start_time: '',  stop_time: '',  $
                   g_rois: 0L,   g_fullxrf: 0L, $
                   g_scan: 0L,      g_environ: 0L }

end
