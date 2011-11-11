;
;
; GSECARS XRM Scan Data Class version 2: read Full XRF Spectra
; 
; function gsexrm_data::open_file

; function gsexrm_data::get('param')

function get_h5data, group, attr
on_ioerror, ERROR
    if group eq 0 then return, 0
    return, h5d_read(h5d_open(group, attr))
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
  if version ne '1.2.0' then begin
      print, "Wrong GSEXRM File Version -- need 1.2.0"
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
   root  = h5g_open(self.fileh,'/xrf_map')

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
           'config':       config  = group
           'scan':         self.g_scan = group
           'xrf_spectra':  self.g_xrf_spectra = group
       endcase
   endfor
   ng_conf = h5g_get_num_objs(config)
   for i = 0, ng_conf-1 do begin
       gname = h5g_get_obj_name_by_idx(config, i)
       group = h5g_open(config, gname)
       case gname of
           'environ':      self.g_environ  = group
           'rois':         self.g_rois     = group
           'scan':         self.g_scan_conf = group
           'general':      self.g_config   = group
           'mca_calib':    self.g_mca_calib = group
           'mca_settings': self.g_mca_settings = group
           'motor_controller': self.g_motor_controller = group
           'positioners':      self.g_positioners = group
       endcase
   endfor

   self.start_time = get_h5attr(root, 'Start_Time')
   self.stop_time  = get_h5attr(root, 'Stop_Time')
   self.dimension  = get_h5attr(root, 'Dimension')

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

function gsexrm_data::get_pos, p
  pos = get_h5data(self.g_scan, 'pos')
  if p eq 'x' then begin
     return, reform(pos[0, *, 0])
  endif else if p eq 'y' then begin
     return, reform(pos[1, 0, *])
  endif else begin
     return, p
  endelse
end

function gsexrm_data::get_roilims, p
  lims = get_h5data(self.g_rois, 'limits')
  if p eq 0 then begin
     return, reform(lims[0, *, *])
  endif else begin
     return, reform(lims[1, *, *])
  endelse
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

        'x':            return, self->get_pos('x')
        'y':            return, self->get_pos('y')
        'pos':          return, self->get_pos('')
        'det':           return, get_h5data(self.g_scan,'det_raw')
        'det_corrected': return, get_h5data(self.g_scan,'det_dtcorr')
        'sums':          return, get_h5data(self.g_scan,'sums_raw')
        'sums_corrected':return, get_h5data(self.g_scan,'sums_dtcorr')

        'det_names':     return, get_h5data(self.g_scan,'det_name')
        'det_pvs':       return, get_h5data(self.g_scan,'det_address')
 
        'pos_names':     return, get_h5data(self.g_scan,'pos_name')
        'pos_pvs':       return, get_h5data(self.g_scan,'pos_address')

        'sums_list':     return, get_h5data(self.g_scan,'sums_list')
        'sums_names':    return, get_h5data(self.g_scan,'sums_name')
        'user_titles':   return, get_h5data(self.g_scan,'user_titles')
        ; 'scan_regions':  return, get_h5data(self.g_scan,'scan_regions')

        'env_names':     return, get_h5data(self.g_environ,'name')
        'env_pvs':       return, get_h5data(self.g_environ,'address')
        'env_vals':      return, get_h5data(self.g_environ,'value')

        'xrf_energies':  return, get_h5data(self.g_xrf_spectra,'energies')
        'xrf_data':      return, get_h5data(self.g_xrf_spectra,'data')
        'dt_factor':     return, get_h5data(self.g_xrf_spectra,'dt_factor')
        'livetime':      return, get_h5data(self.g_xrf_spectra,'livetime')
        'realtime':      return, get_h5data(self.g_xrf_spectra,'realtime')

        'roi_labels':    return, get_h5data(self.g_rois, 'name')
        'roi_limits':    return, get_h5data(self.g_rois, 'limits')
        'roi_llim':      return, self->get_roilims(0)
        'roi_hlim':      return, self->get_roilims(1)
        
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
                   file_ok: 0L,     $
                   dimension: 1L,   scan_prefix: '', $
                   start_time: '',  stop_time: '',  $
                   g_xrf_spectra: 0L, $
                   g_scan: 0L,  $
                   g_environ: 0L, $
                   g_rois: 0L,   $
                   g_scan_conf: 0L, $
                   g_config: 0L, $
                   g_mca_calib: 0L, $
                   g_mca_settings: 0L, $
                   g_motor_controller: 0L, $
                   g_positioners: 0L, $
                   aa: ''}

end
