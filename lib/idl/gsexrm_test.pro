;
;
function read_gsexrm, fname
    ; test reading of gsexrm data.
    on_ioerror, ERROR
    f = obj_new('gsexrm_data', fname=fname)
    x = f->get('x')
    
    x   = f->get('x')
    y   = f->get('y')
    xrf = f->get_merged_spectra()
    en  = f->get_merged_energy()

    help, xrf
    help, en
    
    plot, en, alog( total(total(xrf[*, 5:8, 10:12], 2), 2) > 1.)
    
    ;image_display, total(xrf[700:720, *, *], 1)
    r = f->close()
    return, dat
ERROR:
    return, -2
end

pro gsexrm_test
   ret = read_gsexrm('test.h5')
return
end
