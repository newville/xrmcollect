#!/usr/bin/env python
"""
GUI for displaying maps from HDF5 files

Needed Visualizations:

   XRF spectra display for  full map or selected portions
      choose defined ROIs or create new ROIs

   2x2 grid:
     +-------------+--------------+
     | map1        |  2-color map |
     +-------------+--------------+
     | correlation |  map2        |
     +-------------+--------------+

   All subplots "live" so that selecting regions in
   any (via box or lasso) highlights other plots
         box in map:  show XRF spectra, highlight correlations
         lasso in correlations:  show XRF spectra, enhance map points
"""
import os
import time
from threading import Thread

import wx
import wx.lib.agw.flatnotebook as flat_nb
import wx.lib.scrolledpanel as scrolled
import wx.lib.mixins.inspection
from wx._core import PyDeadObjectError

import h5py
import numpy as np

from wxmplot import ImageFrame

from .utils import (SimpleText, EditableListBox, FloatCtrl,
                    Closure, pack, popup,
                    add_button, add_menu, add_choice, add_menu)

from ..io.xrm_mapfile import (GSEXRM_MapFile,
                              GSEXRM_Exception, GSEXRM_NotOwner)

CEN = wx.ALIGN_CENTER|wx.ALIGN_CENTER_VERTICAL
LEFT = wx.ALIGN_LEFT|wx.ALIGN_CENTER_VERTICAL
RIGHT = wx.ALIGN_RIGHT|wx.ALIGN_CENTER_VERTICAL
ALL_CEN =  wx.ALL|CEN
ALL_LEFT =  wx.ALL|LEFT
ALL_RIGHT =  wx.ALL|RIGHT

# FILE_WILDCARDS = "X-ray Maps (*.0*)|*.0*|All files (*.*)|*.*"

# FILE_WILDCARDS = "X-ray Maps (*.0*)|*.0&"

NOT_OWNER_MSG = """The File
   '%s'
appears to be open by another process.  Having two
processes writing to the file can cause corruption.

Do you want to take ownership of the file?
"""

NOT_GSEXRM_FILE = """The File
   '%s'
doesn't seem to be a Map File
"""
FILE_ALREADY_READ = """The File
   '%s'
has already been read.
"""


def set_choices(choicebox, choices):
    choicebox.Clear()
    choicebox.AppendItems(choices)
    choicebox.SetStringSelection(choices[0])

class MapViewerFrame(wx.Frame):
    _about = """XRF Map Viewer
  Matt Newville <newville @ cars.uchicago.edu>
  """
    def __init__(self, conffile=None,  **kwds):

        kwds["style"] = wx.DEFAULT_FRAME_STYLE
        wx.Frame.__init__(self, None, -1, size=(700, 400),  **kwds)

        self.data = None
        self.filemap = {}
        self.im_displays = []
        self.larch = None

        self.Font14=wx.Font(14, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")
        self.Font12=wx.Font(12, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")
        self.Font11=wx.Font(11, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")
        self.Font10=wx.Font(10, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")
        self.Font9 =wx.Font(9, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")

        self.SetTitle("GSE XRM MapViewer")
        self.SetFont(self.Font9)

        self.createMainPanel()
        self.createMenus()
        self.statusbar = self.CreateStatusBar(2, 0)
        self.statusbar.SetStatusWidths([-3, -1])
        statusbar_fields = ["Initializing....", " "]
        for i in range(len(statusbar_fields)):
            self.statusbar.SetStatusText(statusbar_fields[i], i)

    def createMainPanel(self):
        sizer = wx.BoxSizer(wx.VERTICAL)
        splitter  = wx.SplitterWindow(self, style=wx.SP_LIVE_UPDATE)
        splitter.SetMinimumPaneSize(175)

        self.filelist = EditableListBox(splitter, self.ShowFile)
        self.detailspanel = self.createViewOptsPanel(splitter)
        splitter.SplitVertically(self.filelist, self.detailspanel, 1)
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(splitter, 1, wx.GROW|wx.ALL, 5)
        wx.CallAfter(self.init_larch)
        pack(self, sizer)

    def createViewOptsPanel(self, parent):
        """ panel for selecting ROIS, plot types"""
        panel = wx.Panel(parent)
        panel.SetMinSize((625, 375))
        sizer = wx.GridBagSizer(8, 8)
        self.title = SimpleText(panel, 'initializing...')
        ir = 0
        sizer.Add(self.title, (ir, 0), (1, 5), ALL_CEN, 2)
        ir += 1
        sizer.Add(wx.StaticLine(panel, size=(575, 3), style=wx.LI_HORIZONTAL),
                  (ir, 0), (1, 8), wx.ALIGN_LEFT)

        # Map ROI
        ir += 1
        sizer.Add(SimpleText(panel, 'Simple ROI Map', colour=(190, 10, 10)),
                  (ir, 0), (1, 3), ALL_CEN, 2)

        self.map1_roi1 = add_choice(panel, choices=[], size=(120, -1))
        self.map1_roi2 = add_choice(panel, choices=[], size=(120, -1))
        self.map1_op   = add_choice(panel, choices=['/', '*', '-', '+'], size=(80, -1))
        self.map1_det  = add_choice(panel, choices=['sum', '1', '2', '3', '4'], size=(80, -1))
        self.map1_new  = wx.CheckBox(panel, -1)
        self.map1_cor  = wx.CheckBox(panel, -1)
        self.map1_new.SetValue(1)
        self.map1_cor.SetValue(1)
        self.map1_op.SetSelection(0)
        self.map1_det.SetSelection(0)
        self.map1_show = add_button(panel, 'Show Map', size=(90, -1), action=self.onShowROIMap)
        ir += 1
        sizer.Add(SimpleText(panel, 'Map 1'),             (ir, 0), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(panel, 'Operator'),          (ir, 1), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(panel, 'Map 2'),             (ir, 2), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(panel, 'Detector'),          (ir, 3), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(panel, 'Correct Deadtime?'), (ir, 4), (1, 1), ALL_CEN, 2)
        ir += 1
        sizer.Add(self.map1_roi1,          (ir, 0), (1, 1), ALL_CEN, 2)
        sizer.Add(self.map1_op,            (ir, 1), (1, 1), ALL_CEN, 2)
        sizer.Add(self.map1_roi2,          (ir, 2), (1, 1), ALL_CEN, 2)
        sizer.Add(self.map1_det,           (ir, 3), (1, 1), ALL_CEN, 2)
        sizer.Add(self.map1_cor,           (ir, 4), (1, 1), ALL_CEN, 2)
        ir += 1
        sizer.Add(self.map1_show, (ir, 0), (1, 1), ALL_LEFT, 2)
        sizer.Add(SimpleText(panel, 'Reuse Previous Display'), (ir, 1), (1, 3), ALL_RIGHT, 2)
        sizer.Add(self.map1_new, (ir, 4), (1, 1), ALL_CEN, 2)

        ir += 1
        sizer.Add(wx.StaticLine(panel, size=(575, 3), style=wx.LI_HORIZONTAL),
                  (ir, 0), (1, 8), wx.ALIGN_LEFT)

        # 3 Color Map
        ir += 1
        sizer.Add(SimpleText(panel, 'Three Color ROI Map', colour=(190, 10, 10)),
                  (ir, 0), (1, 3), ALL_CEN, 2)

        self.map3_r = add_choice(panel, choices=[], size=(120, -1), action=Closure(self.onSetRGBScale, color='r'))
        self.map3_g = add_choice(panel, choices=[], size=(120, -1), action=Closure(self.onSetRGBScale, color='g'))
        self.map3_b = add_choice(panel, choices=[], size=(120, -1), action=Closure(self.onSetRGBScale, color='b'))
        self.map3_show = add_button(panel, 'Show Map', size=(90, -1), action=self.onShow3ColorMap)

        self.map3_det  = add_choice(panel, choices=['sum', '1', '2', '3', '4'], size=(80, -1))
        self.map3_new  = wx.CheckBox(panel, -1)
        self.map3_cor  = wx.CheckBox(panel, -1)
        self.map3_new.SetValue(1)
        self.map3_cor.SetValue(1)

        self.map3_rauto = wx.CheckBox(panel, -1, 'Autoscale?')
        self.map3_gauto = wx.CheckBox(panel, -1, 'Autoscale?')
        self.map3_bauto = wx.CheckBox(panel, -1, 'Autoscale?')
        self.map3_rauto.SetValue(1)
        self.map3_gauto.SetValue(1)
        self.map3_bauto.SetValue(1)
        self.map3_rauto.Bind(wx.EVT_CHECKBOX, Closure(self.onAutoScale, color='r'))
        self.map3_gauto.Bind(wx.EVT_CHECKBOX, Closure(self.onAutoScale, color='g'))
        self.map3_bauto.Bind(wx.EVT_CHECKBOX, Closure(self.onAutoScale, color='b'))

        self.map3_rscale = FloatCtrl(panel, precision=0, value=1, minval=0)
        self.map3_gscale = FloatCtrl(panel, precision=0, value=1, minval=0)
        self.map3_bscale = FloatCtrl(panel, precision=0, value=1, minval=0)

        ir += 1
        sizer.Add(SimpleText(panel, 'Red'),   (ir, 0), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(panel, 'Green'), (ir, 1), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(panel, 'Blue'),  (ir, 2), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(panel, 'Detector'),          (ir, 3), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(panel, 'Correct Deadtime?'), (ir, 4), (1, 1), ALL_CEN, 2)

        ir += 1
        sizer.Add(self.map3_r,                (ir, 0), (1, 1), ALL_CEN, 2)
        sizer.Add(self.map3_g,                (ir, 1), (1, 1), ALL_CEN, 2)
        sizer.Add(self.map3_b,                (ir, 2), (1, 1), ALL_CEN, 2)
        sizer.Add(self.map3_det,              (ir, 3), (1, 1), ALL_CEN, 2)
        sizer.Add(self.map3_cor,              (ir, 4), (1, 1), ALL_CEN, 2)

        ir += 1
        sizer.Add(self.map3_rauto,            (ir, 0), (1, 1), ALL_CEN, 2)
        sizer.Add(self.map3_gauto,            (ir, 1), (1, 1), ALL_CEN, 2)
        sizer.Add(self.map3_bauto,            (ir, 2), (1, 1), ALL_CEN, 2)
        ir += 1
        sizer.Add(self.map3_rscale,            (ir, 0), (1, 1), ALL_CEN, 2)
        sizer.Add(self.map3_gscale,            (ir, 1), (1, 1), ALL_CEN, 2)
        sizer.Add(self.map3_bscale,            (ir, 2), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(panel, '<- Intensity Value for Full Scale'),     (ir, 3), (1, 2), ALL_LEFT, 2)


        ir += 1
        sizer.Add(self.map3_show, (ir, 0), (1, 1), ALL_LEFT, 2)

        sizer.Add(SimpleText(panel, 'Reuse Previous Display'), (ir, 1), (1, 3), ALL_RIGHT, 2)
        sizer.Add(self.map3_new, (ir, 4), (1, 1), ALL_CEN, 2)



        ir += 1
        sizer.Add(wx.StaticLine(panel, size=(575, 3), style=wx.LI_HORIZONTAL),
                  (ir, 0), (1, 8), wx.ALIGN_LEFT)

        pack(panel, sizer)
        return panel

    def onAutoScale(self, event=None, color=None, **kws):
        if color=='r':
            self.map3_rscale.Enable()
            if self.map3_rauto.GetValue() == 1:  self.map3_rscale.Disable()
        elif color=='g':
            self.map3_gscale.Enable()
            if self.map3_gauto.GetValue() == 1:  self.map3_gscale.Disable()
        elif color=='b':
            self.map3_bscale.Enable()
            if self.map3_bauto.GetValue() == 1:  self.map3_bscale.Disable()

    def onSetRGBScale(self, event=None, color=None, **kws):
        det =self.map3_det.GetStringSelection()
        if det == 'sum':
            det =  None
        else:
            det = int(det)
        dtcorrect = self.map3_cor.IsChecked()

        if color=='r':
            roi = self.map3_r.GetStringSelection()
            map = self.current_file.get_roimap(roi, det=det, dtcorrect=dtcorrect)
            self.map3_rauto.SetValue(1)
            self.map3_rscale.SetValue(map.max())
        elif color=='g':
            roi = self.map3_g.GetStringSelection()
            map = self.current_file.get_roimap(roi, det=det, dtcorrect=dtcorrect)
            self.map3_gauto.SetValue(1)
            self.map3_gscale.SetValue(map.max())
        elif color=='b':
            roi = self.map3_b.GetStringSelection()
            map = self.current_file.get_roimap(roi, det=det, dtcorrect=dtcorrect)
            self.map3_bauto.SetValue(1)
            self.map3_bscale.SetValue(map.max())

    def onShow3ColorMap(self, event=None):
        det =self.map3_det.GetStringSelection()
        if det == 'sum':
            det =  None
        else:
            det = int(det)
        dtcorrect = self.map3_cor.IsChecked()

        r = self.map3_r.GetStringSelection()
        g = self.map3_g.GetStringSelection()
        b = self.map3_b.GetStringSelection()
        rmap = self.current_file.get_roimap(r, det=det, dtcorrect=dtcorrect)
        gmap = self.current_file.get_roimap(g, det=det, dtcorrect=dtcorrect)
        bmap = self.current_file.get_roimap(b, det=det, dtcorrect=dtcorrect)

        rscale = 1.0/self.map3_rscale.GetValue()
        gscale = 1.0/self.map3_gscale.GetValue()
        bscale = 1.0/self.map3_bscale.GetValue()
        if self.map3_rauto.IsChecked():  rscale = 1.0/rmap.max()
        if self.map3_gauto.IsChecked():  gscale = 1.0/gmap.max()
        if self.map3_bauto.IsChecked():  bscale = 1.0/bmap.max()

        map = np.array([rmap*rscale, gmap*gscale, bmap*bscale]).swapaxes(0, 2).swapaxes(0, 1)
        if len(self.im_displays) == 0 or not self.map3_new.IsChecked():
            self.im_displays.append(ImageFrame(config_on_frame=False))

        title = '%s: R, G, B = %s, %s, %s' % (self.current_file.filename, r, g, b)
        self.display_map(map, title=title, with_config=False)


    def onShowROIMap(self, event=None):
        det =self.map1_det.GetStringSelection()
        if det == 'sum':
            det =  None
        else:
            det = int(det)
        dtcorrect = self.map1_cor.IsChecked()

        roiname1 = self.map1_roi1.GetStringSelection()
        roiname2 = self.map1_roi2.GetStringSelection()
        map = self.current_file.get_roimap(roiname1, det=det, dtcorrect=dtcorrect)
        title = '%s: %s' % (self.current_file.filename, roiname1)

        if roiname2 != '':
            mapx = self.current_file.get_roimap(roiname2, det=det, dtcorrect=dtcorrect)
            op = self.map1_op.GetStringSelection()
            if   op == '+': map +=  mapx
            elif op == '-': map -=  mapx
            elif op == '*': map *=  mapx
            elif op == '/': map /=  mapx

        if len(self.im_displays) == 0 or not self.map1_new.IsChecked():
            self.im_displays.append(ImageFrame())

        self.display_map(map, title=title)

    def display_map(self, map, title='', with_config=True):
        """display a map in an available image display"""
        displayed = False
        while not displayed:
            try:
                imd = self.im_displays.pop()
                imd.display(map, title=title)
                displayed = True
            except IndexError:
                imd = ImageFrame(config_on_frame=with_config)
                imd.display(map, title=title)
                displayed = True
            except PyDeadObjectError:
                displayed = False
        self.im_displays.append(imd)
        imd.Show()
        imd.Raise()

    def init_larch(self):
        t0 = time.time()
        from larch import Interpreter
        from larch.wxlib import inputhook
        self.larch = Interpreter()
        self.larch.symtable.set_symbol('_sys.wx.wxapp', wx.GetApp())
        self.larch.symtable.set_symbol('_sys.wx.parent', self)
        self.SetStatusText('ready')
        self.datagroups = self.larch.symtable
        self.title.SetLabel('')

    def onPlot(self, evt):    self.do_plot(newplot=True)

    def onOPlot(self, evt):   self.do_plot(newplot=False)

    def do_plot(self, newplot=False):

        ix = self.x_choice.GetSelection()
        x  = self.x_choice.GetStringSelection()
        if self.data is None and ix > -1:
            self.SetStatusText( 'cannot plot - no valid data')
        xop = self.x_op.GetStringSelection()
        yop1 = self.y_op1.GetStringSelection()
        yop2 = self.y_op2.GetStringSelection()
        yop3 = self.y_op3.GetStringSelection()

        y1 = self.y1_choice.GetStringSelection()
        y2 = self.y2_choice.GetStringSelection()
        y3 = self.y3_choice.GetStringSelection()
        if y1 == '': y1 = '1'
        if y2 == '': y2 = '1'
        if y3 == '': y3 = '1'

        gname = self.groupname
        lgroup = getattr(self.larch.symtable, gname)

        xlabel_ = xlabel = x
        xunits = lgroup.column_units[ix]
        if xunits != '':
            xlabel = '%s (%s)' % (xlabel, xunits)

        x = "%s.get_data('%s')" % (gname, x)

        if xop == 'log': x = "log(%s)" % x

        ylabel = "[%s%s%s]%s%s" % (y1, yop2, y2, yop3, y3)
        if y2 == '1' and yop2 in ('*', '/') or y2 == '0' and yop2 in ('+', '-'):
            ylabel = "(%s%s%s" % (y1, yop3, y3)
            if y3 == '1' and yop3 in ('*', '/') or y3 == '0' and yop3 in ('+', '-'):
                ylabel = "%s" % (y1)
        elif y3 == '1' and yop3 in ('*', '/') or y3 == '0' and yop3 in ('+', '-'):
            ylabel = "%s%s%s" % (y1, yop2, y2)
        if yop1 != '':
            yoplab = yop1.replace('deriv', 'd')
            ylabel = '%s(%s)' % (yoplab, ylabel)
            if '(' in yop1: ylabel = "%s)" % ylabel

        y1 = y1 if y1 in ('0, 1') else "%s.get_data('%s')" % (gname, y1)
        y2 = y2 if y2 in ('0, 1') else "%s.get_data('%s')" % (gname, y2)
        y3 = y3 if y3 in ('0, 1') else "%s.get_data('%s')" % (gname, y3)

        y = "%s((%s %s %s) %s (%s))" % (yop1, y1, yop2, y2, yop3, y3)
        if '(' in yop1: y = "%s)" % y
        if 'deriv' in yop1:
            y = "%s/deriv(%s)" % (y, x)
            ylabel = '%s/d(%s)' % (ylabel, xlabel_)

        fmt = "plot(%s, %s, label='%s', xlabel='%s', ylabel='%s', new=%s)"
        cmd = fmt % (x, y, self.data.fname, xlabel, ylabel, repr(newplot))
        self.larch(cmd)

    def ShowFile(self, evt=None, filename=None, **kws):
        if filename is None and evt is not None:
            filename = evt.GetString()
        if self.check_ownership(filename):
            self.filemap[filename].process()
        self.current_file = self.filemap[filename]
        self.title.SetLabel("%s" % filename)

        rois = list(self.filemap[filename].xrfmap['roimap/sum_name'])
        rois_extra = [''] + rois

        set_choices(self.map1_roi1, rois)
        set_choices(self.map1_roi2, rois_extra)
        set_choices(self.map3_r, rois)
        set_choices(self.map3_g, rois)
        set_choices(self.map3_b, rois)

    def createMenus(self):
        self.menubar = wx.MenuBar()
        fmenu = wx.Menu()
        add_menu(self, fmenu, "&Open Map\tCtrl+O",
                 "Read Map File or Folder",  self.onRead)

        fmenu.AppendSeparator()
        add_menu(self, fmenu, "&Quit\tCtrl+Q",
                  "Quit program", self.onClose)

        self.menubar.Append(fmenu, "&File")
        self.SetMenuBar(self.menubar)

    def onAbout(self,evt):
        dlg = wx.MessageDialog(self, self._about,"About GSEXRM MapViewer",
                               wx.OK | wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()

    def onClose(self,evt):
        for xrmfile in self.filemap.values():
            xrmfile.close()

        for imd in self.im_displays:
            try:
                imd.Destroy()
            except:
                pass

        for nam in dir(self.larch.symtable._plotter):
            obj = getattr(self.larch.symtable._plotter, nam)
            try:
                obj.Destroy()
            except:
                pass
        for nam in dir(self.larch.symtable._sys.wx):
            obj = getattr(self.larch.symtable._sys.wx, nam)
            del obj
        self.Destroy()

    def onRead(self, evt=None):
        dlg = wx.FileDialog(self, message="Read Map",
                            defaultDir=os.getcwd(),
                            # wildcard=FILE_WILDCARDS,
                            style=wx.OPEN)
        path, read = None, False
        if dlg.ShowModal() == wx.ID_OK:
            read = True
            path = dlg.GetPath().replace('\\', '/')
            if path in self.filemap:
                read = popup(self, "Re-read file '%s'?" % path, 'Re-read file?',
                             style=wx.YES_NO)
        dlg.Destroy()

        if read:
            try:
                parent, fname = os.path.split(path)
                xrmfile = GSEXRM_MapFile(fname)
            except:
                popup(self, NOT_GSEXRM_FILE % fnamex,
                      "Not a Map file!")
                return
            if fname not in self.filemap:
                self.filemap[fname] = xrmfile
            if fname not in self.filelist.GetItems():
                self.filelist.Append(fname)
            #if self.check_ownership(fname):
            #    self.process_file(fname)
            self.ShowFile(filename=fname)

    def process_file(self, filename):
        """Request processing of map file.
        This can take awhile, so is done in a separate thread,
        with updates displayed in message bar
        """
        xrm_map = self.filemap[filename]
        if not xrm_map.folder_has_newdata():
            return
        print 'PROCESS ', filename, xrm_map.folder_has_newdata()
        def on_process(row=0, maxrow=0, fname=None, status='unknown'):
            if maxrow < 1 or fname is None:
                return
            self.SetStatusText('processing row=%i / %i for %s [%s]' %
                               (row, maxrow, fname, status))

        dthread  = Thread(target=self.filemap[filename].process,
                          kwargs={'callback': on_process},
                          name='process_thread')
        dthread.start()
        dthread.join()

    def check_ownership(self, fname):
        """
        check whether we're currently owner of the file.
        this is important!! HDF5 files can be corrupted.
        """
        if not self.filemap[fname].check_hostid():
            if popup(self, NOT_OWNER_MSG % fname,
                     'Not Owner of HDF5 File', style=wx.YES_NO):
                self.filemap[fname].claim_hostid()
        return self.filemap[fname].check_hostid()

class ViewerApp(wx.App, wx.lib.mixins.inspection.InspectionMixin):
    def __init__(self, config=None, dbname=None, **kws):
        self.config  = config
        self.dbname  = dbname
        wx.App.__init__(self)

    def OnInit(self):
        self.Init()
        frame = MapViewerFrame() #
        frame.Show()
        self.SetTopWindow(frame)
        return True

if __name__ == "__main__":
    ViewerApp().MainLoop()
