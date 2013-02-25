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

FNB_STYLE = flat_nb.FNB_NO_X_BUTTON|flat_nb.FNB_SMART_TABS|flat_nb.FNB_NO_NAV_BUTTONS

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


class SimpleMapPanel(wx.Panel):
    """Panel of Controls for choosing what to display a simple ROI map"""
    def __init__(self, parent, owner, **kws):
        wx.Panel.__init__(self, parent, -1, **kws)
        self.owner = owner

        sizer = wx.GridBagSizer(8, 5)

        self.roi1 = add_choice(self, choices=[], size=(120, -1))
        self.roi2 = add_choice(self, choices=[], size=(120, -1))
        self.op   = add_choice(self, choices=['/', '*', '-', '+'], size=(80, -1))
        self.det  = add_choice(self, choices=['sum', '1', '2', '3', '4'], size=(80, -1))
        self.newid  = wx.CheckBox(self, -1, 'Correct Deadtime?')
        self.cor  = wx.CheckBox(self, -1, 'Reuse Previous Display?')
        self.newid.SetValue(1)
        self.cor.SetValue(1)
        self.op.SetSelection(0)
        self.det.SetSelection(0)
        self.show = add_button(self, 'Show Map', size=(90, -1), action=self.onShowMap)

        ir = 0
        sizer.Add(SimpleText(self, 'Map 1'),             (ir, 1), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(self, 'Operator'),          (ir, 2), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(self, 'Map 2'),             (ir, 3), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(self, 'Detector'),          (ir, 0), (1, 1), ALL_CEN, 2)

        ir += 1
        sizer.Add(self.roi1,          (ir, 1), (1, 1), ALL_CEN, 2)
        sizer.Add(self.op,            (ir, 2), (1, 1), ALL_CEN, 2)
        sizer.Add(self.roi2,          (ir, 3), (1, 1), ALL_CEN, 2)
        sizer.Add(self.det,           (ir, 0), (1, 1), ALL_CEN, 2)

        ir += 1
        sizer.Add(self.cor,   (ir, 0), (1, 2), ALL_LEFT, 2)
        sizer.Add(self.newid, (ir, 2), (1, 2), ALL_LEFT, 2)
        sizer.Add(self.show,  (ir+1, 0), (1, 1), ALL_LEFT, 2)

        sizer.Add(wx.StaticLine(self, size=(500, 3), style=wx.LI_HORIZONTAL),
                  (ir+2, 0), (1, 5), ALL_CEN)

        pack(self, sizer)


    def onShowMap(self, event=None):
        datafile  = self.owner.current_file

        det =self.det.GetStringSelection()
        if det == 'sum':
            det =  None
        else:
            det = int(det)
        dtcorrect = self.cor.IsChecked()
        roiname1 = self.roi1.GetStringSelection()
        roiname2 = self.roi2.GetStringSelection()
        map = datafile.get_roimap(roiname1, det=det, dtcorrect=dtcorrect)
        title = '%s: %s' % (datafile.filename, roiname1)

        if roiname2 != '':
            mapx = datafile.get_roimap(roiname2, det=det, dtcorrect=dtcorrect)
            op = self.op.GetStringSelection()
            if   op == '+': map +=  mapx
            elif op == '-': map -=  mapx
            elif op == '*': map *=  mapx
            elif op == '/': map /=  mapx

        if len(self.owner.im_displays) == 0 or not self.newid.IsChecked():
            self.owner.im_displays.append(ImageFrame())
        self.owner.display_map(map, title=title)


class TriColorMapPanel(wx.Panel):
    """Panel of Controls for choosing what to display a 3 color ROI map"""
    def __init__(self, parent, owner, **kws):
        wx.Panel.__init__(self, parent, -1, **kws)
        self.owner = owner
        sizer = wx.GridBagSizer(8, 8)

        self.SetMinSize((425, 275))

        self.rchoice = add_choice(self, choices=[], size=(120, -1), action=Closure(self.onSetRGBScale, color='r'))
        self.gchoice = add_choice(self, choices=[], size=(120, -1), action=Closure(self.onSetRGBScale, color='g'))
        self.bchoice = add_choice(self, choices=[], size=(120, -1), action=Closure(self.onSetRGBScale, color='b'))
        self.show = add_button(self, 'Show Map', size=(90, -1), action=self.onShow3ColorMap)

        self.det  = add_choice(self, choices=['sum', '1', '2', '3', '4'], size=(80, -1))
        self.newid  = wx.CheckBox(self, -1, 'Correct Deadtime?')
        self.cor  = wx.CheckBox(self, -1, 'Reuse Previous Display?')
        self.newid.SetValue(1)
        self.cor.SetValue(1)

        self.rauto = wx.CheckBox(self, -1, 'Autoscale?')
        self.gauto = wx.CheckBox(self, -1, 'Autoscale?')
        self.bauto = wx.CheckBox(self, -1, 'Autoscale?')
        self.rauto.SetValue(1)
        self.gauto.SetValue(1)
        self.bauto.SetValue(1)
        self.rauto.Bind(wx.EVT_CHECKBOX, Closure(self.onAutoScale, color='r'))
        self.gauto.Bind(wx.EVT_CHECKBOX, Closure(self.onAutoScale, color='g'))
        self.bauto.Bind(wx.EVT_CHECKBOX, Closure(self.onAutoScale, color='b'))

        self.rscale = FloatCtrl(self, precision=0, value=1, minval=0)
        self.gscale = FloatCtrl(self, precision=0, value=1, minval=0)
        self.bscale = FloatCtrl(self, precision=0, value=1, minval=0)

        ir = 0
        sizer.Add(SimpleText(self, 'Red'),       (ir, 1), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(self, 'Green'),     (ir, 2), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(self, 'Blue'),      (ir, 3), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(self, 'Detector'),  (ir, 0), (1, 1), ALL_CEN, 2)

        ir += 1
        sizer.Add(self.rchoice,              (ir, 1), (1, 1), ALL_CEN, 2)
        sizer.Add(self.gchoice,              (ir, 2), (1, 1), ALL_CEN, 2)
        sizer.Add(self.bchoice,              (ir, 3), (1, 1), ALL_CEN, 2)
        sizer.Add(self.det,                  (ir, 0), (1, 1), ALL_CEN, 2)

        ir += 1
        sizer.Add(self.rauto,            (ir, 1), (1, 1), ALL_CEN, 2)
        sizer.Add(self.gauto,            (ir, 2), (1, 1), ALL_CEN, 2)
        sizer.Add(self.bauto,            (ir, 3), (1, 1), ALL_CEN, 2)
        ir += 1
        sizer.Add(self.rscale,            (ir, 1), (1, 1), ALL_CEN, 2)
        sizer.Add(self.gscale,            (ir, 2), (1, 1), ALL_CEN, 2)
        sizer.Add(self.bscale,            (ir, 3), (1, 1), ALL_CEN, 2)
        sizer.Add(SimpleText(self, 'Max Intensity:'),     (ir, 0), (1, 1), ALL_LEFT, 2)


        ir += 1
        sizer.Add(self.cor,   (ir, 0), (1, 2), ALL_LEFT, 2)
        sizer.Add(self.newid, (ir, 2), (1, 2), ALL_LEFT, 2)
        sizer.Add(self.show,  (ir+1, 0), (1, 1), ALL_LEFT, 2)

        sizer.Add(wx.StaticLine(self, size=(500, 3), style=wx.LI_HORIZONTAL),
                  (ir+2, 0), (1, 5), ALL_CEN)

        pack(self, sizer)

    def onSetRGBScale(self, event=None, color=None, **kws):
        datafile = self.owner.current_file
        det =self.det.GetStringSelection()
        if det == 'sum':
            det =  None
        else:
            det = int(det)
        dtcorrect = self.cor.IsChecked()

        if color=='r':
            roi = self.rchoice.GetStringSelection()
            map = datafile.get_roimap(roi, det=det, dtcorrect=dtcorrect)
            self.rauto.SetValue(1)
            self.rscale.SetValue(map.max())
            self.rscale.Disable()
        elif color=='g':
            roi = self.gchoice.GetStringSelection()
            map = datafile.get_roimap(roi, det=det, dtcorrect=dtcorrect)
            self.gauto.SetValue(1)
            self.gscale.SetValue(map.max())
            self.gscale.Disable()
        elif color=='b':
            roi = self.bchoice.GetStringSelection()
            map = datafile.get_roimap(roi, det=det, dtcorrect=dtcorrect)
            self.bauto.SetValue(1)
            self.bscale.SetValue(map.max())
            self.bscale.Disable()

    def onShow3ColorMap(self, event=None):
        datafile = self.owner.current_file
        det =self.det.GetStringSelection()
        if det == 'sum':
            det =  None
        else:
            det = int(det)
        dtcorrect = self.cor.IsChecked()

        r = self.rchoice.GetStringSelection()
        g = self.gchoice.GetStringSelection()
        b = self.bchoice.GetStringSelection()
        rmap = datafile.get_roimap(r, det=det, dtcorrect=dtcorrect)
        gmap = datafile.get_roimap(g, det=det, dtcorrect=dtcorrect)
        bmap = datafile.get_roimap(b, det=det, dtcorrect=dtcorrect)

        rscale = 1.0/self.rscale.GetValue()
        gscale = 1.0/self.gscale.GetValue()
        bscale = 1.0/self.bscale.GetValue()
        if self.rauto.IsChecked():  rscale = 1.0/rmap.max()
        if self.gauto.IsChecked():  gscale = 1.0/gmap.max()
        if self.bauto.IsChecked():  bscale = 1.0/bmap.max()

        map = np.array([rmap*rscale, gmap*gscale, bmap*bscale]).swapaxes(0, 2).swapaxes(0, 1)
        if len(self.owner.im_displays) == 0 or not self.newid.IsChecked():
            self.owner.im_displays.append(ImageFrame(config_on_frame=False))

        title = '%s: (R, G, B) = (%s, %s, %s)' % (datafile.filename, r, g, b)
        self.owner.display_map(map, title=title, with_config=False)

    def onAutoScale(self, event=None, color=None, **kws):
        if color=='r':
            self.rscale.Enable()
            if self.rauto.GetValue() == 1:  self.rscale.Disable()
        elif color=='g':
            self.gscale.Enable()
            if self.gauto.GetValue() == 1:  self.gscale.Disable()
        elif color=='b':
            self.bscale.Enable()
            if self.bauto.GetValue() == 1:  self.bscale.Disable()

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
        # self.detailspanel = self.createViewOptsPanel(splitter)

        dpanel = self.detailspanel = wx.Panel(splitter)
        dpanel.SetMinSize((575, 350))
        self.createNBPanels(dpanel)
        splitter.SplitVertically(self.filelist, self.detailspanel, 1)
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(splitter, 1, wx.GROW|wx.ALL, 5)
        wx.CallAfter(self.init_larch)
        pack(self, sizer)

    def createNBPanels(self, parent):
        self.title = SimpleText(parent, 'initializing...')
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(self.title, 0, ALL_CEN)

        self.nb = flat_nb.FlatNotebook(parent, wx.ID_ANY, agwStyle=FNB_STYLE)
        self.nb.SetBackgroundColour('#FCFCFA')
        self.SetBackgroundColour('#F0F0E8')

        self.nbpanels = {}
        for name, key, creator in (('Simple ROI Map',  'roimap', SimpleMapPanel),
                                   ('3-Color ROI Map', '3color',  TriColorMapPanel)):
            #  ('2x2 Grid',         self.MapGridPanel)):
            # print 'panel ' , name, parent, creator
            p = creator(parent, owner=self)
            self.nb.AddPage(p, name, True)
            self.nbpanels[key] = p

        self.nb.SetSelection(0)
        sizer.Add(self.nb, 1, wx.ALL|wx.EXPAND)
        pack(parent, sizer)

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

        set_choices(self.nbpanels['roimap'].roi1, rois)
        set_choices(self.nbpanels['roimap'].roi2, rois_extra)
        set_choices(self.nbpanels['3color'].rchoice, rois)
        set_choices(self.nbpanels['3color'].gchoice, rois)
        set_choices(self.nbpanels['3color'].bchoice, rois)

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
