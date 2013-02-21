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
from random import randrange

import wx
import wx.lib.agw.flatnotebook as flat_nb
import wx.lib.scrolledpanel as scrolled
import wx.lib.mixins.inspection

import h5py
import numpy as np

from .utils import (SimpleText, pack, popup,
                    add_button, add_menu, add_choice, add_menu)

from xrm_mapfile import GSEXRM_MapFile, isGSEXRM_MapFile

CEN = wx.ALIGN_CENTER|wx.ALIGN_CENTER_VERTICAL
LEFT = wx.ALIGN_LEFT|wx.ALIGN_CENTER_VERTICAL
RIGHT = wx.ALIGN_RIGHT|wx.ALIGN_CENTER_VERTICAL
ALL_CEN =  wx.ALL|CEN

# FILE_WILDCARDS = "X-ray Maps (*.h5)|*.h5|All files (*.*)|*.*"

FILE_WILDCARDS = "X-ray Maps (*.h5)|*.h5"

class MapViewerFrame(wx.Frame):
    _about = """XRF Map Viewer
  Matt Newville <newville @ cars.uchicago.edu>
  """
    def __init__(self, conffile=None,  **kwds):

        kwds["style"] = wx.DEFAULT_FRAME_STYLE
        wx.Frame.__init__(self, None, -1, size=(680, 600),  **kwds)

        self.data = None
        self.filemap = {}
        self.larch = None

        self.Font14=wx.Font(14, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")
        self.Font12=wx.Font(12, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")
        self.Font11=wx.Font(11, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")
        self.Font10=wx.Font(10, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")
        self.Font9 =wx.Font(9, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")

        self.SetTitle("GSE XRM MapViewer")
        self.SetSize((850, 775))
        self.SetFont(self.Font9)

        self.createMainPanel()
        self.createMenus()
        self.statusbar = self.CreateStatusBar(2, 0)
        self.statusbar.SetStatusWidths([-4, -1])
        statusbar_fields = ["Initializing....", " "]
        for i in range(len(statusbar_fields)):
            self.statusbar.SetStatusText(statusbar_fields[i], i)

    def createMainPanel(self):
        sizer = wx.BoxSizer(wx.VERTICAL)
        splitter  = wx.SplitterWindow(self, style=wx.SP_LIVE_UPDATE)
        splitter.SetMinimumPaneSize(175)

        self.filelist  = wx.ListBox(splitter)
        self.filelist.SetBackgroundColour(wx.Colour(255, 255, 255))
        self.filelist.Bind(wx.EVT_LISTBOX, self.ShowFile)

        self.detailspanel = self.createDetailsPanel(splitter)

        splitter.SplitVertically(self.filelist, self.detailspanel, 1)
        sizer = wx.BoxSizer(wx.VERTICAL)
        sizer.Add(splitter, 1, wx.GROW|wx.ALL, 5)
        wx.CallAfter(self.init_larch)
        pack(self, sizer)

    def createDetailsPanel(self, parent):
        panel = wx.Panel(parent)
        panel.SetMinSize((600, 250))
        sizer = wx.GridBagSizer(8, 10)
        self.title = SimpleText(panel, 'initializing...')
        ir = 0
        sizer.Add(self.title, (ir, 0), (1, 8), ALL_CEN, 2)
        # x-axis

        #         self.x_choice = add_choice(panel, choices=[], size=(120, -1))
        #         self.x_op     = add_choice(panel, choices=('', 'log'), size=(80, -1))
        # self.xchoice.SetItems(list of choices)
        # self.xchoice.SetStringSelection(default string)


        ir += 1
        sizer.Add(wx.StaticLine(panel, size=(675, 3), style=wx.LI_HORIZONTAL),
                  (ir, 1), (1, 10), wx.ALIGN_CENTER)
        #ir += 1
        #sizer.Add(SimpleText(panel, 'Should add fitting options'),
        #          (ir, 1), (1, 10), wx.ALIGN_CENTER)

        #ir += 1
        #sizer.Add(wx.StaticLine(panel, size=(675, 3), style=wx.LI_HORIZONTAL),
        #          (ir, 1), (1, 10), wx.ALIGN_CENTER)

        pack(panel, sizer)
        return panel

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

    def ShowFile(self, evt=None, filename=None, ftype=None, **kws):
        if filename is None and evt is not None:
            filename = evt.GetString()

        print 'Show File', filename, ftype, evt


    def createMenus(self):
        self.menubar = wx.MenuBar()
        # file
        fmenu = wx.Menu()
        add_menu(self, fmenu, "&Open Map\tCtrl+O",
                 "Read Map File or Folder",  self.onRead)

        fmenu.AppendSeparator()
        add_menu(self, fmenu, "&Quit\tCtrl+Q",
                  "Quit program", self.onClose)

        self.menubar.Append(fmenu, "&File")
        self.SetMenuBar(self.menubar)

    def onAbout(self,evt):
        dlg = wx.MessageDialog(self, self._about,"About MapViewer",
                               wx.OK | wx.ICON_INFORMATION)
        dlg.ShowModal()
        dlg.Destroy()

    def onClose(self,evt):
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
                            wildcard=FILE_WILDCARDS,
                            style=wx.OPEN)
        path, read = None, False
        if dlg.ShowModal() == wx.ID_OK:
            read = True
            path = dlg.GetPath().replace('\\', '/')
            if path in self.filemap:
                read = popup(self, "Re-read file '%s'?" % path, 'Re-read file?')
        dlg.Destroy()

        if read:
            if not isGSEXRM_MapFile(path):
                popup(self, "'%s' doesn't seem to be a Map File" % path,
                      "Not a Map file?")
            else:
                parent, fname = os.path.split(path)
                self.filelist.Append(fname)
                self.filemap[fname] = GSEXRM_MapFile(fname)
                self.ShowFile(filename=path)

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
