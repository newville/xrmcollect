#!/usr/bin/env python
"""
GUI Frame for XRF display, reading either GSECARS MCA files,
or extracted from GSEXRM HDF5 files

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
import matplotlib
from wxmplot import PlotFrame, PlotPanel

from .utils import (SimpleText, EditableListBox, FloatCtrl,
                    Closure, pack, popup,
                    add_button, add_menu, add_choice, add_menu)

from ..io.xrm_mapfile import (GSEXRM_MapFile, GSEXRM_FileStatus,
                              GSEXRM_Exception, GSEXRM_NotOwner)

CEN = wx.ALIGN_CENTER|wx.ALIGN_CENTER_VERTICAL
LEFT = wx.ALIGN_LEFT|wx.ALIGN_CENTER_VERTICAL
RIGHT = wx.ALIGN_RIGHT|wx.ALIGN_CENTER_VERTICAL
ALL_CEN =  wx.ALL|CEN
ALL_LEFT =  wx.ALL|LEFT
ALL_RIGHT =  wx.ALL|RIGHT


FNB_STYLE = flat_nb.FNB_NO_X_BUTTON|flat_nb.FNB_SMART_TABS|flat_nb.FNB_NO_NAV_BUTTONS

FILE_WILDCARDS = "X-ray Maps (*.h5)|*.h5|All files (*.*)|*.*"

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

NOT_GSEXRM_FOLDER = """The Folder
   '%s'
doesn't seem to be a Map Folder
"""
FILE_ALREADY_READ = """The File
   '%s'
has already been read.
"""


def set_choices(choicebox, choices):
    index = 0
    try:
        current = choicebox.GetStringSelection()
        if current in choices:
            index = choices.index(current)
    except:
        pass
    choicebox.Clear()
    choicebox.AppendItems(choices)
    choicebox.SetStringSelection(choices[index])


class XRFDisplayFrame(wx.Frame):
    _about = """XRF Spectral Viewer
  Matt Newville <newville @ cars.uchicago.edu>
  """
    def __init__(self, **kws):

        kws["style"] = wx.DEFAULT_FRAME_STYLE
        wx.Frame.__init__(self, None, -1, size=(700, 400),  **kws)

        self.data = None
        self.plotframe = None

        self.Font14 = wx.Font(14, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")
        self.Font12 = wx.Font(12, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")
        self.Font11 = wx.Font(11, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")
        self.Font10 = wx.Font(10, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")
        self.Font9  = wx.Font(9, wx.SWISS, wx.NORMAL, wx.BOLD, 0, "")

        self.SetTitle("XRF Spectra Viewer")
        self.SetFont(self.Font9)

        self.createMainPanel()
        self.createMenus()
        self.statusbar = self.CreateStatusBar(2, 0)
        self.statusbar.SetStatusWidths([-3, -1])
        statusbar_fields = ["Initializing....", " "]
        for i in range(len(statusbar_fields)):
            self.statusbar.SetStatusText(statusbar_fields[i], i)

    def createMainPanel(self):
        self.wids = {}
        ctrlpanel = self.ctrlpanel = wx.Panel(self)
        roipanel = self.roipanel = wx.Panel(self)
        plotpanel = self.plotpanel = PlotPanel(self, fontsize=7,
                                               axisbg='#FDFDFA',
                                               axissize=[0.08, 0.08, 0.88, 0.88],
                                               output_title='test.xrf',
                                               messenger=self.write_message)
        ## need to customize cursor modes:
        # plotpane.add_cursor_mode('zoom', .....)
        # plotpane.add_cursor_mode('report', .....)
        sizer = wx.GridBagSizer(10, 4)

        labstyle = wx.ALIGN_LEFT|wx.ALIGN_BOTTOM|wx.EXPAND
        ctrlstyle = wx.ALIGN_LEFT|wx.ALIGN_BOTTOM
        rlabstyle = wx.ALIGN_RIGHT|wx.RIGHT|wx.TOP|wx.EXPAND
        txtstyle=wx.ALIGN_LEFT|wx.ST_NO_AUTORESIZE|wx.TE_PROCESS_ENTER

        def txt(label, panel, size=100):
            return wx.StaticText(panel, label=label, size=(size, -1), style=labstyle)

        self.wids['ylog'] = add_choice(ctrlpanel, choices=['log', 'linear'], size=(90, -1))
        self.wids['ylog'].SetSelection(0)

        sizer.Add(txt('Y Scale:', ctrlpanel),  (0, 0), (1, 1), labstyle)
        sizer.Add(self.wids['ylog'],           (1, 0), (1, 1), ctrlstyle)

        ctrlpanel.SetSizer(sizer)
        sizer.Fit(ctrlpanel)

        msizer = wx.BoxSizer(wx.HORIZONTAL)
        msizer.Add(ctrlpanel, 0, wx.GROW|wx.ALL, 5)
        msizer.Add(plotpanel, 1, wx.GROW|wx.ALL, 5)
        msizer.Add(roipanel, 0, wx.GROW|wx.ALL, 5)
        wx.CallAfter(self.init_larch)
        pack(self, msizer)



    def init_larch(self):
        t0 = time.time()
        from larch import Interpreter
        from larch.wxlib import inputhook
        self.larch = Interpreter()
        self.larch.symtable.set_symbol('_sys.wx.wxapp', wx.GetApp())
        self.larch.symtable.set_symbol('_sys.wx.parent', self)
        self.SetStatusText('ready')
        self.datagroups = self.larch.symtable


    def createMenus(self):
        self.menubar = wx.MenuBar()
        fmenu = wx.Menu()
        add_menu(self, fmenu, "&Read MCA Spectra File\tCtrl+O",
                 "Read GSECARS MCA File",  self.onReadMCAFile)
        add_menu(self, fmenu, "&Read XRM Map File\tCtrl+F",
                 "Read GSECARS XRM MAp File",  self.onReadGSEXRMFile)
        add_menu(self, fmenu, "&Open Epics MCA\tCtrl+E",
                 "Read Epics MCA",  self.onOpenEpicsMCA)

        fmenu.AppendSeparator()
        add_menu(self, fmenu, "&Save MCA File\tCtrl+S",
                 "Save GSECARS MCA File",  self.onSaveMCAFile)
        add_menu(self, fmenu, "&Save ASCII Column File\tCtrl+A",
                 "Save Column File",  self.onSaveColumnFile)

        fmenu.AppendSeparator()
        add_menu(self, fmenu, "&Quit\tCtrl+Q",
                  "Quit program", self.onClose)

        omenu = wx.Menu()
        add_menu(self, omenu, "&Calibrate Energy\tCtrl+B",
                 "Calibrate Energy",  self.onCalibrateEnergy)
        add_menu(self, omenu, "&Fit background\tCtrl+G",
                 "Fit smooth background",  self.onFitbackground)

        self.menubar.Append(fmenu, "&File")
        self.menubar.Append(omenu, "&Options")
        self.SetMenuBar(self.menubar)


    def onReadMCAFile(self, event=None):
        pass

    def onReadGSEXRMFile(self, event=None, **kws):
        print '  onReadGSEXRMFile   '
        pass

    def onOpenEpicsMCA(self, event=None, **kws):
        print '  onOpenEpicsMCA   '
        pass

    def onSaveMCAFile(self, event=None, **kws):
        print '  onSaveMCAFile   '
        pass

    def onSaveColumnFile(self, event=None, **kws):
        print '  onSaveColumnFile   '
        pass

    def onCalibrateEnergy(self, event=None, **kws):
        print '  onCalibrateEnergy   '
        pass

    def onFitbackground(self, event=None, **kws):
        print '  onFitbackground   '
        pass

    def write_message(self, s, panel=0):
        """write a message to the Status Bar"""
        self.SetStatusText(s, panel)

    def onAbout(self,evt):
        dlg = wx.MessageDialog(self, self._about,"About GSEXRM MapViewer",
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


    def onReadFile(self, evt=None):
        dlg = wx.FileDialog(self, message="Read Map File",
                            defaultDir=os.getcwd(),
                            wildcard=FILE_WILDCARDS,
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
                popup(self, NOT_GSEXRM_FILE % fname,
                      "Not a Map file!")
                return
            if fname not in self.filemap:
                self.filemap[fname] = xrmfile
            if fname not in self.filelist.GetItems():
                self.filelist.Append(fname)
            if self.check_ownership(fname):
                self.process_file(fname)
            self.ShowFile(filename=fname)


class XRFApp(wx.App, wx.lib.mixins.inspection.InspectionMixin):
    def __init__(self, **kws):
        wx.App.__init__(self)

    def OnInit(self):
        self.Init()
        frame = XRFDisplayFrame() #
        frame.Show()
        self.SetTopWindow(frame)
        return True

if __name__ == "__main__":
    XRFApp().MainLoop()
