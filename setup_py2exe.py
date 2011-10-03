from distutils.core import setup
import py2exe

setup(name="Python Instruments",
      windows=[{'script': 'fastmap_gui.py',
                'icon_resources': [(1, 'fastmap.ico')]}],
      options = dict(py2exe=dict(optimize=0,
                                 includes=['epics', 'ctypes', 'wx'],
                                 excludes=['tcl', 'Tkinter','Tkconstants'],
                                 )
                     )
      )

