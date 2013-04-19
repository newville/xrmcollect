from distutils.core import setup
import py2exe

import epics
import scipy
import scipy.io.netcdf

import matplotlib

ca = epics.ca.initialize_libca()
matplotlib.use('WXAgg')
mpl_data_files = matplotlib.get_py2exe_datafiles()

options = {'optimize': 1,
           'bundle_files': 2,
           'includes': ['epics', 'ctypes', 'wx', 'ConfigParser', 'scipy', 'numpy'],
           'packages': ['epics.ca', 'wx.lib', 'wx.lib.newevent',
                        'h5py', 'scipy.optimize', 'scipy.signal', 'scipy.io',
                        'scipy.interpolate', 'scipy.special', 'scipy.stats',
                        'numpy.random', 'xml.etree', 'xml.etree.cElementTree'], 
           'excludes': ['Tkinter', '_tkinter', 'Tkconstants', 'tcl',
                        '_imagingtk', 'PIL._imagingtk', 'ImageTk',
                        'PIL.ImageTk', 'FixTk''_gtkagg', '_tkagg',
                        'qt', 'PyQt4Gui', 'Carbon', 'email',
                        'IPython', 'PySide'],
           'dll_excludes': ['libgdk-win32-2.0-0.dll',
                            'libgobject-2.0-0.dll']
           }


setup(name="Python Instruments", options={'py2exe': options}, 
      windows=[{'script': 'fastmap_gui.py',
                'icon_resources': [(1, 'fastmap.ico')]}]
      )

