__version__ = '0.2'

from .xmap import read_xmap_netcdf, MultiXMAP, DXP, MCA

import .util

from config import FastMapConfig, conf_files, default_conf
from mapper import mapper
#from .gui.fastmap_gui import FastMapGUI
