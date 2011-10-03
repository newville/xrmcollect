__version__ = '0.2'

from .gui.fastmap_gui import FastMapGUI
from .xmap import read_xmap_netcdf, MultiXMAP, DXP, MCA

import utils

from config import FastMapConfig, conf_files, default_conf
from mapper import mapper
