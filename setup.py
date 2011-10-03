#!/usr/bin/env python
from distutils.core import setup
import sys
import lib

no_sqlalchemy="""
*******************************************************
*** WARNING - WARNING - WARNING - WARNING - WARNING ***

       Install or Upgrade SQLAlchemy!

Version 0.6.5 or higher is needed for the xafs database

try:
      easy_install sqlalchemy

*******************************************************
"""

try:
    import sqlalchemy
except:
    print no_sqlalchemy
    sys.exit()
    
setup(name = 'epicscollect',
      version = lib.__version__,
      author = 'Matthew Newville',
      author_email = 'newville@cars.uchicago.edu',
      url         = 'http://xas.org/XasDataLibrary',
      license = 'BSD License',
      description = 'X-ray Data Collection library using Epics',
      package_dir = {'epicscollect': 'lib'},
      packages = ['epicscollect','epicscollect.gui','epicscollect.xmap',
                  'epicscollect.xps', 
                  'epicscollect.utils'])


