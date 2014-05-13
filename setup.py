# Copyright (C) 2011-2012 CRS4.
#

import glob
import os
from distutils.core import setup

#############################################################################
# main
#############################################################################

# chdir to project's root directory (where this file is located)
os.chdir(os.path.abspath(os.path.dirname(__file__)))

NAME = 'hadoop_galaxy'
URL = "http://www.crs4.it"
# DOWNLOAD_URL = ""
LICENSE = 'BSD'
CLASSIFIERS = [
  "Programming Language :: Python",
  "License :: OSI Approved :: Revised BSD License",
  "Operating System :: POSIX :: Linux",
  "Topic :: Scientific/Engineering :: Bio-Informatics",
  "Intended Audience :: Science/Research",
  ]
PLATFORMS = ["Linux"]
AUTHOR_INFO = [
  ("Luca Pireddu", "luca.pireddu@crs4.it"),
  ("Simone Leo", "simone.leo@crs4.it"),
  ("Gianluigi Zanetti", "gianluigi.zanetti@crs4.it"),
  ]
MAINTAINER_INFO = [
  ("Luca Pireddu", "luca.pireddu@crs4.it"),
  ]
AUTHOR = ", ".join(t[0] for t in AUTHOR_INFO)
AUTHOR_EMAIL = ", ".join("<%s>" % t[1] for t in AUTHOR_INFO)
MAINTAINER = ", ".join(t[0] for t in MAINTAINER_INFO)
MAINTAINER_EMAIL = ", ".join("<%s>" % t[1] for t in MAINTAINER_INFO)


setup(name=NAME,
   #description=DESCRIPTION,
   #long_description=LONG_DESCRIPTION,
   url=URL,
   #download_url=DOWNLOAD_URL,
   license=LICENSE,
   classifiers=CLASSIFIERS,
   author=AUTHOR,
   author_email=AUTHOR_EMAIL,
   maintainer=MAINTAINER,
   maintainer_email=MAINTAINER_EMAIL,
   platforms=PLATFORMS,
   packages=['hadoop_galaxy',
             ],
   scripts=glob.glob("scripts/*"),
)
