# BEGIN_COPYRIGHT
# 
# Copyright (C) 2014 CRS4.
# 
# This file is part of hadoop-galaxy, released under the terms of the BSD
# 3-Clause License <http://opensource.org/licenses/BSD-3-Clause>.
# 
# END_COPYRIGHT

"""
Hadoop-Galaxy provides tools for integrating Hadoop and Galaxy.

Hadoop: <http://hadoop.apache.org>
Galaxy: <http://galaxyproject.org>
"""

import glob
import os
from setuptools import setup

#############################################################################
# main
#############################################################################

# chdir to project's root directory (where this file is located)
os.chdir(os.path.abspath(os.path.dirname(__file__)))

NAME = 'hadoop-galaxy'
DESCRIPTION = __doc__.split("\n", 1)[0]
LONG_DESCRIPTION = __doc__
URL = "https://github.com/crs4/hadoop-galaxy"
DOWNLOAD_URL = URL
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
   description=DESCRIPTION,
   long_description=LONG_DESCRIPTION,
   url=URL,
   download_url=DOWNLOAD_URL,
   license=LICENSE,
   classifiers=CLASSIFIERS,
   author=AUTHOR,
   author_email=AUTHOR_EMAIL,
   maintainer=MAINTAINER,
   maintainer_email=MAINTAINER_EMAIL,
   platforms=PLATFORMS,
   zip_safe=False,
   install_requires=['pydoop'],
   packages=['hadoop_galaxy',
             ],
   scripts=glob.glob("scripts/*"),
)
