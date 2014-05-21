
# BEGIN_COPYRIGHT
#
# Copyright (C) 2014 CRS4.
#
# This file is part of hadoop-galaxy, released under the terms of the BSD
# 3-Clause License <http://opensource.org/licenses/BSD-3-Clause>.
#
# END_COPYRIGHT

import argparse
import os
import subprocess
import sys
import urlparse

import pydoop
import pydoop.hdfs as phdfs

from hadoop_galaxy import log
from hadoop_galaxy.pathset import FilePathset
from hadoop_galaxy.utils import Uri, expand_paths, print_err

ValidModes = ('default', 'local')

def get_default_fs():
    root_ls = phdfs.ls('/')
    if root_ls:
        uri = Uri(urlparse.urlparse(root_ls[0]))
        return uri
    else:
        raise RuntimeError("Could not determine URI of default file system.  It's empty.")

def resolve_datapath(mode, datapath):
    """
    Returns a full URI for datapath
    """
    u = Uri(urlparse.urlparse(datapath))

    if not u.path:
        raise RuntimeError("blank path in %s" % datapath)

    if mode == 'default' and not u.scheme: # datapath not specified completely. Assume it's on the default fs
        u = Uri(urlparse.urlparse(phdfs.path.abspath(u.path)))
    elif mode == 'local':
        if u.scheme and u.scheme != 'file':
            raise RuntimeError("Specified local mode but datapath is a URI with scheme %s (expected no scheme or 'file')" % u.scheme)
        # force the 'file' scheme and make the path absolute
        u.scheme = 'file'
        u.netloc = ''
        u.path = os.path.abspath(datapath)
    return u

def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Make a pathset file from one or more paths")
    parser.add_argument('--force-local', action='store_true', help="Force path to be local (i.e., URI starting with file://")
    parser.add_argument('--data-format', help="Set the type of the pathset contents to this data type (e.g. 'fastq')")
    parser.add_argument('output_path', help="Pathset file to write")
    parser.add_argument('paths', nargs='*', help="Paths to be written to the pathset. Alternatively, provide the on stdin, one per line.")
    return parser.parse_args(args)

def test_hadoop():
    """
    Test the hadoop configuration.
    Calls sys.exit if test fails.
    """
    cmd = [pydoop.hadoop_exec(), 'dfs', '-stat', 'file:///']
    try:
        subprocess.check_output(cmd)
    except subprocess.CalledProcessError as e:
        print_err("Error running hadoop program.  Please check your environment (tried %s)" % ' '.join(cmd))
        print_err("Message:", str(e))
        sys.exit(2)

def do_work(options):
    mode = 'local' if options.force_local else 'default'
    output_path = options.output_path
    data_format = options.data_format # may be None

    if options.paths:
        data_paths = options.paths
    else:
        log.info("reading paths from stdin")
        data_paths = [ line.rstrip() for line in sys.stdin ]
    log.info("read %s paths", len(data_paths))

    # this is the real work
    uris = [ resolve_datapath(mode, p) for p in data_paths ]
    expanded_uris = [ u for wild in uris for u in expand_paths(wild) ]
    output_pathset = FilePathset(*expanded_uris)
    output_pathset.set_datatype(data_format)
    with open(output_path, 'w') as f:
        output_pathset.write(f)

def main(args=None):
    args = args or sys.argv[1:]
    options = parse_args(args)
    test_hadoop() # calls sys.exit if test fails
    do_work(options)

