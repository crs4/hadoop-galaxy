# BEGIN_COPYRIGHT
#
# Copyright (C) 2014 CRS4.
#
# This file is part of hadoop-galaxy, released under the terms of the BSD
# 3-Clause License <http://opensource.org/licenses/BSD-3-Clause>.
#
# END_COPYRIGHT

import logging
import os
import subprocess
import sys
import urlparse

import pydoop
import pydoop.hdfs as phdfs

EnvLogLevel = 'HADOOP_GALAXY_LOG_LEVEL'

def config_logging(log_level='INFO'):
    if isinstance(log_level, basestring):
        level = getattr(logging, log_level.upper())
    elif log_level:
        level = log_level
    elif os.environ.get(EnvLogLevel):
        level = os.environ.get(EnvLogLevel).upper()
        if hasattr(logging, level):
            level = getattr(logging, level)
        else:
            print >> sys.stderr, "Ignoring value of %s because it's not a valid log level" % EnvLogLevel
            level = logging.INFO # default
    else:
        level = logging.INFO # default
    logging.basicConfig(level=level)

def _logger_has_handler(logger):
    l = logger
    while l:
        if l.handlers:
            return True
        else:
            l = l.parent
    return False

def print_err(*args):
    """
    Print and error to the logger, if the module-level logger is configured.
    Else print directly to stderr.
    """
    log_string = ' '.join(args)
    log = logging.getLogger('HadoopGalaxy')
    if _logger_has_handler(log):
        log.error(log_string)
    else:
        print >> sys.stderr, log_string

class Uri(object):
    def __init__(self, *args):
        if len(args) == 1 and all(hasattr(args[0], attr) for attr in ('scheme', 'netloc', 'path')):
            self.scheme = args[0].scheme
            self.netloc = args[0].netloc
            self.path = args[0].path
        elif len(args) == 3:
            self.scheme, self.netloc, self.path = args
        else:
            raise ValueError()
        if self.scheme == 'file':
            if self.netloc:
                raise ValueError("Can't specify a netloc with file: scheme")
            if self.path and not self.path.startswith('/'):
                raise ValueError("Must use absolute paths with file: scheme (found %s)" % self.path)
        if self.netloc and not self.scheme:
            raise ValueError("Can't specify a host without an access scheme")

    def geturl(self):
        if self.scheme:
            url = "%s://%s%s" % (self.scheme, self.netloc, self.path)
        else:
            url = self.path
        return url

def expand_paths(datapath_uri):
    """
    If a URI contains wildcards, this function expands them.

    Returns a list of URIs.
    """
    # simple case:  the path simply exists
    if phdfs.path.exists(datapath_uri.geturl()):
        return [datapath_uri.geturl()]

    # second case:  the path doesn't exist as it is.  It may contain wildcards, so we try
    # listing the datapath with hadoop dfs.  If we were to list with
    # pydoop.hdfs.ls we'd have to implement hadoop wildcards ourselves (perhaps with fnmatch)

    def process(ls_line):
        path = ls_line[(ls_line.rindex(' ') + 1):]
        url = Uri(urlparse.urlparse(path))
        url.scheme = datapath_uri.scheme
        url.netloc = datapath_uri.netloc
        return url.geturl()

    try:
        # run -ls with hadoop dfs the process the output.
        # We drop the first line since it's something like "Found xx items".
        ls_output = subprocess.check_output([pydoop.hadoop_exec(), 'dfs', '-ls', datapath_uri.geturl()]).rstrip('\n').split('\n')[1:]
        # for each data line, run apply the 'process' function to transform it into a full URI
        return map(process, ls_output)
    except subprocess.CalledProcessError as e:
        print_err("Could not list datapath %s.  Please check whether it exists" % datapath_uri.geturl())
        print_err("Message:", str(e))
        sys.exit(1)

def get_abs_executable_path(executable_name, env=None):
    if env is None:
        env = os.environ
    # now verify that we find the executable in the PATh
    if os.path.isabs(executable_name):
        full_path = executable_name
        if not os.access(full_path, os.X_OK):
            raise RuntimeError("Path %s is not an executable" % full_path)
    else:
        paths = env.get('PATH', '')
        try:
            full_path = \
              next(os.path.join(p, executable_name)
                   for p in paths.split(os.pathsep)
                   if os.access(os.path.join(p, executable_name), os.X_OK))
        except StopIteration:
            raise RuntimeError(
              ("The tool %s either isn't in the PATH or isn't executable.\n" +
               "\nPATH: %s") % (executable_name, paths))
    return full_path
