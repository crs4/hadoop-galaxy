
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
import sys
import time
from urlparse import urlparse

import pydoop.hdfs as phdfs

import hadoop_galaxy.pathset as pathset
from hadoop_galaxy import log as _log
from hadoop_galaxy.utils import config_logging

def link_file(src_url, dest_path, delete_source=False):
    # Both source and destination should be on mounted file systems.
    u = urlparse(src_url)
    try:
        if os.path.exists(dest_path):
            os.unlink(dest_path)
    except OSError:
        pass
    try:
        _log.info("Hard linking %s to %s instead of copying", u.path, dest_path)
        os.link(u.path, dest_path)
        if delete_source:
            _log.info("As requested, removing source file %s", u.path)
            try:
                os.unlink(u.path)
            except OSError as e:
                _log.warn("Failed to remove source file %s", u.path)
                _log.warn(str(e))
    except OSError as e:
       _log.info("failed to hard link %s (Reason: %s). Will copy.", u.path, str(e))
       raise

def append_file(src_url, dest_fd):
    ten_mb = 10 * 2**20
    n_bytes = 0

    with open_file(src_url) as input_fd:
        buf = input_fd.read(ten_mb)
        while len(buf) > 0:
            dest_fd.write(buf)
            n_bytes += len(buf)
            buf = input_fd.read(ten_mb)
    return n_bytes

def append_dir(d, output):
    """
    Appends the contents of all files within directory d to the output.
    The contents within the directory are ordered by name.

    Returns the number of bytes appended to the output.
    """
    contents = [ e for e in sorted(phdfs.ls(d)) if not os.path.basename(e).startswith('_') ]
    _log.debug("Appending %s items from directory %s", len(contents), d)
    n_bytes = 0
    for c in contents:
        if phdfs.path.isdir(c):
            _log.debug("Recursively descending into %s", c)
            n_bytes += append_dir(c, output)
        else:
            n_bytes += append_file(c, output)
    return n_bytes

def open_file(path, mode='r'):
    u = urlparse(phdfs.path.abspath(path))
    if u.scheme == 'file':
        return open(u.path, mode)
    else:
        return phdfs.open(path, mode)

def perform_copy(src_pathset, output_uri, delete_source=False):
    """
    :param src_pathset: Pathset from which to copy data
    :param output_uri: URI to which data will be written.
    :param delete_source: if True, the files/directories referenced by `src_pathset` will be deleted after successully copying their data to `output_path`.
    """
    # validate input
    parsed_output = urlparse(output_uri)
    if not parsed_output.scheme:
        raise ValueError("BUG! output_uri must be a full URI. Got %s" % output_uri)

    total = len(src_pathset)
    n_bytes = 0

    _log.info("Concatenating %s paths to %s", total, output_uri)

    def progress(i):
      _log.info("Processed %s of %s (%0.1f %%). Copied %0.1f MB", i, total,
              100*(float(i) / total), float(n_bytes) / 2**20)
    progress(0)

    first_src_uri = iter(src_pathset).next()
    u = urlparse(first_src_uri)

    if len(src_pathset) == 1 and u.scheme == 'file' and os.path.isfile(u.path) and parsed_output.scheme == 'file':
        # Handle single file on local file system as a special case
        _log.debug("Pathset contains single local file. Trying to hard link")
        try:
            link_file(first_src_uri, parsed_output.path, delete_source)
            progress(total)
            return
        except OSError:
            _log.debug("linking failed.  Continue with simple copy")

    start_time = time.time()
    with open_file(output_uri, 'w') as output_fd:
        _log.debug("output_path %s opened for writing", output_uri)
        try:
          for idx, p in enumerate(src_pathset):
            _log.debug("appending path %s", p)
            if phdfs.path.isdir(p):
                n_bytes += append_dir(p, output_fd)
            else:
                n_bytes += append_file(p, output_fd)
            if idx % 5 == 0:
                progress(idx + 1)
            progress(len(src_pathset))
        except StandardError as e:
            _log.exception(e)
            _log.info('Trying to clean-up partial output file %s', output_uri)
            try:
                phdfs.rmr(output_uri)
            except StandardError:
                pass
            raise e
    end_time = time.time()
    mb = float(n_bytes) / 2**20
    duration = end_time - start_time
    _log.info("Concatenation finished. Wrote %0.1f MB in %d seconds (%0.1f MB/s)", mb, round(duration), mb / (min(0.1, duration)))
    if delete_source:
        _log.info("Deleting source data")
        _delete_pathset_data(src_pathset)

def _delete_pathset_data(pset):
    for path in pset:
        try:
            phdfs.rmr(path)
        except IOError as e:
            _log.warn("Unable to delete source path %s", path)
            _log.warn(str(e))

def parse_args(args):
    description = "Simple concatenate the data referenced by a pathset into a single file"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('input_pathset', help="Input pathset")
    parser.add_argument('output_file', help="Output file to be written")
    parser.add_argument('--delete-source', action='store_true', default=False,
            help="Delete the data referenced by the source pathset after it has been concatenated into the destination file.")
    parser.add_argument('--log-level',
            choices=['debug', 'info', 'warn', 'error', 'critical'],
            default='info')

    options = parser.parse_args(args)

    if not os.access(options.input_pathset, os.R_OK):
        parser.error("Can't read specified input path %s" % options.input_pathset)

    u = urlparse(options.output_file)
    if not u.scheme:
        # if the output path isn't specified as a full URI we prefer
        # it to be on the local file system
        options.output_file = 'file://' + os.path.abspath(u.path)

    return options

def main(args=None):
  try:
      options = parse_args(args or sys.argv[1:])
      config_logging(options.log_level)
      _log.debug("arguments parsed: %s", options)

      pset = pathset.FilePathset.from_file(options.input_pathset)
      perform_copy(pset, options.output_file, options.delete_source)
      return 0
  except StandardError as e:
      _log.critical("IOError copying pathset to %s", options.output_file)
      _log.critical(str(e))
      return 1
