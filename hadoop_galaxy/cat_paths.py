#!/usr/bin/env python

import argparse
import logging
import os
import sys
import time
from urlparse import urlparse

import hadoop_galaxy.pathset as pathset
import pydoop.hdfs as phdfs

_log = logging.getLogger('cat_paths')

def link_file(src_url, dest_path):
    # Both source and destination should be on mounted file systems.
    u = urlparse(src_url)
    try:
        if os.path.exists(dest_path):
            os.unlink(dest_path)
    except OSError:
        pass
    try:
        _log.debug("hard linking %s to %s", u.path, dest_path)
        os.link(u.path, dest_path)
        return
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

def perform_copy(src_pathset, output_path):
    total = len(src_pathset)
    n_bytes = 0

    def progress(i):
      _log.info("Processed %s of %s (%0.1f %%). Copied %0.1f MB", i, total,
              100*(float(i) / total), float(n_bytes) / 2**20)
    progress(0)

    first_src_uri = iter(src_pathset).next()
    u = urlparse(first_src_uri)

    if len(src_pathset) == 1 and u.scheme == 'file' and os.path.isfile(u.path):
        _log.debug("Pathset contains single local file")
        try:
            link_file(first_src_uri, output_path)
            progress(total)
            return
        except OSError:
            # linking failed.  Continue with simple copy
            pass

    start_time = time.time()
    with open_file(output_path, 'w') as output_fd:
        try:
          for idx, p in enumerate(src_pathset):
            if phdfs.path.isdir(p):
                n_bytes += append_dir(p, output_fd)
            else:
                n_bytes += append_file(p, output_fd)
            if idx % 5 == 0:
                progress(idx + 1)
            progress(len(src_pathset))
        except StandardError as e:
            _log.exception(e)
            try:
                if os.path.exists(output_path):
                    _log.error('Trying to remove destination file %s', output_path)
                    phdfs.rmr(output_path)
            except StandardError:
                pass
            raise e
    end_time = time.time()
    mb = float(n_bytes) / 2**20
    duration = end_time - start_time
    _log.info("Wrote %0.1f MB in %d seconds (%0.1f MB/s)", mb, round(duration), mb / (min(0.1, duration)))

def parse_args(args):
    description = "Simple concatenate the data referenced by a pathset into a single file"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('input_pathset', help="Input pathset")
    parser.add_argument('output_file', help="Output pathset file to be written")
    options = parser.parse_args(args)

    if not os.access(options.input_pathset, os.R_OK):
        parser.error("Can't read specified input path %s" % options.input_pathset)

    output_file = options.output_file
    u = urlparse(output_file)
    if u.scheme == 'file' or not u.scheme:
        # if the output path isn't specified as a full URI we prefer
        # it to be on the local file system
        output_file = 'file://' + os.path.abspath(u.path)
    else:
        parser.error("Output path must be on locally mounted file system")

    return options.input_pathset, output_file

def main(args):
  input_pathset, output_path = parse_args(args)
  pset = pathset.FilePathset.from_file(input_pathset)

  try:
      perform_copy(pset, output_path)
  except StandardError as e:
    _log.critical("IOError copying pathset to %s", output_path)
    _log.exception(e)
    sys.exit(1)
