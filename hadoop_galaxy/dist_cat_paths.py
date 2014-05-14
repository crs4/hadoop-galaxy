#!/usr/bin/env python

import argparse
import logging
import os
import time
from urlparse import urlparse
from uuid import uuid4
import pydoop.app.main as pydoop_main
import pydoop.hdfs as phdfs

from hadoop_galaxy.pathset import FilePathset

_TenMB = 10 * 2**20

def serialize(src_path_info, dest_path, dest_pos):
    return '\t'.join( map(str, (src_path_info.path, 0, src_path_info.size,
        dest_path, dest_pos)) )

def unserialize(line):
    fields = line.rstrip().split('\t')
    if len(fields) != 5:
        raise ValueError("Invalid format.  Expected 5 lines but found %s. Line: '%s'" % (len(fields), line))
    return dict(
        src_path=fields[0],
        src_pos=int(fields[1]),
        src_size=int(fields[2]),
        dest_path=fields[3],
        dest_pos=int(fields[4]))

def open_file(path, mode='r'):
    u = urlparse(phdfs.path.abspath(path))
    if u.scheme == 'file':
        return open(u.path, mode)
    else:
        return phdfs.open(path, mode)

def bytes_to_mb(b):
    return b / float(2**20)

class CopyState(object):
    def __init__(self, total_bytes):
        self._total_bytes = total_bytes
        self._current_byte = 0
        self._current_time = time.time()
        self._current_speed = 0

    @property
    def current_byte(self):
        return self._current_byte

    @property
    def total_bytes(self):
        return self._total_bytes

    @property
    def bytes_left(self):
        return self._total_bytes - self._current_byte

    @property
    def current_time(self):
        return self._current_time

    @property
    def current_speed(self):
        return self._current_speed

    @property
    def fraction(self):
        if self._total_bytes == 0:
            return 1.0
        else:
            return float(self._current_byte) / self._total_bytes

    def update(self, new_byte_pos):
        the_time = time.time()
        self._current_speed = (new_byte_pos - self._current_byte) / (the_time - self._current_time)
        self._current_byte = new_byte_pos
        self._current_time = the_time


def mapper(_, line, writer):
    record = unserialize(line)

    state = CopyState(record['src_size'])

    def statusline():
        msg = "Copying %s of %s (%0.1f %% at %0.1f MB/s): %s [%s, %s) to %s at pos %s" % \
            (state.current_byte, state.total_bytes,
            round(100 * state.fraction),
            bytes_to_mb(state.current_speed),
            record['src_path'], state.current_byte, state.current_byte + state.total_bytes,
            record['dest_path'], record['dest_pos'])
        return msg
    writer.status(statusline())

    u = urlparse(record['dest_path'])
    if u.scheme != 'file':
        raise ValueError("output scheme must be 'file'. Found: %s" % u.scheme)
    if not os.path.exists(u.path):
        raise RuntimeError("Output file %s doesn't exist.  File a bug report" % u.path)

    with open_file(record['src_path']) as input_fd, \
          open(u.path, 'r+') as output_fd:

        input_fd.seek(record['src_pos'])
        output_fd.seek(record['dest_pos'])
        bytes_left = state.bytes_left

        buf = input_fd.read(min(_TenMB, bytes_left))
        while len(buf) > 0 and bytes_left > 0:
            output_fd.write(buf)
            state.update(state.current_byte + len(buf))
            writer.count('file bytes written', len(buf))
            buf = input_fd.read(min(_TenMB, bytes_left))
            writer.status(statusline())
    writer.count('files catted', 1)

def parse_args(args):
    description = "Use Hadoop to concatenate the data referenced by a pathset into a\n" + \
    "single file on a parallel shared file system"
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('input_pathset', help="Input pathset")
    parser.add_argument('output_file',
            help="Output file. MUST be on a mounted file system accessible from all Hadoop nodes")
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

class _PathInfo(object):
    def __init__(self, path, size):
        self.path = path
        self.size = size

class _CopyJobInfo(object):
    def __init__(self, src_path_info, dest_start_pos):
        self._src_info = src_path_info
        self._dest_start = dest_start_pos

    @property
    def src_path(self):
        return self._src_info.path

    @property
    def slice_size(self):
        return self._src_info.size

    @property
    def dest_start(self):
        return self._dest_start

    @property
    def dest_end(self):
        """One past the last byte to be written."""
        return self._dest_start + self._src_info.size

    @property
    def src_is_local(self):
        return self.src_path.startswith('file:')

class DistCatPaths(object):
    def __init__(self):
        self._src_paths = None
        self._output_path = None
        self._input_pathset = None
        self.log = logging.getLogger('cat paths')

    @property
    def output_path(self):
        return self._output_path

    @output_path.setter
    def output_path(self, p):
        u = urlparse(p)
        if u.scheme == 'file' or not u.scheme:
            self._output_path = 'file://' + os.path.abspath(u.path)
        else:
            raise ValueError("Output path must be on locally mounted file system")

    @property
    def src_paths(self):
        return self._src_paths

    def set_src_pathset(self, pset):
        self._input_pathset = pset

    @staticmethod
    def traverse_input(input_pathset):
        source_paths = []

        pset = FilePathset.from_file(input_pathset)

        def ordered_traverse(root):
            host, port, root_path = phdfs.path.split(root)
            fs = phdfs.fs.hdfs(host, port)
            ipaths = \
                iter(_PathInfo(info['name'], info['size'])
                        for info in fs.walk(root_path)
                            if not os.path.basename(info['name']).startswith('_') and info['kind'] == 'file')
            # appends to the list defined in the parent function
            source_paths.extend(sorted(ipaths, cmp=lambda x, y: cmp(x.path, y.path)))
        for p in pset:
            ordered_traverse(p)
        return source_paths

    def _write_mr_input(self, fd):
        if self._src_paths is None or self._output_path is None:
            raise RuntimeError("You must set source and destination paths")

        dest_pos = 0
        for src in self._src_paths:
            # src path, src read pos, src read size, dest path, dest write pos
            line = serialize(src, self.output_path, dest_pos)
            fd.write("%s\n" % line)
            dest_pos += src.size

    def _clean_up(self, *paths):
        for p in paths:
            try:
                self.log.debug("Removing path: %s", p)
                phdfs.rmr(p)
            except StandardError as e:
                self.log.warning("Error deleting path %s", p)
                self.log.exception(e)

    def run(self):
        if not self._input_pathset:
            raise RuntimeError("You must set the input pathset before running")

        self.log.info("Analysing input paths")
        self._src_paths = self.traverse_input(self._input_pathset)
        self.log.info("Found %s input paths for a total of %0.1f MB", len(self._src_paths),
                sum(i.size for i in self._src_paths) / float(2**20))

        output_dir = os.path.dirname(self.output_path)
        work_dir = os.path.join(output_dir, str(uuid4()))
        work_input_path = os.path.join(work_dir, "dist_cat_input")
        # An output directory is used only because pipes requires it.
        # Hadoop may also put its log files there.
        work_output_path = os.path.join(work_dir, "junk_output")
        work_exec_path = os.path.join(work_dir, "dist_cat_script")

        self.log.debug("Run parameters")
        self.log.debug("output path: %s", self.output_path)
        self.log.debug("work dir: %s", work_dir)
        self.log.debug("work_input_path: %s", work_input_path)
        self.log.debug("work_output_path: %s", work_output_path)
        self.log.debug("work_exec_path: %s", work_exec_path)

        host, port, path = phdfs.path.split(work_dir)
        fs = phdfs.fs.hdfs(host, port)
        if not fs.exists(path):
            self.log.debug("Creating work directory %s on fs (%s, %s)", path, host, port)
            fs.create_directory(path)
        try:
            self.log.debug("Creating job input file %s", work_input_path)
            # need to only pass the path to this call (no host/port)
            with fs.open_file(phdfs.path.split(work_input_path)[2], 'w') as f:
                self._write_mr_input(f)
            self.log.debug("Wrote temp input file %s", work_input_path)

            # create/truncate the output file
            self.log.info("Creating output file %s", work_output_path)
            with open(phdfs.path.split(self.output_path)[2], 'w'):
                pass

            script_args = [
                'script',
                '--num-reducers', '0',
                '-Dmapred.map.tasks=%d' % len(self._src_paths),
                '-Dmapred.input.format.class=org.apache.hadoop.mapred.lib.NLineInputFormat',
                '-Dmapred.line.input.format.linespermap=1',
                '-Dmapred.map.tasks.speculative.execution=false',
                __file__,
                work_input_path,
                work_output_path ]
            self.log.debug("pydoop script args: %s", script_args)

            self.log.info("Launching pydoop job")
            pydoop_main.main(script_args)
            self.log.info("Finished")
        finally:
            self.log.debug("Cleaning up")
            self._clean_up(work_dir)

def main(args):
    input_pathset, output_file = parse_args(args)
    driver = DistCatPaths()
    driver.set_src_pathset(input_pathset)
    driver.output_path = output_file

    start_time = time.time()

    driver.run()

    end_time = time.time()
    duration = end_time - start_time
    mb = sum(i.size for i in driver.src_paths) / float(2**20)
    driver.log.info("Wrote %0.1f MB in %d seconds (%0.1f MB/s)", mb, round(duration), mb / (min(0.1, duration)))
