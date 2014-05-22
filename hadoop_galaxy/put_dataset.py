
# BEGIN_COPYRIGHT
#
# Copyright (C) 2014 CRS4.
#
# This file is part of hadoop-galaxy, released under the terms of the BSD
# 3-Clause License <http://opensource.org/licenses/BSD-3-Clause>.
#
# END_COPYRIGHT

import argparse
import itertools as it
import logging
import os
import sys
import subprocess
from urlparse import urlparse

import pydoop
import pydoop.hdfs as phdfs

from hadoop_galaxy import log
from hadoop_galaxy.utils import expand_paths, config_logging
from hadoop_galaxy.pathset import FilePathset

# Environment variable to specify where to put the datasets
EnvPutDir = 'HADOOP_GALAXY_PUT_DIR'

def parse_args(args=None):
    # define the parser
    parser = argparse.ArgumentParser(description="Copy data referenced by a pathset to HDFS")
    parser.add_argument('src_pathset', metavar="SRC_PATHSET", help="Source pathset")
    parser.add_argument('output_dataset', metavar="DEST_PATHSET", help="Output dataset provided by Galaxy")
    parser.add_argument('--hadoop-workspace', metavar="URI",
            help="URI to a directory on the destination file system where the dataset(s) " +\
                 "will be copied (default: value of %s environment variable)" % EnvPutDir)
    parser.add_argument('--distcp', action='store_true', help="Use Hadoop distcp to perform the copy")
    parser.add_argument('--log-level',
            choices=['debug', 'info', 'warn', 'error', 'critical'],
            default='info')

    options = parser.parse_args(args)

    # validation
    workspace = options.hadoop_workspace or os.environ.get(EnvPutDir)
    if not workspace:
        parser.error("You need to specify a workspace URI, either via the --hadoop-workspace option or the %s environment variable" % EnvPutDir)
    options.workspace = phdfs.path.abspath(workspace)
    if not urlparse(workspace).scheme:
        print >> sys.stderr, "Implicit workspace scheme set to", urlparse(options.workspace).scheme
    if phdfs.path.exists(options.workspace) and not phdfs.path.isdir(options.workspace):
        parser.error("Workspace path %s exists and it's not a directory!" % options.workspace)
    return options

def src_to_dest_path(workspace, src_path):
    """
    Translate a source URI into a destination path in the workspace.
    """
    _, _, path = phdfs.path.split(src_path)
    dest_path = phdfs.path.join(workspace, path)
    return dest_path

def _group_by_dest_dir(src_uris, dest_uris):
    """
    Groups uris by the second-last element of the destination path.
    """
    # This simple implementation assumes the uris are in fact uris and
    # never relative paths.
    it_grouped = it.groupby(it.izip(src_uris, dest_uris), lambda tpl: phdfs.path.dirname(tpl[1]))
    groups = dict(( (dest_dir, [ src for src, dest in tuple_it]) for dest_dir, tuple_it in it_grouped))
    return groups

def perform_distcp(copy_groups):
    cmd_start = [ pydoop.hadoop_exec(), 'distcp2', '-atomic' ]
    try:
        for output_path, src_paths in copy_groups.iteritems():
            cmd = cmd_start[:]
            cmd.extend(src_paths)
            cmd.append(output_path)
            log.debug("%s", cmd)
            subprocess.check_call(cmd)
            # Hadoop distcp2 doesn't seem to correctly report errors through its
            # exit code. For instance, it exists with a 0 even when the job is killed.
            # To verify its success we'll check that the destination directory exists.
            # Since we're using -atomic it should only exist if everything went well.
            if phdfs.path.exists(output_path):
                log.info("Successfully ran distcp")
            else:
                raise RuntimeError("Distcp2 failed to complete. Output path not created: %s" % output_path)
    except (subprocess.CalledProcessError, RuntimeError) as e:
        log.critical("Error running distcp: %s", e.message)
        raise e

def perform_simple_cp(copy_groups):
    try:
        for output_path, src_paths in copy_groups.iteritems():
            for src in src_paths:
                log.debug("pydoop.hdfs.cp('%s','%s')", src, output_path)
                pydoop.hdfs.cp(src, output_path)
    except StandardError as e:
        log.critical("Error while performing copy: %s", e.message)
        raise e

def perform_copy(options):
    with open(options.src_pathset) as f:
        input_pathset = FilePathset.from_file(f)

    # set up workspace
    workspace = options.workspace
    log.info("Workspace set to %s", workspace)
    if not phdfs.path.exists(workspace):
        log.info("Workspace directory %s doesn't exist. Creating it.", workspace)
        phdfs.mkdir(workspace)

    src_paths = [ p for p in input_pathset ]
    log.debug("Source paths (first 5 or less): %s", src_paths[0:5])

    # dest_path is a unique path under the workspace whose name should be the same
    # as the Galaxy dataset name.
    dest_path = phdfs.path.join(workspace, phdfs.path.basename(options.output_dataset))
    log.info("Destination path: %s", dest_path)
    if phdfs.path.exists(dest_path):
        raise RuntimeError("Destination path %s already exists. Did you provide a valid Galaxy output dataset argument?" % dest_path)

    # We need to run a separate copy operation for each "leaf" destination
    # directory.  E.g.,
    #   /tmp/dirA/file1 /tmp/dirA/file2 -> workspace/dirA/
    #   /tmp/dirB/file1                 -> workspace/dirB/
    #
    # As shown in the example, in general we cannot be sure the source paths have
    # unique basenames. We also cannot rename multiple files on-the-fly (to a new
    # name guaranteed to be unique, such as a uuid4).  So, to reduce the number of
    # distcp or cp invocations, we group the source paths together by destination directory
    # (in the example, dirA and dirB).

    # expand for wildcards
    src_uris = [ u for wild in src_paths for u in expand_paths(urlparse(wild)) ]
    log.debug("first 5 src_uris: %s", src_uris[0:5])
    destination_uris = [ src_to_dest_path(dest_path, u) for u in src_uris ]
    log.debug("first 5 destination_uris: %s", destination_uris[0:5])
    copy_groups = _group_by_dest_dir(src_uris, destination_uris)
    if log.isEnabledFor(logging.DEBUG) and len(copy_groups) > 0:
        tpl = next(copy_groups.iteritems())
        log.debug("one copy group:\n\tdest:%s\n\tsrc: %s", tpl[0], tpl[1])

    try:
        if options.distcp:
            perform_distcp(copy_groups)
        else:
            perform_simple_cp(copy_groups)
    except Exception as e:
        log.critical("Failed to copy data to %s", dest_path)
        log.exception(e)
        log.info("Cleaning up %s, if it exists", dest_path)
        try:
            phdfs.rmr(dest_path)
        except IOError:
            log.debug("Failed to clean-up destination path %s. Maybe it was never created.", dest_path)
        raise e
    output_pathset = FilePathset(dest_path)
    output_pathset.set_datatype(input_pathset.datatype)
    output_pathset.comment = "Copied from\n" + '\n'.join(src_paths)
    with open(options.output_dataset, 'w') as f:
        output_pathset.write(f)

def main(args=None):
    try:
        args = args or sys.argv[1:]
        options = parse_args(args)
        config_logging(options.log_level)
        perform_copy(options)
        return 0
    except Exception as e:
        print >> sys.stderr, str(e)
        return 1
