
"""
pydoop script compress text files.

Works in tandem with hadoop_galaxy.text_zipper_mr.
"""


import argparse
import logging
import os
import sys
import tempfile
from urlparse import urlparse
import warnings

import hadoop_galaxy.text_zipper_mr as text_zipper_mr
import hadoop_galaxy.utils as utils

import pydoop.hdfs as hdfs
import pydoop.app.main as pydoop_app

def walk(fs, root):
    """
    Walk a directory hierarchy.  Yield all files.
    """
    root_info = fs.get_path_info(root)
    if root_info['kind'] == 'file':
        # special case -- the root is a file, so we yield it and exit
        yield root_info
    elif root_info['kind'] == 'directory':
        listing = fs.list_directory(root_info['name'])
        for parent in listing:
            if parent['kind'] == 'file':
                yield parent
            else:
                for child in walk(fs, parent['name']):
                    yield child
    else:
        warnings.warn("Skipping item %s. Unsupported file kind %s" % (root_info['name'], root_info['kind']))

class TextZipperDriver(object):
    def __init__(self, options):
        self.log = logging.getLogger('TextZipper')
        if hdfs.path.exists(options.output_dir):
            raise RuntimeError("output path %s already exists." % options.output_dir)
        self.output_path = hdfs.path.abspath(options.output_dir)
        self.input_paths = map(hdfs.path.abspath, options.input_paths)
        dont_exist = [ p for p in self.input_paths if not hdfs.path.exists(p) ]
        if dont_exist:
            raise RuntimeError("Error!  %s input paths don't exist.\n\t%s" % (len(dont_exist), '\n\t'.join(dont_exist)))
        # log some
        self.log.info("Writing output to %s", self.output_path)
        if self.log.isEnabledFor(logging.DEBUG):
            self.log.debug("Input paths:")
            for ipath in self.input_paths:
                self.log.debug("\t%s", ipath)

    def __write_mr_input(self, fd):
        """
        Write paths to compress in sorted order.

        Returns the number of records written.
        """
        count = 0
        for input_root in sorted(self.input_paths):
            fs_host, fs_port, _ = hdfs.path.split(input_root)
            fs = hdfs.fs.hdfs(fs_host, fs_port)
            self.log.debug("Walking %s", input_root)
            files = [ file_info['name'] for file_info in walk(fs, input_root) ]
            files.sort()
            for in_name in files:
                if input_root == in_name:
                    # the file was explicitly named as an input path
                    root = hdfs.path.dirname(in_name)
                    output_name = os.path.basename(in_name)
                else:
                    root = input_root
                    output_name = in_name.replace(input_root, '').lstrip('/')
                # we write a line with:
                # 1) input directory
                # 2) output directory
                # 3) relative path to file to be compressed
                # 1/3 -> abs path to input file; 2/3 + extension -> abs path to final output file
                line = '\t'.join( (root, self.output_path, output_name) )
                self.log.debug("Will compress: %s", line)
                fd.write(line)
                fd.write("\n")
            count += len(files)
        return count

    def run(self):
        exit_code = 1
        with tempfile.NamedTemporaryFile() as f:
            self.log.debug("opened scratch MR job input file %s", f.name)
            # We write the files to be compressed to a temporary file.  Later we'll re-read
            # this temporary file to rename the files as well.  I've opted not to keep the
            # table in memory in the hope of scaling better to jobs with a large number of
            # files (we reduce memory requirements).
            num_files = self.__write_mr_input(f)
            f.flush()
            self.log.debug("Finished writing temp input file")
            input_filename = tempfile.mktemp(dir=os.path.dirname(self.output_path), prefix="dist_txt_zipper_input")
            tmpfile_uri = "file://%s" % f.name
            try:
                self.log.debug("copying input from %s to %s", tmpfile_uri, input_filename)
                hdfs.cp(tmpfile_uri, input_filename)
                self.log.info("Run analyzed.  Launching distributed job")
                # launch mr task
                pydoop_args = \
                    [ 'script', '--num-reducers', '0','--kv-separator', '',
                      '-Dmapred.map.tasks=%d' % num_files,
                      '-Dmapred.input.format.class=org.apache.hadoop.mapred.lib.NLineInputFormat',
                      '-Dmapred.line.input.format.linespermap=1',
                      '-Dmapred.output.compress=true',
                      '-Dmapred.output.compression.codec=%s' % 'org.apache.hadoop.io.compress.GzipCodec',
                      text_zipper_mr.__file__,
                      input_filename,
                      self.output_path]
                self.log.debug("pydoop_args: %s", pydoop_args)
                self.log.info("Compressing %s files", num_files)
                pydoop_app.main(pydoop_args)
                self.log.info("Distributed job complete")
                self.rename_compressed_files(f)
                self.log.info("finished")
                exit_code = 0
            finally:
                try:
                    self.log.debug("Removing temporary input file %s", input_filename)
                    hdfs.rmr(input_filename)
                except IOError as e:
                    self.log.warning("Problem cleaning up.  Error deleting temporary input file %s", input_filename)
                    self.log.exception(str(e))
            return exit_code

    @staticmethod
    def get_compressor_extension(output_file_list):
        ext = None
        for fname in output_file_list:
            _, ext = os.path.splitext(fname)
            if ext:
                break
        return ext or ''

    def rename_compressed_files(self, file_table):
        # find the extension
        output_files = hdfs.ls(self.output_path)
        if len(output_files) == 0:
            return

        compressor_extension = self.get_compressor_extension(output_files)
        self.log.debug("compressor extension is %s", compressor_extension)

        hdfs_host, hdfs_port, _ = hdfs.path.split(output_files[0])
        if hdfs_host == '':
            is_local_fs = True
        else:
            is_local_fs = False
            output_hdfs = hdfs.hdfs(hdfs_host, hdfs_port)

        file_table.seek(0)
        for mapid, line in enumerate(file_table.xreadlines()):
            _, _, relative_output_name = line.rstrip('\n').split('\t')
            # we expect the map task ids to be assigned in the same order as the input
            # file list, so we can match the input file to an output file by its position
            # in the input file list.
            hadoop_output = os.path.join(self.output_path, "part-%05d" % mapid) + compressor_extension
            desired_file_name = os.path.join(self.output_path, relative_output_name) + compressor_extension
            if hadoop_output != desired_file_name:
                self.log.debug("renaming %s to %s", hadoop_output, desired_file_name)
                if is_local_fs:
                    # Though we could transparently use hdfs.move for both local fs and hdfs,
                    # using native methods for the local fs should be faster.
                    # os.renames automatically creates necessary parent directories for destination.
                    os.renames(urlparse(hadoop_output).path, urlparse(desired_file_name).path)
                else:
                    # create the output subdirectory, if necessary
                    dirname = os.path.dirname(relative_output_name)
                    if dirname:
                        output_hdfs.create_directory( os.path.join(self.output_path, dirname) )
                    if output_hdfs.exists(desired_file_name):
                        raise RuntimeError("Can't overwrite file in output directory: %s" % desired_file_name)
                    output_hdfs.move(hadoop_output, output_hdfs, desired_file_name)

def parse_args(args):
    parser = argparse.ArgumentParser(description="Distributed text file zipper.")
    parser.add_argument('input_paths', nargs='+', help="Input paths (directories will be traversed)")
    parser.add_argument('output_dir', help="Path where the output compressed files")
    parser.add_argument('-l', '--log-level', choices=['debug', 'info', 'warn', 'error', 'critical'],
            help="logging level (default: info)", default='info')
    #parser.add_argument('-z', '--codec', codec, help="Compression class", default="gzip")

    options = parser.parse_args(args)
    return options

def main(args=None):
    args = args or sys.argv[1:]
    options = parse_args(args)
    utils.config_logging(options.log_level)

    try:
        driver = TextZipperDriver(options)
    except StandardError as e:
        logging.critical("Error initializing")
        if e.message:
            logging.exception(e)
        sys.exit(1)
    driver.run()
    return 0

# vim: expandtab tabstop=4 shiftwidth=4 autoindent
