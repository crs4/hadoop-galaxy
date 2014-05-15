
# BEGIN_COPYRIGHT
#
# Copyright (C) 2014 CRS4.
#
# This file is part of hadoop-galaxy, released under the terms of the BSD
# 3-Clause License <http://opensource.org/licenses/BSD-3-Clause>.
#
# END_COPYRIGHT


"""
Split a pathset into two parts using a regular expression as a test.

Paths in the input pathset are tested against the provided regexp.  If the
path matches it is put in the "true" pathset, else in the "false" pathset.
The `match` method from re.RegexObject is used to test, so the expression
must match from the start of the path.  The expression can optionally be
anchored to the end as well.

Optionally, the tool can descend into the input path up to a specified
maximum number of levels (--expand-levels) before applying the regex to
each resulting path individually.  By default expansion is off (0 levels).
"""


import hadoop_galaxy.pathset as pathset
import pydoop.hdfs as hdfs

import argparse
import re
import sys
import warnings

def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Split a pathset by regular expression")
    parser.add_argument('-a', '--anchor-end', action="store_true",
            help="If set, the regular expression must match at end of the path (like appending '$')")

    parser.add_argument('-e', '--expand-levels', metavar="N", type=int, default=0,
            help="Number of levels to descent into path (Default: 0)")
    parser.add_argument('expression', help="Regular expression to apply as a test")
    parser.add_argument('input_pathset', help="Input pathset file")
    parser.add_argument('output_true', help="Output pathset for paths matching the expression")
    parser.add_argument('output_false', help="Output pathset for paths not matching the expression")

    options = parser.parse_args(args)

    if options.expand_levels < 0:
        parser.error("number of levels to descend into path must be >= 0 (got %s)" % options.expand_levels)
    return options


def expand(fs, root, max_levels):
    """
    Walk a directory hierarchy up to max_levels.  Yield all leaves.
    """
    if max_levels <= 0:
      yield root
    else: # max_levels >= 1
      root_info = fs.get_path_info(root)
      if root_info['kind'] == 'file':
          yield root_info['name']
      elif root_info['kind'] == 'directory':
          listing = \
            [ path_info for path_info in fs.list_directory(root_info['name'])
                   # Skip hidden files
                   if path_info['name'][0] not in ('.', '_') ]
          for item in listing:
              if max_levels == 1 or item['kind'] == 'file':
                  yield item['name']
              else:
                  for child in expand(fs, item['name'], max_levels - 1):
                      yield child
      else:
          warnings.warn("Skipping item %s. Unsupported file kind %s" % (root_info['name'], root_info['kind']))

def main(args=None):
    options = parse_args(args)

    expr = options.expression
    if options.anchor_end:
        expr += '$'
    try:
        pattern = re.compile(expr)
    except RuntimeError as e:
        print >> sys.stderr, "Error compiling regular expression '%s'" % expr
        print >> sys.stderr, e
        sys.exit(2)

    # read input pathset
    source_pathset = pathset.FilePathset.from_file(options.input_pathset)
    # and set up output pathsets
    match_pathset = pathset.FilePathset()
    match_pathset.datatype = source_pathset.datatype

    no_match_pathset = pathset.FilePathset()
    no_match_pathset.datatype = source_pathset.datatype

    # now iterate through the paths
    for p in source_pathset:
        host, port = hdfs.path.split(p)[0:2]
        fs = hdfs.fs.hdfs(host, port)
        for leaf in expand(fs, p, options.expand_levels):
            if pattern.match(leaf):
                match_pathset.append(leaf)
            else:
                no_match_pathset.append(leaf)

    # write to file
    with open(match_pathset, 'w') as f:
        match_pathset.write(f)
    with open(no_match_pathset, 'w') as f:
        no_match_pathset.write(f)
