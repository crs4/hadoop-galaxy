# BEGIN_COPYRIGHT
#
# Copyright (C) 2014 CRS4.
#
# This file is part of hadoop-galaxy, released under the terms of the BSD
# 3-Clause License <http://opensource.org/licenses/BSD-3-Clause>.
#
# END_COPYRIGHT

import os
import urlparse

class Pathset(object):
  """
  A collection of paths, with an associated data type.
  """

  Unknown = "Unknown"

  def __init__(self, *pathlist):
    """
    Create a pathset.  If paths are provided in pathset,
    they will be sanitized and inserted into the new pathset.
    """
    self.paths = map(self.sanitize_path, pathlist)
    self.datatype = self.Unknown
    self._comment = ""

  @property
  def comment(self):
    return self._comment

  @comment.setter
  def comment(self, c):
    self._comment = (c if c is not None else "")

  def set_datatype(self, datatype):
    self.datatype = datatype

  @staticmethod
  def sanitize_path(path):
    """
    Turns a path into a full URI, if it's not already.  This method is applied
    to the input and output paths passed to the Hadoop command.  At the moment
    we're assuming all paths that don't specify a scheme are on the local file
    system (file://).

    TODO:  consider incomplete URI's to be on the configured default file
    system instead of file://
    """
    url = urlparse.urlparse(path)
    if url.scheme: # empty string if not available
      return path
    else:
      return "file://" + os.path.abspath(path)

  def append(self, p):
    """
    Appena a path to this pathset.  The path will be sanitized.
    """
    self.paths.append(self.sanitize_path(p))
    return self

  def get_paths(self):
    return self.paths

  def __iter__(self):
    return iter(self.paths)

  def __len__(self):
    return len(self.paths)

  def __str__(self):
    return os.pathsep.join(self.paths)

class FilePathset(Pathset):
  """
  Extends the Pathset with a serialization format.
  """
  Magic = "# Pathset"
  VersionTag = "Version"
  Version = "0.0"
  DataTypeTag = "DataType"

  @staticmethod
  def from_file(fd):
    retval = FilePathset()
    if not hasattr(fd, 'readline'):
      # not an I/O object.  Assume it's a file path
      with open(fd) as io:
        retval.read(io)
    else:
      # assue it's an I/O object and try to read it directly
      retval.read(fd)
    return retval

  def __init__(self, *pathlist):
    super(FilePathset, self).__init__(*pathlist)

  def __format_header(self):
    return '\t'.join((self.Magic, ':'.join( (self.VersionTag, self.Version) ), ':'.join( (self.DataTypeTag, self.datatype or self.Unknown) )))

  def __parse_header(self, header_line):
    if not header_line.startswith(self.Magic):
      raise ValueError("Unrecognized Pathset file format. Expected string '%s' not found at start of file. Found %s" %\
          (self.Magic, header_line if len(header_line) <= 10 else header_line[0:10] + '...'))
    fields = header_line.split('\t')
    field_dict = dict( f.split(':') for f in fields[1:] )

    if field_dict.get(self.VersionTag, self.Version) != self.Version:
      raise ValueError("Incompatible Pathset file format (found version %s but expected version %s)" % (field_dict[self.VersionTag], self.Version))
    self.datatype = field_dict.get(self.DataTypeTag, self.Unknown)

  def read(self, fd):
    header = fd.readline().rstrip('\n')
    self.__parse_header(header)
    self.paths = [ ]
    comments = []
    for line in fd.xreadlines():
      line = line.rstrip('\n')
      if not line:
        continue # skip blank lines
      if line.startswith('#'): # comment line
        if len(line) >= 2 and line[1] == ' ': # trim one space after #, if it's there
          comments.append(line[2:])
        else:
          comments.append(line[1:]) # else only trim the #
      else:
        self.paths.append(line)
    self._comment = '\n'.join(comments)
    return self

  def write(self, fd):
    fd.write(self.__format_header() + '\n')
    # write comments
    if self._comment:
      fd.write('#')
      fd.write(self._comment.replace('\n', '\n# '))
      if self._comment[-1] != '\n':
        fd.write('\n')
    for p in self.paths:
      fd.write(p)
      fd.write('\n')

# vim: expandtab autoindent shiftwidth=2 tabstop=2
