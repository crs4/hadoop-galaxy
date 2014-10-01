#!/usr/bin/env python

# BEGIN_COPYRIGHT
#
# Copyright (C) 2014 CRS4.
#
# This file is part of hadoop-galaxy, released under the terms of the BSD
# 3-Clause License <http://opensource.org/licenses/BSD-3-Clause>.
#
# END_COPYRIGHT


from StringIO import StringIO
import unittest

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from hadoop_galaxy.pathset import Pathset, FilePathset

class TestPathset(unittest.TestCase):
    def setUp(self):
        self.ps = Pathset()

    def test_new(self):
        self.assertEqual(0, len(self.ps))
        self.assertEqual("", self.ps.comment)
        self.assertEqual(Pathset.Unknown, self.ps.datatype)

    def test_build_w_paths(self):
        paths = [ '/etc', '/lib', 'file:///bin' ]
        ps = Pathset(*paths)
        self.assertEqual(len(paths), len(ps))
        self.assertEqual(
                set(['file:///etc', 'file:///lib', 'file:///bin' ]),
                set(ps.get_paths()))

    def test_comment_none(self):
        self.ps.comment = None
        self.assertEqual("", self.ps.comment)

    def test_sanitize(self):
        # simple absolute path
        self.assertEqual("file:///tmp", self.ps.sanitize_path("/tmp"))
        cwd = os.path.abspath(os.getcwd())
        self.assertEqual("file://%s/tmp" % cwd, self.ps.sanitize_path("tmp"))
        self.assertEqual("hdfs://localhost:9000/user/myname",
            self.ps.sanitize_path("hdfs://localhost:9000/user/myname"))

    def test_append(self):
        retval = self.ps.append("/tmp")
        self.assertTrue(retval is self.ps)
        self.assertEqual(1, len(self.ps))
        self.assertEqual("file:///tmp", next(iter(self.ps)))

class TestFilePathset(unittest.TestCase):
    def setUp(self):
        self.paths = [ '/etc', '/lib', 'file:///bin' ]
        self.ps = FilePathset(*self.paths)
        self.ps.datatype = 'text/plain'
        self.ps.comment = "A test pathset"

        self.ps = FilePathset()

    def test_build_from_file(self):
        io = StringIO()
        self.ps.write(io)
        io.seek(0)
        ps2 = FilePathset.from_file(io)
        self.assertEqual(len(self.ps), len(ps2))
        self.assertEqual(set(self.ps.get_paths()), set(ps2.get_paths()))
        self.assertEqual(self.ps.datatype, ps2.datatype)
        self.assertEqual(self.ps.comment, ps2.comment)

    def test_read_write(self):
        io = StringIO()
        self.ps.write(io)
        io.seek(0)
        ps2 = FilePathset()
        ps2.read(io)
        self.assertEqual(len(self.ps), len(ps2))
        self.assertEqual(set(self.ps.get_paths()), set(ps2.get_paths()))
        self.assertEqual(self.ps.datatype, ps2.datatype)
        self.assertEqual(self.ps.comment, ps2.comment)


def suite():
    s = unittest.TestLoader().loadTestsFromTestCase(TestPathset)
    s.addTests(unittest.TestLoader().loadTestsFromTestCase(TestFilePathset))
    return s

def main():
    result = unittest.TextTestRunner(verbosity=2).run(suite())
    return 0 if result.wasSuccessful() else 1

if __name__ == '__main__':
    sys.exit(main())
