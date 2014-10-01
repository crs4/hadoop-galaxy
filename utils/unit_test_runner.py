
# BEGIN_COPYRIGHT
#
# Copyright (C) 2014 CRS4.
#
# This file is part of hadoop-galaxy, released under the terms of the BSD
# 3-Clause License <http://opensource.org/licenses/BSD-3-Clause>.
#
# END_COPYRIGHT

from glob import glob
import imp
import logging
import os
import sys
import unittest

_log = logging.getLogger('UnitTestRunner')

## Code borrowed from Seal (http://github.com/crs4/seal)
class UnitTestRunner(object):

    def __init__(self, test_modules=None):
      if test_modules:
          self.autotest_list = test_modules
      else:
          proj_path = os.path.join(os.path.dirname(__file__), '..')
          self.autotest_list = glob(os.path.join(proj_path, 'tests', 'test_*.py'))
      _log.info("Autotest list: %s", self.autotest_list)

    @staticmethod
    def __load_suite(module_path):
        module_name = os.path.splitext(os.path.basename(module_path))[0]
        ## so that test modules can import other modules in their own
        ## directories, we directly modify sys.path
        sys.path.append(os.path.dirname(module_path))
        fp, pathname, description = imp.find_module(module_name)
        try:
            module = imp.load_module(module_name, fp, pathname, description)
            del sys.path[-1]  # clean up to avoid conflicts
            return module.suite()
        finally:
            fp.close()

    def run(self):
        print >> sys.stderr, "Running tests from these modules:", self.autotest_list
        suites = map(UnitTestRunner.__load_suite, self.autotest_list)
        test_result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(tuple(suites)))
        return test_result

def main():
    res = UnitTestRunner().run()
    return 0 if res.wasSuccessful() else 1

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())
