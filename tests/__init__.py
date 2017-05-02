#!/usr/bin/env python
import unittest

if __name__ == '__main__':
    test_suite = unittest.TestLoader().discover('.', pattern = "*_test.py")
    unittest.TextTestRunner(verbosity=2).run(test_suite)
