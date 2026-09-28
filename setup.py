#!/usr/bin/env python

import os.path as op
import sys
from setuptools import setup

sys.path.insert(0, op.dirname(op.abspath(__file__)))

from _datalad_buildsupport.setup import (
    BuildManPage,
)

cmdclass = {}
cmdclass.update(build_manpage=BuildManPage)

if __name__ == "__main__":
    setup(cmdclass=cmdclass)