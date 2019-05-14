#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os

here = os.path.abspath(os.path.dirname(__file__))

version = {}
with open(os.path.join(here, 'packer', '__version__.py')) as f:
    exec(f.read(), version)

print(version['__version__'])
