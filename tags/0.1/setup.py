#! /usr/bin/env python
"""pglib, setup file.

$Id$

THIS SOFTWARE IS UNDER MIT LICENSE.
Copyright (c) 2006 Perillo Manlio (manlio.perillo@gmail.com)

Read LICENSE file for more informations.
"""


from distutils.core import setup, Extension


setup(name="pglib",
      version="0.1",
      author="Manlio Perillo",
      author_email="manlio.perillo@gmail.com",
      description="an implementation of the PostgreSQL protocol, version 3.0",
      license="MIT",
      url="http://developer.berlios.de/projects/pglib/",
      classifiers=[
          "Development Status :: 4 - Beta",
          "Environment :: Console",
          "Intended Audience :: Developers",
          "License :: OSI Approved :: MIT License",
          "Operating System :: OS Indipendent", 
          "Programming Language :: Python",
          "Topic :: Database :: Front-Ends",
          ],
      packages=["pglib"]
      )
