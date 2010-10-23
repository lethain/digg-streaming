#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name="Digg Streaming",
      version="1.0",
      description="Python interface for Digg Streaming API",
      author="Will Larson",
      packages=find_packages(),
      include_package_data=True,
      license="MIT",
      author_email="wlarson@digg.com",
      install_requires=["pycurl"],
      keywords="Digg streaming API",
      )
