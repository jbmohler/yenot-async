#!/usr/bin/env python

from distutils.core import setup

setup(
    name="yenot",
    version="0.1",
    description="Yenot REST server",
    author="Joel B. Mohler",
    author_email="joel@kiwistrawberry.us",
    url="https://bitbucket.org/jbmohler/yenot",
    #packages=["yenot", "yenot.client", "yenot.backend", "yenot.server", "rtlib"],
    packages=["yenot", "yenot.backend", "yenot.server", "rtlib"],
)
