#!/usr/bin/env python

from setuptools import setup

setup(
    name="tap-codex",
    version="0.1.0",
    description="Singer.io tap for extracting data from the Codex API",
    author="0x-s13i",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap-codex"],
    install_requires=[
        "singer-python==5.12.1",
        "requests==2.29.0",
        "urllib3==1.26.20",
        "backoff==1.8.0",
        "python-dateutil==2.8.2",
    ],
    extras_require={"dev": ["pylint==2.6.2", "ipdb", "nose", "requests-mock==1.9.3"]},
    entry_points="""
          [console_scripts]
          tap-codex=tap_codex:main
      """,
    packages=["tap_codex"],
    package_data={"tap_codex": ["schemas/*.json"]},
    include_package_data=True,
)
