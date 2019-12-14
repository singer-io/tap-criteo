#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-criteo",
    version="0.1.0",
    description="Singer.io tap for extracting data from Criteo Marketing API",
    author="Judah Rand",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_criteo"],
    install_requires=[
        "singer-python==5.9.0",
        "criteo_marketing==1.0.159"
    ],
    entry_points="""
    [console_scripts]
    tap-criteo=tap_criteo:main
    """,
    packages=["tap_criteo"],
    package_data = {
        "schemas": ["tap_criteo/schemas/*.json"],
        "metadata": ["tap_criteo/metadata/*.json"]
    },
    include_package_data=True,
)
