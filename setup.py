import os
from shutil import copyfile
import socket

from setuptools import setup, find_packages, Command
from distutils.command.install import install
from distutils.command.sdist import sdist


here = os.path.dirname(os.path.abspath(__file__))


class InstallCommand(install):

    user_options = install.user_options

    def initialize_options(self):
        install.initialize_options(self)

    def finalize_options(self):
        install.finalize_options(self)

    def run(self):
        install.run(self)


setup(
    name="sqlalchemy-gpudb",
    version="7.0.1",
    author="Andrew Duberstein",
    author_email="ksutton@kinetica.com",
    description="Kinetica dialect for SQLAlchemy",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
    ],
    keywords="SQLAlchemy GPUDb",
    cmdclass={
        "install": InstallCommand,
        "sdist": sdist,
    },
    extras_require={
        "dev": [
            "pytest",
            "black",
        ]
    },
    packages=find_packages(include=["sa_gpudb"]),
    include_package_data=True,
    install_requires=["SQLAlchemy", "pyodbc"],
)
