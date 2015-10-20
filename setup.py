from setuptools import setup, find_packages
import sys, os

version = '0.1.2'

setup(name='pyparade',
      version=version,
      description="Python library to parallel process data efficiently",
      long_description="""\
PyParade is a lightweight thread-based multiprocessing framework that makes it easy to parallel process large-scale datasets efficiently.""",
      classifiers=["License :: OSI Approved :: MIT License",
                   "Development Status :: 3 - Alpha",
                   "Environment :: Console",
                   "Intended Audience :: Information Technology",
                   "Natural Language :: English",
                   "Operating System :: MacOS :: MacOS X",
                   "Operating System :: POSIX :: Linux",
                   "Programming Language :: Python :: 2.7"],
      keywords='big data parallel processing',
      author='Nils Breyer',
      author_email='mail@nilsbreyer.eu',
      url='nilsbreyer.eu/#!projects/pyparade',
      license='MIT License',
      packages=find_packages(),
      include_package_data=True,
      zip_safe=True,
      install_requires=[
          "futures>=3.0"
      ]
      )