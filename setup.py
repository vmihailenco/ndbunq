#!/usr/bin/env python

from distutils.core import setup


readme = open('README.rst').read()


setup(
    name='ndbunq',
    version='0.1.0',
    description='Poor man unique constraints for Google Appengine NDB',
    long_description=readme,
    author='Vladimir Mihailenco',
    author_email='vladimir.webdev@gmail.com',
    url='https://github.com/vmihailenco/ndbunq/',
    packages=['ndbunq'],
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Environment :: Web Environment',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
)
