"""A setuptools based setup module.

See:
https://packaging.python.org/en/latest/distributing.html
https://github.com/pypa/sampleproject
"""

# Always prefer setuptools over distutils
from setuptools import setup, find_packages
# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='bitty',

    version='0.0.3',

    description='Collect Crypto info',
    long_description=long_description,

    # The project's main homepage.
    url='https://github.com/reallistic/bitty',

    # Author details
    author='Reallistic',
    author_email='something_real@gmail.com',

    license='MIT',

    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Crypto Trading Tools',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
    ],

    # What does your project relate to?
    keywords='gdax poloniex websocket trading bitcoin ethereum',

    packages=find_packages(),

    install_requires=['ujson', 'aiohttp', 'autobahn', 'pytz'],

    extras_require={
        'dev': [],
        'test': ['nose'],
    },


    entry_points={
        'console_scripts': [
            'bitty=bitty.consumer:main',
        ],
    },
)
