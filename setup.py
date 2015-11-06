#
# setup.py
#
"""
A pythonic interface to the LevelDB database.
"""

from os import path
from imp import load_source
from setuptools import setup, find_packages

metadata = load_source("metadata", path.join("levelpy", "__meta__.py"))

REQUIRES = [
]

OPTIONAL_REQUIRES = {
}

TESTS_REQUIRE = [
    'pytest',
]

PACKAGES = find_packages(
    exclude=["*.tests", "*.tests.*", "tests.*", "tests"]
)

CLASSIFIERS = [
    "Development Status :: 3 - Alpha",
    "License :: OSI Approved :: MIT License",
    "Operating System :: OS Independent",
    "Topic :: Database",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Natural Language :: English",
]

tar_url = 'https://github.com/akubera/levelpy/archive/v%s.tar.gz' % (metadata.version)  # noqa

setup(
    name="levelpy",
    packages=PACKAGES,
    version=metadata.version,
    author=metadata.author,
    author_email=metadata.author_email,
    license=metadata.license,
    url=metadata.url,
    download_url=tar_url,
    description=__doc__.strip(),
    classifiers=CLASSIFIERS,
    install_requires=REQUIRES,
    extras_require=OPTIONAL_REQUIRES,
    tests_require=TESTS_REQUIRE,
    platforms='all',
)
