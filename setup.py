import setuptools
from xtracthub import __version__

setuptools.setup(
    name="xtracthub",
    version=__version__,
    author="Ryan Wong",
    author_email="rewong03@gmail.com",
    description="SDK for xtract-container-service",
    long_description_content_type="text/markdown",
    url="https://github.com/xtracthub/xtract-container-service",
    packages=setuptools.find_packages(),
)
