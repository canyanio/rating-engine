# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


with open("README.md") as f:
    readme = f.read()

with open("LICENSE") as f:
    license = f.read()

setup(
    name="rating-engine",
    version="0.1.1",
    author="Canyan Team",
    author_email="dev@canyan.io",
    description="Canyan Rating Engine API",
    long_description=readme,
    long_description_content_type="text/markdown",
    license=license,
    url="https://canyan.io",
    install_requires=[
        "Click>=7.0",
        "aiohttp>=3.6.2",
        "aio-pika>=6.3.0",
        "pydantic",
        "python-dateutil",
        "pytz",
    ],
    packages=find_packages(exclude=("tests")),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    entry_points="""
        [console_scripts]
        rating-engine=rating_engine.main:main_with_env
    """,
)
