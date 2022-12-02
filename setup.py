#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import find_packages, setup

with open("README.md") as readme_file:
    readme = readme_file.read()

test_requirements = [
    "codecov",
    "flake8",
    "pytest",
    "pytest-cov",
    "pytest-raises",
]

setup_requirements = [
    "pytest-runner",
]

dev_requirements = [
    "bumpversion>=0.5.3",
    "coverage>=5.0a4",
    "flake8>=3.7.7",
    "ipython>=7.5.0",
    "m2r>=0.2.1",
    "pytest>=4.3.0",
    "pytest-cov==2.6.1",
    "pytest-raises>=0.10",
    "pytest-runner>=4.4",
    "Sphinx>=2.0.0b1",
    "sphinx_rtd_theme>=0.1.2",
    "tox>=3.5.2",
    "twine>=1.13.0",
    "wheel>=0.33.1",
    # "lkaccess",
]

requirements = [
    "aics_dask_utils==0.2.0",
    "aicsimageio>=4.9.3",
    "aicsimageprocessing>=0.8.3",
    "bokeh<3",
    "dask[complete]==2022.10.2",
    "dask_jobqueue==0.8.1",
    "distributed==2022.10.2",
    "labkey",
    # "lkaccess>=1.4.21",
    "ome-types>=0.2.3",
    "pandas",
    "prefect==2.6.7",
    "quilt3",
    "jinja2",
    "urllib3",  # quilt3
    "python-dateutil==2.8.0",  # quilt3
    "cloudpickle==2.2.0",  # prefect
]

extra_requirements = {
    "test": test_requirements,
    "setup": setup_requirements,
    "dev": dev_requirements,
    "all": [*requirements, *test_requirements, *setup_requirements, *dev_requirements],
}

setup(
    author="DMT",
    author_email="danielt@alleninstitute.org",
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Developers",
        "License :: Allen Institute Software License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    data_files=[
        ("", ["cellbrowser_tools/batch_job.j2", "cellbrowser_tools/single_job.j2"])
    ],
    description="Build dataset for release in Cell Feature Explorer",
    entry_points={
        "console_scripts": [
            "build_release=cellbrowser_tools.bin.build_release:main",
            "make_images=cellbrowser_tools.bin.make_images:main",
            "make_dataset_from_csv=cellbrowser_tools.bin.make_dataset_from_csv:main",
            "make_downloader_manifest=cellbrowser_tools.bin.make_downloader_manifest:main",
            "processImageWithSegmentation=cellbrowser_tools.bin.processImageWithSegmentation:main",
        ],
    },
    install_requires=requirements,
    license="Allen Institute Software License",
    long_description=readme,
    long_description_content_type="text/markdown",
    include_package_data=True,
    keywords="cellbrowser_tools",
    name="cellbrowser_tools",
    packages=find_packages(),
    python_requires=">=3.7",
    setup_requires=setup_requirements,
    test_suite="cellbrowser_tools/tests",
    tests_require=test_requirements,
    extras_require=extra_requirements,
    # Do not edit this string manually, always use bumpversion
    # Details in CONTRIBUTING.rst
    version="0.1.0",
    zip_safe=False,
)
