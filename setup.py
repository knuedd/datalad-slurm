#!/usr/bin/env python

import sys
from setuptools import setup, find_packages
import versioneer

from _datalad_buildsupport.setup import (
    BuildManPage,
)

cmdclass = versioneer.get_cmdclass()
cmdclass.update(build_manpage=BuildManPage)

if __name__ == '__main__':
    setup(
        name='datalad-slurm',
        version=versioneer.get_version(),
        packages=find_packages(where="src"),
        package_dir={"": "src"},
        cmdclass=cmdclass,
        install_requires=[
            "datalad>=0.18.0",
            "sqlalchemy>=1.4.0",
            "tqdm>=4.0.0",
        ],
        entry_points={
            'datalad.extensions': [
                'slurm=datalad_slurm:command_suite',
            ],
        },
        python_requires=">=3.8",
        author="Andreas Knüpfer, Timothy Callow",
        author_email="a.knuepfer@hzdr.de, t.callow@hzdr.de",
        description="A DataLad extension for HPC (slurm) systems",
        long_description=open("README.md").read(),
        long_description_content_type="text/markdown",
        url="https://github.com/datalad/datalad-slurm",
        license="MIT",
        classifiers=[
            "Development Status :: 3 - Alpha",
            "Intended Audience :: Science/Research",
            "License :: OSI Approved :: MIT License",
            "Programming Language :: Python :: 3",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
        ],
    )
