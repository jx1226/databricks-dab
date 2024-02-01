from setuptools import setup, find_packages

import sys
sys.path.append('./src')

import datetime
import aep_profiles_kpi

setup(
    name="aep_profiles_kpi",
    # We use timestamp as Local version identifier (https://peps.python.org/pep-0440/#local-version-identifiers.)
    # to ensure that changes to wheel package are picked up when used on all-purpose clusters
    version=aep_profiles_kpi.__version__ + "+" + datetime.datetime.utcnow().strftime("%Y%m%d.%H%M%S"),
    author="Henkel",
    description="wheel file based on aep_profiles_kpi/src",
    packages=find_packages(where='./src'),
    package_dir={'': 'src'},
    entry_points={
        "packages": [
            "main=aep_profiles_kpi.main:main"
        ]
    },
    install_requires=[
        "aepp"
        "setuptools"
    ],
)
