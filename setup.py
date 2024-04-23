import os

from setuptools import find_packages, setup

VERSION = "0.1.0.dev"

HERE = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file.
with open(os.path.join(HERE, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="data-integration-pipelines",
    author="BlueBrain DKE",
    author_email="bbp-ou-dke@groupes.epfl.ch",
    version=VERSION,
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    keywords="framework knowledge graph forge data processing mapper mapping",
    packages=find_packages(),
    python_requires=">=3.7",
    include_package_data=True,
    setup_requires=['setuptools_scm'],
    install_requires=[
        "pandas",
        "nexusforge@git+https://github.com/BlueBrain/nexus-forge@fix_search_unequal",
        "morphio==v3.3.7",
        "morph-tool==v2.10.2",
        "morphology-workflows==0.10.1",
        "voxcell==3.1.9",
        "python-keycloak>=0.24.0",
        # "pynwb==2.0.0",
        "pyjwt",
        "neurom==3.2.10",
        "pynrrd==1.0.0",
        # "numpy<1.24",
        # f"../pyswcparser",
        "XlsxWriter==3.1.9",
        "importlib_metadata",
        "IPython",
        "openpyxl",
    ],
    extras_require={
        "dev": ["pytest", "pytest-cov", "pytest-mock", "flake8"],
    },
    classifiers=[
        "Development Status :: 2 - Pre-Alpha",
        "Intended Audience :: Information Technology",
        "Intended Audience :: Science/Research",
        "Topic :: Scientific/Engineering",
        "Programming Language :: Python :: 3 :: Only",
        "Natural Language :: English",
    ]
)
