import re
from io import open
from os import path

from setuptools import find_packages, setup

VERSION = "5.2.0"


def read_requirements(path):
    with open(path, "r") as f:
        requirements = f.read().splitlines()
        processed_requirements = []

        for req in requirements:
            if req.startswith("git+") or "@" in req:
                pkg_name = re.search(r"(#egg=)([\w\-_]+)", req)
                if pkg_name:
                    processed_requirements.append(pkg_name.group(2))

                else:
                    continue

            else:
                processed_requirements.append(req)

        return processed_requirements


requirements = read_requirements("requirements.txt")
here = path.abspath(path.dirname(__file__))

with open(path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


setup(
    name="vision",
    version=VERSION,
    description="Giving access to bittensor",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/namoray/nineteen",
    author="namoray",
    packages=find_packages(),
    include_package_data=True,
    author_email="",
    license="MIT",
    python_requires=">=3.10",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "vision=vision:cli",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Build Tools",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Scientific/Engineering",
        "Topic :: Scientific/Engineering :: Mathematics",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
