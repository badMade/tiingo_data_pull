"""Setup configuration for tiingo_data_pull package."""
from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and
                    not line.startswith("#")]

setup(
    name="tiingo_data_pull",
    version="0.1.0",
    author="badMade",
    description="Pipeline to sync Tiingo market data "
    "with Notion and Google Drive",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/badMade/tiingo_data_pull",
    package_dir={"": "src"},
    packages=find_packages(where="src"),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.11",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.11",
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "tiingo-data-pull=tiingo_data_pull.cli:run",
        ],
    },
)
