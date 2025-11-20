"""Setup configuration for tiingo_data_pull package."""
import re
from pathlib import Path

from setuptools import find_packages, setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

with open("requirements.txt", "r", encoding="utf-8") as fh:
    requirements = [line.strip() for line in fh if line.strip() and
                    not line.startswith("#")]

# Read version from __init__.py
init_file = Path(__file__).parent / "src" / "tiingo_data_pull" / "__init__.py"
version_match = re.search(r'^__version__\s*=\s*[\'\"]([\w\.-]+)[\'"]',
                          init_file.read_text(encoding="utf-8"), re.MULTILINE)
if not version_match:
    raise RuntimeError(f"Unable to find version string in {init_file}")
version = version_match.group(1)

setup(
    name="tiingo_data_pull",
    version=version,
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
