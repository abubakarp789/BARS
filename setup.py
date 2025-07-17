from setuptools import setup, find_packages
import os
from pathlib import Path

# Read the contents of README.md
this_directory = Path(__file__).parent
long_description = (this_directory / "README.md").read_text(encoding="utf-8")

# Get version
version = {}
with open("bars_enhanced/__init__.py") as fp:
    exec(fp.read(), version)


def get_requirements():
    """Load requirements from requirements.txt."""
    req_path = this_directory / "requirements.txt"
    with open(req_path, encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip() and not line.startswith("#")]


setup(
    name="bars_enhanced",
    version=version.get("__version__", "0.1.0"),
    author="BARS Development Team",
    author_email="contact@example.com",
    description="Broadcaster Activity Rating System - Enhanced",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/yourusername/bars-enhanced",
    packages=find_packages(include=["bars_enhanced", "bars_enhanced.*"]),
    python_requires=">=3.8",
    install_requires=get_requirements(),
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.20.0",
            "black>=22.8.0",
            "flake8>=5.0.0",
            "mypy>=0.990",
            "types-python-dateutil>=2.8.19",
            "types-requests>=2.28.11",
        ],
    },
    entry_points={
        "console_scripts": [
            "bars=bars_enhanced.cli:main",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
)

if __name__ == "__main__":
    pass
