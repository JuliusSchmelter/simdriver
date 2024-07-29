from setuptools import setup
from pathlib import Path

weio = (Path(__file__).parent / "extern/weio").as_uri()

setup(
    name="simdriver",
    version="0.0.1",
    author="Julius Schmelter",
    author_email="schmelter@ifb.uni-stuttgart.de",
    description="OpenFAST Simulation Manager",
    url="https://git.ifb.uni-stuttgart.de/jschmelter/simdriver",
    packages=["simdriver"],
    install_requires=[
        f"weio @ {weio}",
        'importlib-metadata; python_version<="3.12"',
    ],
)
