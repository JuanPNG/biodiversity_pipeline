from setuptools import setup

setup(
    name="biodiversity_pipelines",
    version="0.1.0",
    packages=["utils", "src"],
    package_dir={"utils": "utils", "src": "src"},
    install_requires=[],
    author="JPNG",
    description="Modular Apache Beam pipelines for biodiversity data processing",
)
