from setuptools import setup, find_packages

setup(
    name="sales_etl_project",
    version="1.0.0",
    author="Shravani",
    description="Medallion Architecture Data Engineering Project",
    packages=find_packages(),
    install_requires=[
        "pyspark",
        "pytest",
        "pyyaml",
        "pandas",
        "pyarrow"
    ],
    python_requires=">=3.10"
)