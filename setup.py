from setuptools import setup, find_packages

setup(
    name="POC_Test Genoa",
    version="1.0.0",
    description="Proof of concept for Azure testing - Genoa Data",
    author="David Torres",
    author_email="torres_arley@optum.com",
    packages=find_packages(),
    install_requires=[
        "pandas",
        "requests",
        "azure-storage-blob",
        "azure-storage-file-datalake",
        "pytest",
        "pytest-html",
        "pytest-email",
        "pytest-sugar"
    ],
    classifiers=[
        "Programming Language :: Python :: 3.11.5",
    ],
    python_requires=">=3.7",
)