from setuptools import setup, find_packages

setup(
    name="gcp-de-data-pipeline",
    version="0.1",
    install_requires=["apache-beam[gcp]"],
    packages=find_packages(where="src"),  # Ensures modules inside 'src/' are detected
    package_dir={"": "src"}  # Points Python to 'src/' as the base directory
)