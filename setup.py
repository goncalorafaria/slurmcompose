import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

with open("requirements.txt", "r") as fr:
    installation_requirements = fr.readlines()

setuptools.setup(
    name="slurmcompose",
    version="0.0.1",
    author="Goncalo Faria",
    author_email="gfaria@cs.washington.edu",
    description="Package for managing a fixed composition slurm cluster.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/goncalorafaria/slurmcompose",
    packages=setuptools.find_packages(),
    install_requires=installation_requirements,
    python_requires=">=3.6.0",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
