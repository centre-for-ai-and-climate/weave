from setuptools import find_packages, setup

setup(
    name="weave",
    packages=find_packages(exclude=["weave_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
