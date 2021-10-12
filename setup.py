from setuptools import setup

if __name__ == '__main__':
    with open("README.md", "r") as fh:
        long_description = fh.read()

    setup(
        name="sqltest",
        install_requires=["openpyxl >= 3.0.9"]
    )
