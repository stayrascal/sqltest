from setuptools import setup

if __name__ == '__main__':
    with open("README.md", "r") as fh:
        long_description = fh.read()

    setup(name="sqltest",
          version="0.0.1",
          author="stayrascal",
          author_email="stayrascal@gmail.com",
          description="sqltest: easy testing ETL SQLs",
          long_description=long_description,
          keywords=["test", "ETL", "sql"])
