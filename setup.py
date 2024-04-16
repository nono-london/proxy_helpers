from setuptools import find_packages, setup

setup(
    name="proxy_helpers",
    packages=find_packages(),
    version="0.0.2.3",
    description="Handles proxies and database access",
    author="Nono London",
    author_email="",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=["python-dotenv",
                      "pandas",
                      "mysql-connector-python",
                      "requests",
                      "mysql_helpers>=0.0.0.1"
                      ],
    dependency_links=["git+https://github.com/nono-london/mysql_helpers.git"],
    setup_requires=["pytest-runner"],
    tests_require=["pytest"],
    test_suite="tests",
)

if __name__ == '__main__':
    pass
