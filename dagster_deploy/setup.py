from setuptools import find_packages, setup

setup(
    name="dagster_deploy",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "dagster_deploy": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-databricks",
        "dbt-databricks",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)

