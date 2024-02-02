#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


from setuptools import find_packages, setup

MAIN_REQUIREMENTS = [
    "litellm",
    "polars",
    "airbyte-cdk",
    "boto3",
    "botocore",
    "google-cloud-aiplatform",
    "openai",
    "tenacity"  # used for retires by litellm
]

TEST_REQUIREMENTS = ["pytest~=6.2"]

setup(
    name="destination_kapiche_email_cleaning_s3",
    description="Destination implementation for Kapiche Email Cleaning S3.",
    author="Airbyte",
    author_email="contact@airbyte.io",
    packages=find_packages(),
    install_requires=MAIN_REQUIREMENTS,
    package_data={"": ["*.json"]},
    extras_require={
        "tests": TEST_REQUIREMENTS,
    },
)
