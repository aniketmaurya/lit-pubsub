#!/usr/bin/env python

from setuptools import find_packages, setup

setup(
    name="lit_pubsub",
    version="0.0.1",
    description="Lightning AI integration for publisher subscriber messaging queues like Kafka and AWS SQS",
    author="Aniket Maurya",
    author_email="theaniketmaurya@gmail.com",
    # REPLACE WITH YOUR OWN GITHUB PROJECT LINK
    url="https://github.com/aniketmaurya/lit-pubsub",
    install_requires=[],
    packages=find_packages(),
    package_dir={"": "src"},
)
