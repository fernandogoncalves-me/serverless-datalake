import setuptools


with open("README.md") as fp:
    long_description = fp.read()


setuptools.setup(
    name="serverless_datalake",
    version="0.0.1",

    description="A Serverless DataLake example written using CDK Python",
    long_description=long_description,
    long_description_content_type="text/markdown",

    author="Fernando Gonçalves",
    author_email="fernandosg88@gmail.com",

    package_dir={"": "serverless_datalake"},
    packages=setuptools.find_packages(where="serverless_datalake"),

    install_requires=[
        "aws-cdk.core==1.32.2",
        "aws-cdk.aws-apigateway==1.32.2",
        "aws-cdk.aws-dynamodb==1.32.2",
        "aws-cdk.aws-iam==1.32.2",
        "aws-cdk.aws-kinesisfirehose==1.32.2",
        "aws-cdk.aws-lambda==1.32.2",
        "aws-cdk.aws-lambda-event-sources==1.32.2",
        "aws-cdk.aws-logs==1.32.2",
        "aws-cdk.aws-s3==1.32.2",
        "aws-cdk.aws-s3-assets==1.32.2",
        "aws-cdk.aws-s3-notifications==1.32.2",
        "aws-cdk.aws-sns==1.32.2",
        "aws-cdk.aws-sns-subscriptions==1.32.2",
        "aws-cdk.aws-sqs==1.32.2",
        "aws-cdk.aws-ssm==1.32.2",
        "boto3==1.12.22",
        "botocore==1.15.22"
    ],

    python_requires=">=3.6",

    classifiers=[
        "Development Status :: 4 - Beta",

        "Intended Audience :: Developers",

        "License :: OSI Approved :: Apache Software License",

        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",

        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",

        "Typing :: Typed",
    ],
)
