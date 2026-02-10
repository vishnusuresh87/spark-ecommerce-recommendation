from setuptools import setup, find_packages

setup(
    name="spark-ecommerce-recommendation",
    version="1.0.0",
    description="Complete e-commerce recommendation pipeline on Databricks",
    author="Vishnu Suresh",
    packages=find_packages(),
    install_requires=[
        "pyspark>=3.5.0",
        "delta-spark>=3.0.0",
        "pandas>=2.0.3",
        "numpy>=1.24.3",
        "scikit-learn>=1.3.0",
        "pytest>=7.4.0",
        "python-dotenv>=1.0.0",
    ],
    python_requires=">=3.9",
)