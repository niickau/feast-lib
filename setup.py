import setuptools

setuptools.setup(
    name="feastlib",
    version="0.2.2",
    description="Package with classes for a fast data ingestion from Spark to Feast.",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    
    include_package_data=True,
    package_data={'': ['*.json']}
)
