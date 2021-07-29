import setuptools

with open('README.md') as f:
    long_desc = f.read()

setuptools.setup(
        name='dask-actor-singleton',
        version='1.1.1',
        author='Walt Woods',
        author_email='woodswalben@gmail.com',
        description='Helper library to allocate and retrieve singleton actors in Dask',
        long_description=long_desc,
        long_description_content_type='text/markdown',
        url='https://github.com/wwoods/dask-actor-singleton',
        packages=setuptools.find_packages(),
        install_requires=[
            'dask >= 2017.7.0',
        ],
        classifiers=[
            "Programming Language :: Python :: 3",
            "License :: OSI Approved :: MIT License",
            "Operating System :: OS Independent",
        ],
)

