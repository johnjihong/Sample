from setuptools import setup, find_packages

setup(
    name='Sample Application',
    version='0.0.1',
    description='Sample Application',
    url='https://github.com/johnjihong/Sample.git',
    author='Ji Hong',
    author_email='johnjihong@hotmail.com',
    license='MIT',
    find_packages=find_packages(exclude=['contribute', 'docs', 'tests*'])
)
