import os
from setuptools import find_packages, setup

README = 'Coming Soon'

os.chdir(os.path.normpath(os.path.join(os.path.abspath(__file__), os.pardir)))

setup(
    name='automation',
    version='1.0.0',
    packages=find_packages(exclude=("auto","testimpl",)),
    include_package_data=True,
    license='BSD License',
    description='Background job processing module',
    long_description=README,
    url='https://github.com/nanaduah1/django-automation',
    author='Nana Duah',
    install_requires=[
        'django',
    ],
)
