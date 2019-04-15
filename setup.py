from setuptools import setup
from setuptools import find_packages

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='d2ix',
    version='1.3.4',
    packages=find_packages(),
    url='https://github.com/tum-ewk/d2ix',
    license='tbd',
    author='Thomas Zipperle und Clara Orthofer',
    author_email='gu47zip@tum.de',
    description='TUM API for the IIASA ixmp modeling platform',
    python_requires='>=3',
    install_requires=requirements,
    include_package_data=True,
    package_data={'d2ix': ['config/*.yml', '*.yaml']}

)
