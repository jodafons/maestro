# Imports
from setuptools import setup, find_packages

# Loading README file
with open("README.md", "r") as f:
    long_description = f.read()
with open("requirements.txt", "r") as f:
    requirements = f.read()


setup(
    name='maestro',
    version='1.0.0',
    license='GPL-3.0',
    description='maestro orquestrator',
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=find_packages(),
    author='João Victor da Fonseca Pinto',
    author_email='jodafons@lps.ufrj.br',
    url='https://github.com/jodafons/maestro',
    keywords=['orchestration'],
    install_requires=requirements,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    entry_points = {
        'console_scripts' : [
            'maestro = maestro.main:run',
        ]
    }
)