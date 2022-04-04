# -*- coding: utf-8 -*-

# Learn more: https://github.com/ErwingForeroXpert/AutPresupuesto

from setuptools import setup, find_packages

with open('README') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='Clustering',
    version='1.0.0',
    description='Project for cluter databases',
    long_description=readme,
    author='Erwing Forero',
    author_email='erwing.forero@xpertgroup.co',
    url='https://github.com/ErwingForeroXpert/Presupuesto',
    license=license,
    packages=find_packages(exclude=('test','docs'))
)