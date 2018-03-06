from setuptools import setup, find_packages

setup(
    name='shed-sidewinder',
    version='0.0.5',
    packages=find_packages(),
    author='Christopher J. Wright',
    author_email='cjwright4242@gmail.com',
    entry_points={'console_scripts': 'sidewind = shed_sidewinder.main:main'},
    package_data={'shed_sidewinder.data': ['nomad/*']}
)
