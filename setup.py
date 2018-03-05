from setuptools import setup, find_packages

setup(
    name='SHEDsidewinder',
    version='0.0.1',
    packages=find_packages(),
    url='',
    license='',
    author='Christopher J. Wright',
    author_email='cjwright4242@gmail.com',
    description='',
    scripts=['scripts/sidewind'],
    package_data={'shed_sidewinder.data': ['nomad/*']}
)
