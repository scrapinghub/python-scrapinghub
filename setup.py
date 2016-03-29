from os.path import join, dirname
from setuptools import setup, find_packages

__version__ = open(join(dirname(__file__), 'VERSION')).read().strip()


setup(
    name='scrapinghub',
    version=__version__,
    license='BSD',
    description='Client interface for Scrapinghub API',
    author='Scrapinghub',
    author_email='info@scrapinghub.com',
    url='http://scrapinghub.com',
    platforms = ['Any'],
    packages=find_packages(),
    package_data={
        'scrapinghub': ['VERSION'],
        'hubstorage': ['VERSION']},
    py_modules = ['scrapinghub'],
    install_requires=['requests', 'retrying>=1.3.3'],
    classifiers=['Development Status :: 4 - Beta',
               'License :: OSI Approved :: BSD License',
               'Operating System :: OS Independent',
               'Programming Language :: Python'],
)
