from os.path import join, dirname
from setuptools import setup, find_packages

# We can't import hubstorage.__version__ because it imports "requests" and it
# can not be available yet
__version__ = open(join(dirname(__file__), 'hubstorage/VERSION')).read().strip()


setup(name='hubstorage',
      version=__version__,
      license='BSD',
      description='Client interface for Scrapinghub HubStorage',
      author='Scrapinghub',
      author_email='info@scrapinghub.com',
      url='http://scrapinghub.com',
      platforms=['Any'],
      packages=find_packages(),
      package_data={'hubstorage': ['VERSION']},
      install_requires=['requests', 'retrying>=1.3.3', 'six>=1.10.0'],
      classifiers=['Development Status :: 4 - Beta',
                   'License :: OSI Approved :: BSD License',
                   'Operating System :: OS Independent',
                   'Programming Language :: Python'],
      )
