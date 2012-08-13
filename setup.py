from setuptools import setup, find_packages

setup(name='hubstorage',
      version='0.2',
      license='BSD',
      description='Client interface for Scrapinghub HubStorage',
      author='Scrapinghub',
      author_email='info@scrapinghub.com',
      url='http://scrapinghub.com',
      platforms = ['Any'],
      packages = find_packages(),
      install_requires = ['requests'],
      classifiers = [ 'Development Status :: 4 - Beta',
                      'License :: OSI Approved :: BSD License',
                      'Operating System :: OS Independent',
                      'Programming Language :: Python']
)
