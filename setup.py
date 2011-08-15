try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(name='scrapinghub',
      version='0.1',
      license='BSD',
      description='Client interface for Scrapinghub API',
      author='Scrapinghub',
      author_email='info@scrapinghub.com',
      url='http://github.com/scrapinghub/python-scrapinghub',
      platforms = ['Any'],
      py_modules = ['scrapinghub'],
      classifiers = [ 'Development Status :: 4 - Beta',
                      'License :: OSI Approved :: BSD License',
                      'Operating System :: OS Independent',
                      'Programming Language :: Python']
)
