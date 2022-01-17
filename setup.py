import sys
from os.path import dirname, join

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


with open(join(dirname(__file__), 'scrapinghub/VERSION'), 'rb') as f:
    version = f.read().decode('ascii').strip()

# keeping this as it might be useful at some point
is_pypy = '__pypy__' in sys.builtin_module_names

setup(
    name='scrapinghub',
    version=version,
    license='BSD',
    description='Client interface for Scrapinghub API',
    author='Scrapinghub',
    author_email='info@scrapinghub.com',
    url='http://github.com/scrapinghub/python-scrapinghub',
    platforms=['Any'],
    packages=['scrapinghub', 'scrapinghub.client', 'scrapinghub.hubstorage'],
    package_data={'scrapinghub': ['VERSION']},
    install_requires= [
        'msgpack>=1.0.0',
        'requests>=1.0',
        'retrying>=1.3.3',
        'six>=1.10.0'
    ],
    extras_require={
        # Kept for backward compatibility, msgpack is not a direct dependency.
        'msgpack': [],
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Internet :: WWW/HTTP',
    ],
)
