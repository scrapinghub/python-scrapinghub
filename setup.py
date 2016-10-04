import sys
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

# We can't import hubstorage.__version__ because it imports "requests" and it
# can not be available yet
is_pypy = '__pypy__' in sys.builtin_module_names
mpack_required = 'msgpack-pypy>=0.0.2' if is_pypy else 'msgpack-python>=0.4.7'

setup(
    name='scrapinghub',
    version='1.8.0',
    license='BSD',
    description='Client interface for Scrapinghub API',
    author='Scrapinghub',
    author_email='info@scrapinghub.com',
    url='http://github.com/scrapinghub/python-scrapinghub',
    platforms = ['Any'],
    packages=['scrapinghub', 'scrapinghub.hubstorage'],
    install_requires=['requests', 'retrying>=1.3.3', 'six>=1.10.0'],
    extras_require = {'msgpack': [mpack_required]},
    classifiers = [
        'Development Status :: 5 - Production/Stable',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: Implementation :: CPython',
        'Programming Language :: Python :: Implementation :: PyPy',
        'Topic :: Internet :: WWW/HTTP',
    ],
)
