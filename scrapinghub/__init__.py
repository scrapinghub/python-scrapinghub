__all__ = ["APIError", "Connection", "HubstorageClient", "ScrapinghubClient"]


import pkgutil
__version__ = pkgutil.get_data(__package__, 'VERSION')
__version__ = str(__version__.decode('ascii').strip())
del pkgutil


from .legacy import *
from .hubstorage import HubstorageClient
from .client import ScrapinghubClient
