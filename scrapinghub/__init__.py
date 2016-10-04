__all__ = ['__version__', "APIError", "Connection", "HubstorageClient"]


import pkgutil
__version__ = pkgutil.get_data(__package__, 'VERSION').decode('ascii').strip()
del pkgutil


from .legacy import *
from .hubstorage import HubstorageClient
