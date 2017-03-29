__all__ = ["APIError", "Connection", "HubstorageClient",
           "ScrapinghubClient", "ScrapinghubAPIError",
           "DuplicateJobError", "BadRequest", "NotFound",
           "Unauthorized", "ValueTooLarge", "ServerError"]

import pkgutil
__version__ = pkgutil.get_data(__package__, 'VERSION')
__version__ = str(__version__.decode('ascii').strip())
del pkgutil


from .legacy import *
from .hubstorage import HubstorageClient
from .client import ScrapinghubClient
from .client.exceptions import (
    ScrapinghubAPIError,
    DuplicateJobError,
    BadRequest,
    NotFound,
    Unauthorized,
    ValueTooLarge,
    ServerError,
)
