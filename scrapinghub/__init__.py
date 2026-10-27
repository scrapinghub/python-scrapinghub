__all__ = ["APIError", "Connection", "HubstorageClient",
           "ScrapinghubClient", "ScrapinghubAPIError",
           "DuplicateJobError", "BadRequest", "NotFound",
           "Unauthorized", "ValueTooLarge", "ServerError"]

from importlib.metadata import version as _version
__version__ = _version("scrapinghub")


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
