from __future__ import absolute_import

from .utils import _Proxy


class Samples(_Proxy):
    """Representation of collection of job samples.

    Not a public constructor: use :class:`Job` instance to get a
    :class:`Samples` instance. See :attr:`Job.samples` attribute.

    Please note that list() method can use a lot of memory and for a large
    amount of samples it's recommended to iterate through it via iter()
    method (all params and available filters are same for both methods).

    Usage:

    - retrieve all samples from a job::

        >>> job.samples.iter()
        <generator object mpdecode at 0x10f5f3aa0>

    - retrieve samples with timestamp greater or equal to given timestamp::

        >>> job.samples.list(startts=1484570043851)
        [[1484570043851, 554, 576, 1777, 821, 0],
         [1484570046673, 561, 583, 1782, 821, 0]]
    """
