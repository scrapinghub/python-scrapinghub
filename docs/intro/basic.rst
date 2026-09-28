Basic usage
===========

Instantiate a new client with your Scrapy Cloud API key::

    >>> from scrapinghub import ScrapinghubClient
    >>> apikey = '84c87545607a4bc0****************' # your API key as a string
    >>> client = ScrapinghubClient(apikey)

.. note:: Your Scrapy Cloud API key is available at the bottom of
    https://app.zyte.com/o/settings after you sign up.

If you instantiate the client without an explicit API key, it reads the
``SH_APIKEY`` (or its ``SHUB_APIKEY`` alias, or ``SHUB_JOBAUTH``) environment
variable instead::

    >>> client = ScrapinghubClient()  # reads SH_APIKEY from the environment

Instead of exporting the variable yourself, you can store it in a ``.env``
file and let the client load it:

.. code-block:: bash
    :caption: :file:`.env`

    SH_APIKEY=84c87545607a4bc0****************

By default the client reads the nearest ``.env`` file, looking in the current
directory and then walking up through its parent directories. Use the
``dotenv_path`` argument to point it at a different file::

    >>> client = ScrapinghubClient(dotenv_path='/path/to/myenv')

Only the ``SH_APIKEY``, ``SHUB_APIKEY`` and ``SHUB_JOBAUTH`` variables are read
from the file; any other variables it contains are ignored. A variable already
set in the environment takes precedence over the value in the file.

List your deployed projects::

    >>> client.projects.list()
    [123, 456]

Run a new job for one of your projects::

    >>> project = client.get_project(123)
    >>> project.jobs.run('spider1', job_args={'arg1': 'val1'})
    <scrapinghub.client.Job at 0x106ee12e8>>

Access your job's output data::

    >>> job = client.get_job('123/1/2')
    >>> for item in job.items.iter():
    ...     print(item)
    {
        'name': ['Some item'],
        'url': 'http://some-url/item.html',
        'value': 25,
    }
    {
        'name': ['Some other item'],
        'url': 'http://some-url/other-item.html',
        'value': 35,
    }
    ...

Checkout all the other features in :ref:`overview` or in the more
detailed :ref:`api-reference`.
