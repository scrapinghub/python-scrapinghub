#!/usr/bin/env python
"""
This is a script to convert log data downloaded from Scrapinghub log.json API
to the format expected by HubStorage /logs/ POST requests.

Example usage:

    $ shlog2hs < shlog.json > hslog.jl

Then you can upload the log to hubstorage using for example:

    $ curl -X POST -T hslog.jl http://localhost:8002/logs/1/1/1

WARNING: this script loads the entire log in memory. This restriction could be
dropped but is not a high priority since this script is mainly for testing
purposes.

"""

import sys, json, logging, struct

def main():
    logs = json.load(sys.stdin)
    for l in logs:
        l['level'] = getattr(logging, l['logLevel'])
        oid = l.pop('id').decode("hex")
        t = struct.unpack(">i", oid[0:4])[0]
        c = struct.unpack(">i", "\x00" + oid[-3:])[0]
        l['time'] = t*1000 + c
        del l['logLevel']

    logs.sort(key=lambda x: x['time'])

    for l in logs:
        print json.dumps(l)

if __name__ == "__main__":
    main()
