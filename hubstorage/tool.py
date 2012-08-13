import sys, optparse, logging
from hubstorage import Client

def parse_opts():
    op = optparse.OptionParser(usage='%prog [options] server-path')
    op.add_option('-s', '--server', metavar='URL', default='http://localhost:8002',
        help='hubstorage server url (default: %default)')
    op.add_option('-k', '--apikey',
        help='hubstorage api key')
    op.add_option('-L', '--loglevel', metavar='LEVEL', default='WARNING',
        help='log level (default: %default)')
    op.add_option('--load', action='store_true',
        help='load jsonlines from stdin')
    op.add_option('--dump', action='store_true',
        help='dump contents')
    opts, args = op.parse_args()
    if len(args) != 1:
        op.error('incorrect number of arguments')
    return opts, args[0]

def main():
    opts, path = parse_opts()
    logging.basicConfig(level=getattr(logging, opts.loglevel))
    client = Client(opts.apikey, url=opts.server)
    if opts.load:
        with client.open_item_writer(path) as writer:
            for line in sys.stdin:
                writer.write_json_item(line.strip())
    elif opts.dump:
        for jsonitem in client.iter_json_items(path):
            print jsonitem.rstrip()

if __name__ == '__main__':
    main()
