#!/usr/bin/env python

import argparse
import getpass
import socket
import sys

from ingest.streams.context import init_streams_context
from settings.settings import DEPLOYMENT, PRODUCTION
from settings.ingestservers import init_servers


def check_positive_int(value):
    ivalue = int(value)
    if ivalue <= 0:
        raise argparse.ArgumentTypeError("%s is an invalid positive int value" % value)
    return ivalue


def check_ipv4_address(value):
    try:
        socket.inet_aton(value)
    except socket.error:
        raise argparse.ArgumentTypeError("%s is an invalid IPv4 address" % value)
    return value

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='The Guardian!', epilog="There might exist bugs!", add_help=False)
    # parser.add_argument('-h', '--host', type=check_ipv4_address, nargs='?', default='0.0.0.0',
    #                    help='Server host (default: 0.0.0.0)', metavar='HOST')
    parser.add_argument('-p', '--port', type=check_positive_int, nargs='?', default=8000,
                        help='Server port (default: 8000)', metavar='PORT')
    parser.add_argument('-dh', '--dhost', nargs='?', default='127.0.0.1',
                        help='MonetDB database host (default: 127.0.0.1)', metavar='HOST')
    parser.add_argument('-dp', '--dport', type=check_positive_int, nargs='?', default=50000,
                        help='Database listening port (default: 50000)', metavar='PORT')
    parser.add_argument('-d', '--database', nargs='?', default='db', help='Database name (default: db)')
    parser.add_argument('-u', '--user', nargs='?', default='monetdb', help='Database user (default: monetdb)')
    parser.add_argument('-?', '--help', action='store_true', help='Display this help')

    try:
        args = vars(parser.parse_args())
    except BaseException as ex:
        print(ex)
        sys.exit(1)

    if args['help']:
        parser.print_help()
        sys.exit(0)

    if DEPLOYMENT != PRODUCTION:
        con_password = 'monetdb'
    else:
        con_password = getpass.getpass(prompt="Insert password for user " + args['user'] + ":")

    init_streams_context(args['dhost'], args['dport'], args['user'], con_password, args['database'])
    init_servers(args['port'])
