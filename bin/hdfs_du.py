#!/usr/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4


import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--human', '-H', default=0, dest='human_readable', help='human readable sizes')
parser.add_argument('--larger', '-l', default=0, dest='larger_than', help='only show data larger than X Gigabytes', type=int)
parser.add_argument('--total', '-t', default=0, dest='total', help='gather total size')
parser.add_argument('path', help='path to du')
args = parser.parse_args()

f=args.larger_than*(1024*1024*1024)

total=0

from snakebite.client import AutoConfigClient
client = AutoConfigClient()
if client.test(args.path, exists=True):
    for x in client.ls([args.path]):
        du = client.du([x['path']], include_toplevel=True, include_children=False)
        for y in list(du):
            if y['length'] >  f:
                total = total + y['length']
                print total
                print y['path'],
                if args.human_readable:
                    print "%.2f G" % (y['length'] / (1024*1024*1024))
                else:
                    print y['length']

if total > 0:
    if args.human_readable:
        print "total: %.2f G" % (y['length'] / (1024*1024*1024))
    else:
        print "total: %f bytes" % total
else:
    print args.path, "is not found on hdfs"
