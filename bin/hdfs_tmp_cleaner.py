#!/usr/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
#
# python version of the hdfs tmp cleaner that utilizes snakebite
# library instead of execing hadoop fs commands.


import argparse
import re
import datetime
import krbV
import logging
import subprocess
import socket
from snakebite.client import AutoConfigClient


parser = argparse.ArgumentParser()
parser.add_argument('--age', '-a', default=2*86400, dest='cleanup_age', help='file age in seconds (default 2 days)', type=int)
parser.add_argument('--recurse', '-r', default=False, dest='recurse_filesystem', help='Recurse through specified paths instead of looking at just the top-level components' , action='store_true')
parser.add_argument('--delete', default=False, dest='actually_delete', help='Commit the file deletion', action='store_true')
parser.add_argument('path', help='path to du')
args = parser.parse_args()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(parser.prog)

older_than = datetime.datetime.now()-datetime.timedelta(seconds=args.cleanup_age)

def has_ticket():
    '''
    Checks to see if the user has a valid ticket.
    '''
    ctx = krbV.default_context()
    cc = ctx.default_ccache()
    try:
        princ = cc.principal()
        retval = True
    except krbV.Krb5Error:
        retval = False

    return retval

if not has_ticket():
    print "No valid kerberos ticket; either kinit or k5start the hdfs user's keytab"
    exit(-1)

# figure out which cluster we're on via the Hadoop Client Configs local to
# this sytem.
hdfs_cluster = subprocess.Popen(['hdfs','getconf','-confKey','dfs.nameservices'], stdout=subprocess.PIPE).communicate()[0].rstrip('\n')
logger.info("hdfs cluster name is " + hdfs_cluster)

# find our namenodes
namenode_list = subprocess.Popen(['hdfs', 'getconf', '-confKey', 'dfs.ha.namenodes.' + hdfs_cluster ], stdout=subprocess.PIPE )
namenodes = namenode_list.communicate()[0].rstrip('\n').split(',')

# figure out which namenode is the active one
active_namenode = ''
for node in namenodes:
    is_active_output = subprocess.Popen(['hdfs','haadmin', '-getServiceState', node], stdout=subprocess.PIPE )
    is_active = is_active_output.communicate()[0].rstrip('\n')
    if is_active == 'active':
        logger.info(node + " is the active node")
        which_active_namenode = subprocess.Popen(['hdfs','getconf','-confKey','dfs.namenode.rpc-address.'+hdfs_cluster+'.'+node], stdout=subprocess.PIPE)
        active_namenode = which_active_namenode.communicate()[0].rstrip('\n').rsplit(':')[0]
        break
    else:
        logger.info(node + " is the standby node")
        continue

# bail out if the current node we're running on is not the active namenode.
if active_namenode != socket.getfqdn():
    logger.info("active node " + active_namenode + " is not the current host, so bailing out.")
    exit(-1)
else:
    logger.info("active node is " + active_namenode)



client = AutoConfigClient()

client.use_trash=False

donotdelete_whitelist = [
    # don't remove hadoop-mapred, this kills running jobs
    re.compile("hadoop-mapred"),

    # let's explicitly match hbase.
    re.compile("^/hbase/?"),

    # don't nuke this; hbase uses it for bulk loading.
    re.compile("^/tmp/hbase-staging/?"),

    # let's try to make sure we're not matching against a top-level path
    re.compile("^/[-_.a-zA-Z0-9]+/?$"),

    re.compile("cloudera_health_monitoring_canary_files"),

    # let's bail out explicitly on anything in our data path
    re.compile("^/data/production/?"),
]


if client.test(args.path, exists=True):
    for x in client.ls([args.path], recurse=args.recurse_filesystem):
        if any(regex.search(x['path']) for regex in donotdelete_whitelist):
            logger.info("Matched banned thing, not attempting to delete it: %s", x['path'])
        else:
            f_timestamp = datetime.datetime.fromtimestamp(x['modification_time']/1000)
            if  f_timestamp < older_than:
                logger.info("I might delete this: %s %s", x['path'], f_timestamp)
                if args.actually_delete:
                    logger.info("Issuing delete of %s", list(client.delete([x['path']], recurse=True)))
                    if client.test(x['path'], exists=True):
                        logger.info("Removed %s", x['path'])
                else:
                    logger.info( "I would have deleted this: %s ", x['path'])
else:
    logger.warn("%s is not found on hdfs", args.path)
