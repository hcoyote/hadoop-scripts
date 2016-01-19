#!/usr/bin/python
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4
#
# python tool to create the /user/<username> dir in hdfs based on the
# contents of the passwd map.
#
# this is compatible with both /etc/passwd and with passwd files supplied
# using sssd/LDAP.
#
# Additionally, this can be run on both namenodes in an HA cluster. The 
# script will figure out which one is the active namenode and bail out if
# it is running on the secondary. This way you don't risk a race condition
# of attempting to create the same directory at the same time on two different
# namenodes.
#


import argparse
import re
import datetime
import krbV
import logging
import subprocess
import socket
import pwd
from snakebite.client import AutoConfigClient


parser = argparse.ArgumentParser()
parser.add_argument('--debug', default=False, dest='debug', help='enable debug logging', action='store_true')


args = parser.parse_args()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(parser.prog)

if args.debug:
    logger.setLevel(logging.DEBUG)



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


hdfs_cluster = subprocess.Popen(['hdfs','getconf','-confKey','dfs.nameservices'], stdout=subprocess.PIPE).communicate()[0].rstrip('\n')
logger.info("hdfs cluster name is " + hdfs_cluster)

namenode_list = subprocess.Popen(['hdfs', 'getconf', '-confKey', 'dfs.ha.namenodes.' + hdfs_cluster ], stdout=subprocess.PIPE )
namenodes = namenode_list.communicate()[0].rstrip('\n').split(',')

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


if active_namenode != socket.getfqdn():
    logger.info("active node " + active_namenode + " is not the current host, so bailing out.")
    exit(-1)
else:
    logger.info("active node is " + active_namenode)

client = AutoConfigClient()


logger.info("Getting user list from /etc/passwd and ldap")

# get a sorted user list from the passwd directory (file+ldap)
user_list = sorted(pwd.getpwall(), key=lambda tup: tup[0])

for user in user_list:
    username = user.pw_name
    userdir = "/user/" + username
    if user.pw_uid <= 500:
        continue
    if user.pw_uid >= 65534:
        continue
    if client.test(userdir, exists=True):
        logger.debug("User exists " +  username)
    else:
        logger.info("username doesn't exist " + username + "; Creating")
        if list(client.mkdir([userdir]))[0]['result']:
            logger.info("Created " + userdir)
            if list(client.chown([userdir], username))[0]['result']:
                logger.info("Chowning userdir to " + username)
            else:
                logger.warn("Could not chown to user" + username)

            if client.test(userdir, exists=True):
                logger.info("... created")
        else:
            logger.warn("Could not create /user/" + username)


logger.info("finished.")
